from __future__ import division
import os
from pyspark.sql import functions as F

import numpy
import pandas
import sparkmanager as sm

from .circuit import Circuit
from .definitions import CellClass, CheckpointPhases
from .schema import pathway_i_to_str, touches_with_pathway
from ._filtering import DataSetOperation
from .utils import get_logger
from .utils.checkpointing import checkpoint_resume, CheckpointHandler
from .utils.spark import cache_broadcast_single_part
from .filter_udfs import reduce_cut_parameter_udef
from .random import RNGThreefry, uniform

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False
_DEBUG_CSV_OUT = False
_MB = 1024 * 1024

_KEY_REDUCE = 0x100
_KEY_CUT = 0x101
_KEY_ACTIVE = 0x102

if _DEBUG:
    os.path.isdir("_debug") or os.makedirs("_debug")


class SomaDistanceFilter(DataSetOperation):
    """Filter touches based on distance from soma

    Prevents the analysis/simulation of touches within the soma

    :param morphos: the morphology database to use
    """
    def __init__(self, morphos):
        self.__morphos = morphos

    def apply(self, circuit, *args, **kwargs):
        """Remove touches within the soma.
        """
        check_soma_axon_distance = self._create_soma_distance_filter_udf()
        return circuit.withColumn('valid_touch',
                                  check_soma_axon_distance(F.col('dst_morphology_i'),
                                                           F.col('distance_soma'))) \
                      .where(F.col('valid_touch')) \
                      .drop('valid_touch')

    def _create_soma_distance_filter_udf(self):
        """Produce UDF to filter soma touches/gap junctions
        """
        @F.pandas_udf('boolean')
        def check_soma_axon_distance(dst_morpho_i, distance_soma):
            """
            :param dst_morpho_i: Pandas series with the 
            :param distance_soma: distances to the soma
            """
            res = pandas.Series(data=False, index=dst_morpho_i.index, dtype='bool_')
            for idx, morpho in dst_morpho_i.iteritems():
                res[idx] = distance_soma[idx] >= self.__morphos[morpho].soma_radius
            return res
        return check_soma_axon_distance


class GapJunctionFilter(DataSetOperation):
    """Implement filtering touches for gap junctions

    :param morphos: the morphology database to use
    """

    DENDRITE_COLUMNS = ['src', 'dst', 'pre_section', 'pre_segment', 'post_section', 'post_segment']

    def __init__(self, morphos):
        self.__morphos = morphos

    def apply(self, circuit, *args, **kwargs):
        """Apply both the dendrite-soma and dendrite-dendrite filters.
        """
        circuit = self._add_group_column(circuit, 'groupid')
        circuit = circuit.withColumnRenamed('rand_idx', 'pre_junction') \
                         .withColumn('post_junction', F.col('pre_junction'))

        trim_touches = self._create_soma_filter_udf(circuit)
        match_dendrites = self._create_dendrite_match_udf(circuit)

        somas = circuit.where("post_section == 0").groupby("groupid").apply(trim_touches)
        dendrites = circuit.where("post_section > 0").groupby("groupid").apply(match_dendrites)
        return somas.union(dendrites).drop('groupid').repartition("src", "dst")

    @staticmethod
    def _add_group_column(circuit, name):
        """Add an additional column to the circuit for grouping on.
        """
        @F.pandas_udf('long')
        def _calc_group_id(src, dst):
            def _id(row):
                """Directionally independent ordering
                """
                r = row // 100
                return hash((r.min(), r.max()))
            df = pandas.DataFrame({'src': src, 'dst': dst})
            return df.apply(numpy.vectorize(_id, signature='(2)->()'), axis='columns', raw=True)
        return circuit.withColumn(name, _calc_group_id(F.col("src"), F.col("dst")))

    def _create_soma_filter_udf(self, circuit):
        """Produce UDF to filter soma touches/gap junctions

        Filter dendrite to soma gap junctions, removing junctions that are
        on parent branches of the dendrite and closer than 3 times the soma
        radius.

        :param circuit: a dataframe whose schema to re-use for the UDF
        """
        @F.pandas_udf(circuit.schema, F.PandasUDFType.GROUPED_MAP)
        def trim_touches(data):
            """
            :param data: a Pandas dataframe
            """
            if len(data) == 0:
                return data
            res = []

            # Use internal grouping to allow for several src, dst pairs in
            # one python batch
            for _, group in data.groupby(["src", "dst"]):
                mindex = group["src_morphology_i"].iloc[0]
                morpho = self.__morphos[mindex]
                min_dist = 3 * morpho.soma_radius

                local_res = []
                for row in group.itertuples():
                    if len(local_res) > 0:
                        path = set(morpho.path(row.pre_section))
                        dist = morpho.distance_of(row.pre_section, row.pre_segment)
                        drop = []
                        for idx in local_res:
                            if data.pre_section[idx] in path:
                                odist = morpho.distance_of(data.pre_section[idx],
                                                           data.pre_segment[idx])
                                if dist - odist < min_dist:
                                    drop.append(idx)
                        for idx in drop:
                            local_res.remove(idx)
                    local_res.append(row.Index)
                res.extend(local_res)
            return data.loc[res]
        return trim_touches

    @staticmethod
    def is_within(r, rs, data):
        """Find element `r` in list `rs` with tolerances.

        The first match is removed from the list.

        :param r: the segments/sections to search for in `rs`
        :param rs: a list segment/section coordinates
        """
        def is_close(a, b):
            # FIXME why not the following, which seems to be more precise?
            # abs(a.pre_seg - b.post_seg) <= 1 and abs(a.post_seg - b.pre_seg) <= 1:
            return (data.at[a, 'pre_section'] == data.at[b, 'post_section']) and \
                   (data.at[a, 'post_section'] == data.at[b, 'pre_section']) and \
                   ((data.at[a, 'pre_segment'] == data.at[b, 'post_segment'] and
                     data.at[a, 'post_segment'] == data.at[b, 'pre_segment']) or
                    (abs(data.at[a, 'pre_segment'] - data.at[b, 'post_segment']) == 1 and
                     abs(data.at[a, 'post_segment'] - data.at[b, 'pre_segment']) == 1))
        for n, r2 in enumerate(rs):
            if is_close(r, r2):
                return rs.pop(n)
        return None

    @staticmethod
    def update_position(data, i1, i2):
        """Update the touch position of `i1` with info from `i2`
        """
        # FIXME according to the documentation of the
        # C-functionalizer/comments there, this should be done the other
        # way around?
        data.at[i1, 'pre_section'] = data.at[i2, 'post_section']
        data.at[i1, 'pre_segment'] = data.at[i2, 'post_segment']
        data.at[i1, 'post_section'] = data.at[i2, 'pre_section']
        data.at[i1, 'post_segment'] = data.at[i2, 'pre_segment']
        data.at[i1, 'post_junction'] = data.at[i2, 'pre_junction']
        data.at[i2, 'post_junction'] = data.at[i1, 'pre_junction']

    def _create_dendrite_match_udf(self, circuit):
        """Produce UDF to match dendrite touches/gap junctions

        Filter dendrite to dendrite junctions, keeping only junctions that
        have a match in both directions, with an optional segment offset of
        one.

        :param circuit: a dataframe whose schema to re-use for the UDF
        """
        @F.pandas_udf(circuit.schema, F.PandasUDFType.GROUPED_MAP)
        def match_touches(data):
            """
            :param data: a Pandas dataframe
            """
            if len(data) == 0:
                return data
            import time
            t = time.time()
            data['accept'] = False
            for (src, dst), group in data.groupby(["src", "dst"]):
                if src > dst:
                    continue
                transposed = data[(data.src == dst) & (data.dst == src)]
                to_match = list(transposed.index)
                for r in group.index:
                    o = self.is_within(r, to_match, data)
                    if o:
                        data.at[r, 'accept'] = True
                        data.at[o, 'accept'] = True
                        self.update_position(data, r, o)
                        continue
            dur = time.time() - t
            conn = set(zip(data.src, data.dst))
            print(f"Time spent on {len(data)} rows ({len(conn)} connections): {dur:0.2f} ({len(data) / dur:0.2f} rows/s)")
            return data[data['accept']].drop(columns=['accept'])
        return match_touches


# -------------------------------------------------------------------------------------------------
class BoutonDistanceFilter(DataSetOperation):
    """
    Class implementing filtering by Bouton Distance (min. distance to soma)
    """
# -------------------------------------------------------------------------------------------------

    def __init__(self, bouton_distance_obj):
        self._bouton_distance_obj = bouton_distance_obj

    def apply(self, circuit, *args, **kw):
        """Apply filter
        """
        # Use broadcast of Neuron version
        new_circuit = circuit.where("(distance_soma >= %f AND dst_syn_class_i = %d) OR "
                                    "(distance_soma >= %f AND dst_syn_class_i = %d)" % (
                                        self._bouton_distance_obj.inhibitorySynapsesDistance,
                                        CellClass.INH.fzer_index,
                                        self._bouton_distance_obj.excitatorySynapsesDistance,
                                        CellClass.EXC.fzer_index)
                                    )
        return new_circuit


# -------------------------------------------------------------------------------------------------
class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """
    Reverse version of Bouton Distance filter, only keeping outliers.
    """
# -------------------------------------------------------------------------------------------------

    def apply(self, circuit, *args, **kw):
        new_circuit = circuit.where("(distance_soma < %f AND dst_syn_class_i = %d) OR "
                                    "(distance_soma < %f AND dst_syn_class_i = %d)" % (
                                        self._bouton_distance_obj.inhibitorySynapsesDistance,
                                        self.synapse_classes_indexes[CellClass.CLASS_INH],
                                        self._bouton_distance_obj.excitatorySynapsesDistance,
                                        self.synapse_classes_indexes[CellClass.CLASS_EXC])
                                    )
        return new_circuit


# -------------------------------------------------------------------------------------------------
class TouchRulesFilter(DataSetOperation):
    """
    Class implementing TouchRules filter.
    """
# -------------------------------------------------------------------------------------------------

    def __init__(self, matrix):
        """Create a new instance

        :param matrix: a matrix indicating
        """
        rdd = sm.parallelize(((i,) for i, v in enumerate(matrix.flatten()) if v == 0), 200)
        self._rules = sm.createDataFrame(rdd, schema=["fail"])
        self._indices = list(matrix.shape)
        for i in reversed(range(len(self._indices) - 1)):
            self._indices[i] *= self._indices[i + 1]

    def apply(self, circuit, *args, **kw):
        """ .apply() method (runner) of the filter
        """
        # For each neuron we require: preLayer, preMType, postLayer, postMType, postBranchIsSoma
        # The first four fields are properties of the neurons, part of neuronDF, while postBranchIsSoma
        # is a property if the touch, checked by the index of the target neuron section (0->soma)
        touches = circuit.withColumn("fail",
                                     circuit.src_layer_i * self._indices[1] +
                                     circuit.dst_layer_i * self._indices[2] +
                                     circuit.src_mtype_i * self._indices[3] +
                                     circuit.dst_mtype_i * self._indices[4] +
                                     (circuit.post_section > 0).cast('integer')) \
            .join(F.broadcast(self._rules), "fail", "left_anti") \
            .drop("fail")
        return touches


# -------------------------------------------------------------------------------------------------
class ReduceAndCut(DataSetOperation):
    """
    Class implementing ReduceAndCut
    """
# -------------------------------------------------------------------------------------------------

    def __init__(self, conn_rules, stats, seed):
        self.conn_rules = sm.broadcast(conn_rules)
        self.stats = stats
        self.seed = seed

    # ---
    def apply(self, circuit, *args, **kw):
        full_touches = Circuit.only_touch_columns(touches_with_pathway(circuit))
        mtypes = kw["mtypes"]

        # Get and broadcast Pathway stats
        # NOTE we cache and count to force evaluation in N tasks, while sorting in a single task
        logger.debug("Computing Pathway stats...")
        _params = self.compute_reduce_cut_params(full_touches)
        params_df = F.broadcast(_params)

        # Params ouput for validation
        _params_out_csv(params_df)

        #
        # Reduce
        logger.info("Applying Reduce step...")
        reduced_touches = self.apply_reduce(full_touches, params_df)

        #
        # Cut
        logger.info("Calculating CUT part 1: survival rate")
        cut1_shall_keep_connections = self.calc_cut_survival_rate(
            reduced_touches,
            params_df,
            mtypes=mtypes
        )

        _connection_counts_out_csv(cut1_shall_keep_connections, "cut_counts.csv", mtypes)

        logger.info("Calculating CUT part 2: Active Fractions")
        cut_shall_keep_connections = self.calc_cut_active_fraction(
            cut1_shall_keep_connections,
            params_df,
            mtypes=mtypes
        ).select("src", "dst")

        with sm.jobgroup("Filtering touches CUT step", ""):
            cut2AF_touches = (
                reduced_touches.join(cut_shall_keep_connections, ["src", "dst"])
            )

            _touch_counts_out_csv(cut2AF_touches, "cut2af_counts.csv", mtypes)

        # Only the touch fields
        return Circuit.only_touch_columns(cut2AF_touches)

    # ---
    @sm.assign_to_jobgroup
    @checkpoint_resume("pathway_stats", bucket_cols="pathway_i", n_buckets=1)
    def compute_reduce_cut_params(self, full_touches):
        """ Computes the pathway parameters, used by Reduce and Cut filters
        """
        # First obtain the pathway (morpho-morpho) stats dataframe
        _n_parts = max(full_touches.rdd.getNumPartitions() // 20, 100)
        pathway_stats = (self.stats
            .get_pathway_touch_stats_from_touches_with_pathway(full_touches)
            .coalesce(_n_parts))

        # param create udf
        rc_param_maker = reduce_cut_parameter_udef(self.conn_rules)

        # Run UDF
        params_df = pathway_stats.select(
            "*",
            rc_param_maker(pathway_stats.pathway_i, pathway_stats.average_touches_conn).alias("rc_params")
        )

        # Return the interesting params
        params_df = (params_df
                     .select("pathway_i",
                             "total_touches",
                             F.col("average_touches_conn").alias("structural_mean"),
                             "rc_params.*")
                     .cache())
        # materialize in parallel
        params_df.count()
        # Save in single partition
        return params_df.coalesce(1)

    # ---
    @sm.assign_to_jobgroup
    @checkpoint_resume(CheckpointPhases.FILTER_REDUCED_TOUCHES.name, bucket_cols=("src", "dst"),
                       # Even if we change to not break exec plan we always keep only touch cols
                       handlers=[CheckpointHandler.before_save(Circuit.only_touch_columns)])
    def apply_reduce(self, all_touches, params_df):
        """ Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a pathway (morpho-morpho association)
        logger.debug(" -> Building reduce fractions")
        fractions = F.broadcast(params_df.select("pathway_i", "pP_A"))

        logger.debug(" -> Cutting touches")
        return _add_random_column(
            all_touches.join(fractions, "pathway_i"),
            "reduce_rand", self.seed, _KEY_REDUCE,
            F.col("src"), F.col("rand_idx")
        ).where(F.col("pP_A") > F.col("reduce_rand")) \
         .drop("reduce_rand", "rand_idx", "pP_A") \
         .repartition("src", "dst")

    # ---
    @sm.assign_to_jobgroup
    def calc_cut_survival_rate(self, reduced_touches, params_df, mtypes):
        """
        Apply cut filter
        Cut computes a survivalRate and activeFraction for each post neuron
        And filters out those not passing the random test
        """
        logger.info("Computing reduced touch counts")
        reduced_touch_counts_connection = (
            reduced_touches
            .groupBy("src", "dst")
            .agg(F.first("pathway_i").alias("pathway_i"),
                 F.count("src").alias("reduced_touch_counts_connection"))
        )
        # Debug
        _connection_counts_out_csv(reduced_touch_counts_connection, "reduced_touch_counts_pathway", mtypes)

        params_df_sigma = (
            params_df
            .where(F.col("pMu_A").isNotNull())
            .select("pathway_i", "pMu_A")
            .withColumn("sigma", params_df.pMu_A / 4)
        )

        connection_survival_rate = (
            reduced_touch_counts_connection
            .join(params_df_sigma, "pathway_i")  # Fetch the pathway params
            .withColumn("survival_rate",  # Calc survivalRate
                        F.expr("1.0 / (1.0 + exp((-4.0/sigma) * (reduced_touch_counts_connection-pMu_A)))"))
            .drop("sigma", "pMu_A")
        )

        _dbg_df = connection_survival_rate.select("src", "dst", "reduced_touch_counts_connection", "survival_rate")
        _write_csv(_dbg_df, "connection_survival_rate.csv")

        logger.debug(" -> Computing connections to cut according to survival_rate")
        _df = connection_survival_rate
        cut_connections = _add_random_column(
            _df, "cut_rand", self.seed, _KEY_CUT,
            F.col("src"), F.col("dst")
        ).where((_df.survival_rate > .0) & (_df.survival_rate > F.col("cut_rand"))) \
         .select("src", "dst", "pathway_i", "reduced_touch_counts_connection")
        # Much smaller data volume but we cant coealesce
        return cut_connections

    # ----
    @sm.assign_to_jobgroup
    @checkpoint_resume("shall_keep_connections", bucket_cols=("src", "dst"))
    def calc_cut_active_fraction(self, cut_touch_counts_connection, params_df, mtypes):
        """
        Performs the second part of the cut algorithm according to the active_fractions
        :param params_df: The parameters DF (pA, uA, active_fraction_legacy)
        :param cut_touch_counts_connection: The DF with the cut touch counts per connection \
               (built previously in an optimized way)
        :return: The final cut touches
        """

        logger.debug("Computing Pathway stats")
        cut_touch_counts_pathway = (
            cut_touch_counts_connection
            .groupBy("pathway_i")
            .agg(F.sum("reduced_touch_counts_connection").alias("cut_touch_counts_pathway"))
        )

        active_fractions = (
            cut_touch_counts_pathway
            .join(params_df, "pathway_i")
            .withColumn("actual_reduction_factor",
                F.col("cut_touch_counts_pathway") / F.col("total_touches")
            )
            .withColumn("active_fraction",
                F.when(F.col("bouton_reduction_factor").isNull(),
                    F.col("active_fraction_legacy")
                )
                .otherwise(
                    F.when(F.col("bouton_reduction_factor") > F.col("actual_reduction_factor"),
                        F.lit(1.0)
                    )
                    .otherwise(
                        F.col("bouton_reduction_factor") / F.col("actual_reduction_factor")
                    )
                )
            )
            .select("pathway_i", "active_fraction")
        )

        logger.debug("Computing Active Fractions")
        # On both ends data is small (500x less) so we can reduce parallelism
        _n_parts = max(cut_touch_counts_connection.rdd.getNumPartitions() // 50, 50)
        # Result is a minimal DS so we cache and broadcast in single partition
        active_fractions = cache_broadcast_single_part(active_fractions, parallelism=_n_parts)

        _write_csv(pathway_i_to_str(active_fractions, mtypes), "active_fractions.csv")

        shall_keep_connections = _add_random_column(
            cut_touch_counts_connection.join(active_fractions, "pathway_i"),
            "active_rand", self.seed, _KEY_ACTIVE,
            F.col("src"), F.col("dst")
        ).where(F.col("active_rand") < F.col("active_fraction")) \
         .select("src", "dst")

        return shall_keep_connections


def _add_random_column(df, name, seed, key, primary, secondary):
    """Add a random column to a dataframe

    :param df: the dataframe to augment
    :param name: name for the random column
    :param seed: the seed to use for the RNG
    :param key: first key to derive the RNG with
    :param primary: column to sort internally and second derivative
    :param secondary: column for third derivative
    :return: the dataframe with a random column
    """
    @F.pandas_udf('float')
    def _fixed_rand(col1, col2):
        rng = RNGThreefry().seed(seed).derivate(key)
        return pandas.Series(uniform(rng, col1.values, col2.values))

    return df.sortWithinPartitions(primary) \
             .withColumn(name, _fixed_rand(primary, secondary))


def _params_out_csv(df):
    debug_info = df.select("pathway_i", "total_touches", "structural_mean",
                           "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
    _write_csv(debug_info, "pathway_params.csv")


def _touch_counts_out_csv(df, filename, mtypes):
    _write_csv(pathway_i_to_str(
        df.groupBy("pathway_i").count(),
        mtypes
    ), filename)


def _connection_counts_out_csv(df, filename, mtypes):
    _write_csv(pathway_i_to_str(
        df.groupBy("pathway_i").sum("reduced_touch_counts_connection"),
        mtypes
    ), filename)


def _write_csv(df, filename):
    if _DEBUG_CSV_OUT:
        df.coalesce(1).write.csv("_debug/" + filename, header=True, mode="overwrite")


# -------------------------------------------------------------------------------------------------
class CumulativeDistanceFilter(DataSetOperation):
    """
    Filtering based on cumulative distances (By D. Keller - BLBLD-29)
    """
# -------------------------------------------------------------------------------------------------

    def __init__(self, recipe, stats):
        self.recipe = recipe
        self.stats = stats

    def apply(self, circuit, *args, **kw):
        """Returns the touches of the circuit: NOOP
        """
        return circuit.touches

    def filterInhSynapsesBasedOnThresholdInPreSynapticData(self):
        pass

    def filterExcSynapsesBasedOnDistanceInPreSynapticData(self):
        pass
