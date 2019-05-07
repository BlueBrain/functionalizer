from __future__ import division
import os
from pyspark.sql import functions as F
from pyspark.sql import types as T

import numpy
import pandas
import sparkmanager as sm

from fnmatch import filter as matchfilter
from ..circuit import Circuit
from ..definitions import CellClass, CheckpointPhases
from ..schema import pathway_i_to_str, touches_with_pathway
from ..filters import DatasetOperation
from ..utils import get_logger
from ..utils.checkpointing import checkpoint_resume, CheckpointHandler
from ..utils.spark import cache_broadcast_single_part
from .udfs import reduce_cut_parameter_udf, match_dendrites
from ..random import RNGThreefry, uniform

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False
_MB = 1024 * 1024

_KEY_REDUCE = 0x100
_KEY_CUT = 0x101
_KEY_ACTIVE = 0x102


class SomaDistanceFilter(DatasetOperation):
    """Filter touches based on distance from soma

    Removes all touches that are located within the soma.
    """

    def __init__(self, recipe, morphos, stats):
        self.__morphos = morphos

    def apply(self, circuit):
        """Remove touches within the soma.
        """
        soma_radius = self._create_soma_radius_udf()
        radii = circuit.neurons.select('morphology_i') \
                               .distinct() \
                               .withColumn('radius_soma',
                                           soma_radius(F.col('morphology_i'))) \
                               .withColumnRenamed('morphology_i', 'dst_morphology_i')
        _n_parts = max(radii.rdd.getNumPartitions() // 20, 100)
        radii = cache_broadcast_single_part(radii, parallelism=_n_parts)
        return circuit.df.join(radii, 'dst_morphology_i') \
                         .where(F.col('distance_soma') >= F.col('radius_soma')) \
                         .drop('radius_soma')

    def _create_soma_radius_udf(self):
        """Produce a UDF to calculate soma radii
        """
        @F.pandas_udf('float')
        def soma_radius(morphos):
            def r(idx):
                return self.__morphos[idx].soma_radius()
            f = numpy.vectorize(r)
            return pandas.Series(data=f(morphos.values), dtype='float')
        return soma_radius


class GapJunctionFilter(DatasetOperation):
    """Synchronize gap junctions

    Ensures that:

    * Dendro-dentro and dendro-soma touches are present as src-dst and the
      corresponding dst-src pair.  The sections of the touches have to be
      aligned exactly, while the segment may deviate to neighboring ones.

    * Dendro-somatic touches: the structure of the neuron morphology is
      traversed and from all touches that are within a distance of 3 soma
      radii on the same branch only the "parent" ones are kept.
    """

    DENDRITE_COLUMNS = ['src', 'dst', 'pre_section', 'pre_segment', 'post_section', 'post_segment']

    _checkpoint = True

    def __init__(self, recipe, morphos, stats):
        self.__morphos = morphos

    def apply(self, circuit):
        """Apply both the dendrite-soma and dendrite-dendrite filters.
        """
        touches = circuit.df.withColumnRenamed('synapse_id', 'pre_junction') \
                            .withColumn('post_junction', F.col('pre_junction'))

        trim = self._create_soma_filter_udf(touches)
        match = self._create_dendrite_match_udf(touches)

        touches = touches.groupby(F.least(F.col("src"),
                                          F.col("dst")),
                                  F.shiftRight(F.greatest(F.col("src"),
                                                          F.col("dst")),
                                               15)) \
                         .apply(match)
        dendrites = touches.where("post_section > 0 and pre_section > 0")
        somas = touches.where("post_section == 0 or pre_section == 0") \
                       .groupby(F.shiftRight(F.col("src"), 4)) \
                       .apply(trim)

        return somas.union(dendrites) \
                    .repartition("src", "dst")

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

            src = data.src.values
            dst = data.dst.values
            sec = data.pre_section.values
            seg = data.pre_segment.values
            soma = data.post_section.values

            jid1 = data.pre_junction.values
            jid2 = data.post_junction.values

            morphos = data.src_morphology_i.values
            activated = numpy.zeros_like(src, dtype=bool)
            distances = numpy.zeros_like(src, dtype=float)

            connections = numpy.stack((src, dst, morphos)).T
            unique_conns = numpy.unique(connections, axis=0)
            unique_morphos = numpy.unique(connections[:, 2])

            for m in unique_morphos:
                # Work one morphology at a time to conserve memory
                morpho = self.__morphos[m]
                mdist = 3 * morpho.soma_radius(cache=True)

                # Resolve from indices matching morphology to connections
                idxs = numpy.where(unique_conns[:, 2] == m)[0]
                conns = unique_conns[idxs]
                for conn in conns:
                    # Indices where the connections match
                    idx = numpy.where((connections[:, 0] == conn[0]) &
                                      (connections[:, 1] == conn[1]))[0]
                    # Match up gap-junctions that are reverted at the end
                    if len(idx) == 0 or soma[idx[0]] != 0:
                        continue
                    for i in idx:
                        path, dist = morpho.path_and_distance_of(sec[i], seg[i])
                        distances[i] = dist
                        for j in idx:
                            if i == j:
                                break
                            if activated[j] and sec[j] in path and \
                                    abs(distances[i] - distances[j]) < mdist:
                                activated[j] = False
                        activated[i] = True
            # Activate reciprocal connections
            activated[numpy.isin(jid1, jid2[activated])] = True
            return data[activated]
        return trim_touches

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
            accept = match_dendrites(data.src.values,
                                     data.dst.values,
                                     data.pre_section.values,
                                     data.pre_segment.values,
                                     data.pre_junction.values,
                                     data.post_section.values,
                                     data.post_segment.values,
                                     data.post_junction.values).astype(bool)
            return data[accept]
        return match_touches


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, morphos, stats):
        self.synapses_distance = recipe.synapses_distance

    def apply(self, circuit):
        """Apply filter
        """
        # Use broadcast of Neuron version
        return circuit.df.where("(distance_soma >= %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma >= %f AND dst_syn_class_i = %d)" % (
                                    self.synapses_distance.inhibitorySynapsesDistance,
                                    CellClass.INH.fzer_index,
                                    self.synapses_distance.excitatorySynapsesDistance,
                                    CellClass.EXC.fzer_index)
                                )


# -------------------------------------------------------------------------------------------------
class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """
    Reverse version of Bouton Distance filter, only keeping outliers.
    """

    _visible = False

    def apply(self, circuit):
        return circuit.df.where("(distance_soma < %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma < %f AND dst_syn_class_i = %d)" % (
                                    self.synapses_distance.inhibitorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_INH],
                                    self.synapses_distance.excitatorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_EXC])
                                )


# -------------------------------------------------------------------------------------------------
class TouchRulesFilter(DatasetOperation):
    """Filter touches based on recipe rules

    Defined in the recipe as `TouchRules`, restrict connections between
    mtypes, types (dendrite/soma), and layers.  Any touches not allowed are
    removed.

    This filter is deterministic.
    """

    _checkpoint = True

    def apply(self, circuit):
        """ .apply() method (runner) of the filter
        """
        indices = list(circuit.touch_rules.shape)
        for i in reversed(range(len(indices) - 1)):
            indices[i] *= indices[i + 1]

        rdd = sm.parallelize(((i,)
                              for i, v in enumerate(circuit.touch_rules.flatten())
                              if v == 0), 200)
        rules = sm.createDataFrame(rdd, schema=["fail"])
        # For each neuron we require:
        # - preLayer
        # - preMType
        # - postLayer
        # - postMType
        # - postBranchType
        #
        # The first four fields are properties of the neurons, part of
        # neuronDF, while postBranchType is a property if the touch,
        # historically checked by the index of the target neuron
        # section (0->soma)
        touches = circuit.df
        if not hasattr(circuit.df, 'branch_type'):
            touches = touches.withColumn('branch_type',
                                         (touches.post_section > 0).cast('integer'))
        return touches.withColumn("fail",
                                  touches.src_layer_i * indices[1] +
                                  touches.dst_layer_i * indices[2] +
                                  touches.src_mtype_i * indices[3] +
                                  touches.dst_mtype_i * indices[4] +
                                  touches.branch_type) \
                      .join(F.broadcast(rules), "fail", "left_anti") \
                      .drop("fail")


# -------------------------------------------------------------------------------------------------
class ReduceAndCut(DatasetOperation):
    """Reduce and cut touch distributions

    Goes through the touches and matches up distributions present and
    expected by random sampling. Steps:

    1. Pathway statistics are determined and reduction factors are
       calculated

       Calulate `pP_A`, `pMu_A`, bouton reduction factor, and legacy
       active fraction based on the number of touches per pathway and
       pathway count.

    2. Reduction factors are applied to the touches

       Based on `pP_A` calculated previously, with random numbers drawn
       for sampling.

    3. Survival rates of the remaining touches are calculated

       Trim src-dst connections based on the survival of the previous
       step, and relying on `pMu_A`, calculates a survival rate and keeps
       "surviving" connections by sampling.

    4. Active connection fractions are deduced from survival rates and
       applied

       Using the `bouton_reduction_factor` from the `ConnectionRules`
       part of the recipe to determine the overall fraction of the
       touches that every mtype--mtype connection class is allowed to
       have active.

    To calculate random numbers, a seed derived from the `synapseSeed` in
    the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = True
    _checkpoint_buckets = ("src", "dst")

    def __init__(self, recipe, morphos, stats):
        self.recipe = recipe
        self.stats = stats

        if len(self.recipe.conn_rules) == 0:
            raise RuntimeError("No connection rules loaded. Please check the recipe.")

    def apply(self, circuit):
        """Filter the circuit according to the logic described in the
        class.
        """
        full_touches = Circuit.only_touch_columns(touches_with_pathway(circuit.df))
        mtypes = circuit.mtype_df
        conn_rules = self._build_concrete_mtype_conn_rules(self.recipe.conn_rules,
                                                           circuit.morphology_types)
        self.conn_rules = sm.broadcast(conn_rules)

        # Get and broadcast Pathway stats
        # NOTE we cache and count to force evaluation in N tasks, while sorting in a single task
        logger.debug("Computing Pathway stats...")
        _params = self.compute_reduce_cut_params(full_touches)
        params_df = F.broadcast(_params)

        # Params ouput for validation
        _params_out_csv(params_df, "pathway_params", mtypes)

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

    @staticmethod
    def _build_concrete_mtype_conn_rules(src_conn_rules, mTypes):
        """ Transform conn rules into concrete rule instances (without wildcards) and indexed by pathway
        """
        mtypes_rev = {mtype: i for i, mtype in enumerate(mTypes)}
        conn_rules = {}

        for rule in src_conn_rules:  # type: ConnectivityPathRule
            srcs = matchfilter(mTypes, rule.source)
            dsts = matchfilter(mTypes, rule.destination)
            for src in srcs:
                for dst in dsts:
                    # key = src + ">" + dst
                    # Key is now an int
                    key = (mtypes_rev[src] << 16) + mtypes_rev[dst]
                    if key in conn_rules:
                        # logger.debug("Several rules applying to the same mtype connection: %s->%s [Rule: %s->%s]",
                        #                src, dst, rule.source, rule.destination)
                        prev_rule = conn_rules[key]
                        # Overwrite if it is specific
                        if (('*' in prev_rule.source and '*' not in rule.source) or
                                ('*' in prev_rule.destination and '*' not in rule.destination)):
                            conn_rules[key] = rule
                    else:
                        conn_rules[key] = rule

        return conn_rules

    # ---
    @sm.assign_to_jobgroup
    @checkpoint_resume("pathway_stats", bucket_cols="pathway_i", n_buckets=1, child=True)
    def compute_reduce_cut_params(self, full_touches):
        """Computes pathway parameters for reduce and cut

        Based on the number of touches per pathway and the total number of
        connections (unique pathways).

        :param full_touches: touches with a pathway column
        :return: a dataframe containing reduce and cut parameters
        """
        # First obtain the pathway (morpho-morpho) stats dataframe
        _n_parts = max(full_touches.rdd.getNumPartitions() // 20, 100)
        pathway_stats = (self.stats
            .get_pathway_touch_stats_from_touches_with_pathway(full_touches)
            .coalesce(_n_parts))

        # param create udf
        rc_param_maker = reduce_cut_parameter_udf(self.conn_rules)

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
                       handlers=[CheckpointHandler.before_save(Circuit.only_touch_columns)],
                       child=True)
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
            "reduce_rand", self.recipe.seeds.synapseSeed, _KEY_REDUCE,
            F.col("synapse_id")
        ).where(F.col("pP_A") > F.col("reduce_rand")) \
         .drop("reduce_rand", "pP_A") \
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
                 F.min("synapse_id").alias("synapse_id"),
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
        # Debug
        _params_out_csv(params_df_sigma, "survival_params", mtypes)

        connection_survival_rate = (
            reduced_touch_counts_connection
            .join(params_df_sigma, "pathway_i")  # Fetch the pathway params
            .withColumn("survival_rate",  # Calc survivalRate
                        F.expr("1.0 / (1.0 + exp((-4.0/sigma) * (reduced_touch_counts_connection-pMu_A)))"))
            .drop("sigma", "pMu_A")
        )

        # Deactivated due to large output size.
        # _dbg_df = connection_survival_rate.select("pathway_i", "reduced_touch_counts_connection", "survival_rate")
        # _write_csv(pathway_i_to_str(_dbg_df.groupBy("pathway_i").count(), mtypes),
        #            "connection_survival_rate.csv")

        logger.debug(" -> Computing connections to cut according to survival_rate")
        _df = connection_survival_rate
        cut_connections = _add_random_column(
            _df, "cut_rand", self.recipe.seeds.synapseSeed, _KEY_CUT,
            F.col("synapse_id"),
        ).where((_df.survival_rate > .0) & (_df.survival_rate > F.col("cut_rand"))) \
         .select("src", "dst", "synapse_id", "pathway_i", "reduced_touch_counts_connection")
        # Much smaller data volume but we cant coealesce
        return cut_connections

    # ----
    @sm.assign_to_jobgroup
    @checkpoint_resume("shall_keep_connections", bucket_cols=("src", "dst"), child=True)
    def calc_cut_active_fraction(self, cut_touch_counts_connection, params_df, mtypes):
        """Cut according to the active_fractions

        Args:
            params_df: the parameters DF (pA, uA, active_fraction_legacy)
            cut_touch_counts_connection: the DF with the cut touch counts
                per connection (built previously in an optimized way)
        Returns:
            The final cut touches
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
            "active_rand", self.recipe.seeds.synapseSeed, _KEY_ACTIVE,
            F.col("synapse_id"),
        ).where(F.col("active_rand") < F.col("active_fraction")) \
         .select("src", "dst")

        return shall_keep_connections


def _add_random_column(df, name, seed, key, derivative):
    """Add a random column to a dataframe

    :param df: the dataframe to augment
    :param name: name for the random column
    :param seed: the seed to use for the RNG
    :param key: first key to derive the RNG with
    :param derivative: column for second derivative of the RNG
    :return: the dataframe with a random column
    """
    @F.pandas_udf('float')
    def _fixed_rand(col):
        rng = RNGThreefry().seed(seed).derivate(key)
        return pandas.Series(uniform(rng, col.values))

    return df.withColumn(name, _fixed_rand(derivative.cast(T.LongType())))


def _params_out_csv(df, filename, mtypes):
    of_interest = ("pathway_i", "total_touches", "structural_mean",
                   "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
    cols = []
    for col in of_interest:
        if hasattr(df, col):
            cols.append(col)
    debug_info = df.select(*cols)
    _write_csv(pathway_i_to_str(debug_info, mtypes), filename)


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


class CSVWriter:
    """Helper class to debug via CSV dumps
    """
    def __init__(self):
        if not os.path.isdir("_debug"):
            os.makedirs("_debug")
        self._stage = 1

    def __call__(self, df, filename):
        """Write out a CSV file of a dataframe
        """
        end = "" if filename.endswith(".csv") else ".csv"
        filename = f"_debug/{self._stage:02d}_{filename}{end}"

        data = df.toPandas()
        data.to_csv(filename, index=False)

        self._stage += 1


def _write_csv(df, filename):
    pass


def enable_debug():
    global _DEBUG
    global _write_csv
    logger.info("Activating debug output...")
    _DEBUG = True
    _write_csv = CSVWriter()


# -------------------------------------------------------------------------------------------------
class CumulativeDistanceFilter(DatasetOperation):
    """
    Filtering based on cumulative distances (By D. Keller - BLBLD-29)
    """
# -------------------------------------------------------------------------------------------------

    def __init__(self, recipe, morphos, stats):
        self.recipe = recipe
        self.stats = stats

    def apply(self, circuit):
        """Returns the touches of the circuit: NOOP
        """
        return circuit

    def filterInhSynapsesBasedOnThresholdInPreSynapticData(self):
        pass

    def filterExcSynapsesBasedOnDistanceInPreSynapticData(self):
        pass


class SynapseProperties(DatasetOperation):
    """Assign synapse properties

    This "filter" augments touches with properties of synapses by

    * shifting the post-section of synapses for ChC and SpAA cells to the
      soma according to the `SynapsesReposition` rules of the recipe.
    * adding the fields

      - `gsyn` following a Gamma-distribution,
      - `d` following a Gamma-distribution,
      - `f` following a Gamma-distribution,
      - `u` following a truncated Normal-distribution,
      - `dtc` following a truncated Normal-distribution,
      - `nrrp` following a Poisson-distribution

      as specified by the `SynapsesClassification` part of the recipe.

    To draw from the distributions, a seed derived from the `synapseSeed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = True
    _morphologies = True

    def __init__(self, recipe, morphos, stats):
        self.recipe = recipe

    def apply(self, circuit):
        from ..synapse_properties import patch_ChC_SPAA_cells
        from ..synapse_properties import compute_additional_h5_fields

        if self._morphologies:
            circuit.df = patch_ChC_SPAA_cells(circuit.df,
                                              circuit.morphologies,
                                              circuit.synapse_reposition_pathways)

        extended_touches = compute_additional_h5_fields(
            circuit.df,
            circuit.reduced,
            circuit.synapse_class_matrix,
            circuit.synapse_class_properties,
            self.recipe.seeds.synapseSeed
        )
        return extended_touches
