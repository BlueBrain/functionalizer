from __future__ import division
import os
import sys
from pyspark.sql import functions as F
import sparkmanager as sm
# from pyspark import StorageLevel
from .circuit import Circuit
from .definitions import CellClass, CheckpointPhases
from .schema import pathway_i_to_str, touches_with_pathway
from ._filtering import DataSetOperation
from .utils import get_logger
from .utils.spark import checkpoint_resume, cache_broadcast_single_part
from .filter_udfs import reduce_cut_parameter_udef

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False
_DEBUG_CSV_OUT = False
_MB = 1024 * 1024

if _DEBUG:
    os.path.isdir("_debug") or os.makedirs("_debug")


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
        new_circuit = circuit.where("(distance_soma >= %f AND dst_syn_class_index = %d) OR "
                                    "(distance_soma >= %f AND dst_syn_class_index = %d)" % (
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
        new_circuit = circuit.where("(distance_soma < %f AND dst_syn_class_index = %d) OR "
                                    "(distance_soma < %f AND dst_syn_class_index = %d)" % (
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
                                     circuit.src_morphology_i * self._indices[3] +
                                     circuit.dst_morphology_i * self._indices[4] +
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

    def __init__(self, conn_rules, stats):
        self.conn_rules = sm.broadcast(conn_rules)
        self.stats = stats

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
    @checkpoint_resume("pathway_stats",
                       bucket_cols="pathway_i", n_buckets=1)
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
    @staticmethod
    @sm.assign_to_jobgroup
    @checkpoint_resume(CheckpointPhases.FILTER_REDUCED_TOUCHES.name,
                       bucket_cols=("src", "dst"),
                       # Even if we change to not break exec plan we always keep only touch cols
                       before_save_handler=Circuit.only_touch_columns)
    def apply_reduce(all_touches, params_df):
        """ Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a pathway (morpho-morpho association)
        logger.debug(" -> Building reduce fractions")
        fractions = params_df.select("pathway_i", "pP_A").rdd.collectAsMap()

        logger.debug(" -> Cutting touches")
        return (all_touches
                .sampleBy("pathway_i", fractions)
                .repartition("src", "dst"))

    # ---
    # Note: apply_cut is not checkpointed since it
    #       builds up with apply_cut_active_fraction filter
    @staticmethod
    @sm.assign_to_jobgroup
    def calc_cut_survival_rate(reduced_touches, params_df, mtypes):
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
        cut_connections = (
            _df
            .where((_df.survival_rate > .0) & (_df.survival_rate > F.rand()))
            .select("src", "dst", "pathway_i", "reduced_touch_counts_connection")
        )
        # Much smaller data volume but we cant coealesce
        return cut_connections

    # ----
    @staticmethod
    @sm.assign_to_jobgroup
    @checkpoint_resume("shall_keep_connections", bucket_cols=("src", "dst"))
    def calc_cut_active_fraction(cut_touch_counts_connection, params_df, mtypes):
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

        shall_keep_connections = (
            cut_touch_counts_connection
            .join(active_fractions, "pathway_i")
            .where(F.rand() < F.col("active_fraction"))
            .select("src", "dst")
        )

        return shall_keep_connections


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
