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
from .utils.spark import checkpoint_resume, number_shuffle_partitions
from .filter_udfs import reduce_cut_parameter_udef

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False
_DEBUG_REDUCE = False
_DEBUG_CUT = False
_DEBUG_CUT2AF = False
_MB = 1024*1024


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

    def __init__(self, conn_rules, stats, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.conn_rules = self.sc.broadcast(conn_rules)
        self.stats = stats

    # ---
    def apply(self, circuit, *args, **kw):
        full_touches = Circuit.only_touch_columns(touches_with_pathway(circuit))

        # Get and broadcast Pathway stats
        # NOTE we cache and count to force evaluation in N tasks, while sorting in a single task
        logger.info("Computing Pathway stats...")
        _params = self.compute_reduce_cut_params(full_touches)
        params_df = F.broadcast(_params)

        # Debug params info -----------------------------------------------------------------------
        if _DEBUG and "mtypes" not in kw:
            logger.warning("Cant debug without mtypes df. Please provide as kw arg to apply()")
        if _DEBUG and _DEBUG_REDUCE:
            debug_info = params_df.select("pathway_i", "total_touches", "structural_mean",
                                          "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
            debug_info.show()
            debug_info.write.csv("_debug/pathway_params.csv", header=True, mode="overwrite")
        # -----------------------------------------------------------------------------------------

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Reduce
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Reduce step...")
        reduced_touches = self.apply_reduce(full_touches, params_df)

        if _DEBUG and _DEBUG_REDUCE:  # -----------------------------------------------------------
            (reduced_touches
             .groupBy("pathway_i").count()
             .coalesce(1)
             .write.csv("_debug/reduce_counts.csv", header=True, mode="overwrite"))
        # -----------------------------------------------------------------------------------------

        logger.info("Computing reduced touch counts")
        with sm.jobgroup("Computing reduced touch counts", ""):
            reduced_touch_counts_connection = (
                reduced_touches
                .groupBy("src", "dst")
                .agg(F.first("pathway_i").alias("pathway_i"),
                     F.count("src").alias("reduced_touch_counts_connection"))
            )

            # Make this computation resumable.
            # Compute/Resume and Checkpoint from TABLE (which preserves partitioning)
            f = checkpoint_resume("reduced_conn_counts", bucket_cols=("src", "dst"))(
                lambda: reduced_touch_counts_connection
            )
            reduced_touch_counts_connection = f()

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Cut
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Cut step...")
        logger.debug(" -> Calculating cut touches and resulting connection counts...")
        cut_touches, cut_touch_counts_connection = (
            self.apply_cut(reduced_touches,
                           params_df,
                           reduced_touch_counts_connection,
                           mtypes=kw["mtypes"])
        )

        # cut_touch_counts_connection is only an exec plan (no actions within)
        # We can use checkpoint_resume on it
        # Bucketing is to be done by pathway, to optimize subsequent queries. Bucket count should
        # still be optimal since pathway is directly dependent on src-dst ids
        f = checkpoint_resume("cut_conn_counts", bucket_cols=("src", "dst"))(
            lambda: cut_touch_counts_connection
        )
        cut_touch_counts_connection = f()

        if _DEBUG and _DEBUG_CUT:  # --------------------------------------------------------------
            cut_touch_counts_pathway = (
                cut_touch_counts_connection
                .groupBy("pathway_i")
                .agg(F.sum("reduced_touch_counts_connection").alias("cut_touch_counts_pathway"))
            )
            pathway_i_to_str(cut_touch_counts_pathway, kw["mtypes"])\
                .coalesce(1)\
                .write.csv("_debug/cut_counts.csv", header=True, mode="overwrite")
            logger.warning("Debugging: Execution terminated for debugging Cut")
        # -----------------------------------------------------------------------------------------

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Cut 2: by Active Fraction
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Cut step part 2: Active Fraction...")
        cut2AF_touches = self.apply_cut_active_fraction(cut_touches, params_df,
                                                        cut_touch_counts_connection,
                                                        mtypes=kw["mtypes"])
        if _DEBUG and _DEBUG_CUT2AF:  # -----------------------------------------------------------
            cut2AF_touches = cut2AF_touches.checkpoint(False)
            (cut2AF_touches
                .groupBy("pathway_i").count()
                .coalesce(1)
                .write.csv("_debug/cut2af_counts.csv", header=True, mode="overwrite"))
        # -----------------------------------------------------------------------------------------

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
        n_parts_shuffle = max(full_touches.rdd.getNumPartitions() // 20, 100)
        pathway_stats = (self.stats
                         .get_pathway_touch_stats_from_touches_with_pathway(full_touches)
                         .coalesce(n_parts_shuffle))

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
        return all_touches.sampleBy("pathway_i", fractions).repartition("src", "dst")

    # ---
    # Note: apply_cut is not checkpointed since it
    #       builds up with apply_cut_active_fraction filter
    @staticmethod
    @sm.assign_to_jobgroup
    def apply_cut(reduced_touches, params_df, reduced_touch_counts_connection, **kw):
        """
        Apply cut filter
        Cut computes a survivalRate and activeFraction for each post neuron
        And filters out those not passing the random test
        """
        params_df_sigma = (
            params_df
            .where(F.col("pMu_A").isNotNull())
            .select("pathway_i", "pMu_A")
            .withColumn("sigma", params_df.pMu_A / 4)
        )

        connection_survival_rate = (
            reduced_touch_counts_connection
            .join(params_df_sigma, "pathway_i")  # Fetch the pathway params
            .withColumn("survival_rate",         # Calc survivalRate
                        F.expr("1.0 / (1.0 + exp((-4.0/sigma) * (reduced_touch_counts_connection-pMu_A)))"))
            .where("survival_rate < 1.0")
            .drop("sigma", "pMu_A")
        )

        if _DEBUG and _DEBUG_CUT:
            _dbg_df = connection_survival_rate\
                .select("src", "dst", "reduced_touch_counts_connection", "survival_rate")
            _dbg_df.show()
            _dbg_df.coalesce(1).write.csv("_debug/connection_survival_rate.csv", header=True, mode="overwrite")

        # --- COMPUTING STRATEGY ------
        # Shall cut gets us the list of connections which shall be removed -> always 0 count -> little mem (12b /entry)
        # We later reuse this to recompute the updated touch count
        # It should be also quite fast to drop connections given both DFs are partitioned by pathway_i

        logger.debug(" -> Computing connections to cut according to survival_rate")
        shall_cut = (
            connection_survival_rate
            .where(F.rand() >= F.col("survival_rate"))
            .select("src", "dst")
        )
        # Checkpoint resume this
        shall_cut = checkpoint_resume("shall_cut", bucket_cols=("src", "dst"))(lambda: shall_cut)()

        cut_touches = (reduced_touches
                       .join(shall_cut, ["src", "dst"], how="left_anti"))

        # Calc cut_touch_counts_connection
        # Since we cut full connections, it is enough to exclude them from the counts
        # This shall not shuffle since both DF are bucketed&sorted by src-dst
        cut_touch_counts_connection = (reduced_touch_counts_connection
                                       .join(shall_cut, ["src", "dst"], how="left_anti"))

        return cut_touches, cut_touch_counts_connection

    # ----
    @staticmethod
    @sm.assign_to_jobgroup
    # Note: Filter not checkpointed since it's the last stage
    #       of global filtering (which is checkpointed)
    def apply_cut_active_fraction(cut_touches, params_df, cut_touch_counts_connection, **kw):
        """
        Performs the second part of the cut algorithm according to the active_fractions
        :param cut_touches: The cut (phase1) touches (probably not materialized yet)
        :param params_df: The parameters DF (pA, uA, active_fraction_legacy)
        :param cut_touch_counts_connection: The DF with the cut touch counts per connection \
               (built previously in an optimized way)
        :return: The final cut touches
        """
        _n_parts = max(cut_touch_counts_connection.rdd.getNumPartitions() // 10, 100)
        with number_shuffle_partitions(_n_parts):
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
            .cache()
        )

        # This is a minimal DS so we cache and broadcast in single partition
        active_fractions.count()
        active_fractions = F.broadcast(active_fractions.coalesce(1))

        shall_cut2 = (
            cut_touch_counts_connection
            .join(active_fractions, "pathway_i")
            .where(F.rand() >= F.col("active_fraction"))
            .select("src", "dst")
        )

        f = checkpoint_resume("shall_cut2", bucket_cols=("src", "dst"))(
            lambda: shall_cut2
        )
        shall_cut2 = f()

        if _DEBUG and _DEBUG_CUT2AF:
            active_fractions = pathway_i_to_str(active_fractions, kw["mtypes"])
            active_fractions.show()
            active_fractions.coalesce(1).write.csv("_debug/active_fractions.csv", header=True, mode="overwrite")
            sys.exit(0)

        cut2AF_touches = (
            cut_touches
            .join(shall_cut2, ["src", "dst"], how="left_anti")
        )

        return cut2AF_touches


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
