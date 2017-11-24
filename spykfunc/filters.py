from __future__ import division

import os, sys
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel
from .definitions import CellClass
from .schema import to_pathway_i, pathway_i_to_str
from ._filtering import DataSetOperation
from .utils import get_logger
from .filter_udfs import reduce_cut_parameter_udef

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False
_DEBUG_REDUCE = False
_DEBUG_CUT = False
_DEBUG_CUT2AF = False


if _DEBUG:
    os.path.isdir("_debug") or os.makedirs("_debug")


# --------------------------------------------------------------------------------------------------------------------
class BoutonDistanceFilter(DataSetOperation):
    """
    Class implementing filtering by Bouton Distance (min. distance to soma)
    """
# --------------------------------------------------------------------------------------------------------------------
    def __init__(self, bouton_distance_obj):
        self._bouton_distance_obj = bouton_distance_obj

    def apply(self, neuronG, *args, **kw):
        # neuronDF = F.broadcast(neuronG.vertices)
        neuronDF = neuronG.vertices
        touchDF = neuronG.edges

        # Use broadcast of Neuron version
        newTouches = touchDF.alias("t").join(neuronDF.alias("n"), neuronDF.id == touchDF.dst) \
            .where("(t.distance_soma >= %f AND n.syn_class_index = %d) OR "
                   "(t.distance_soma >= %f AND n.syn_class_index = %d)" % (
                       self._bouton_distance_obj.inhibitorySynapsesDistance,
                       CellClass.INH.fzer_index,
                       self._bouton_distance_obj.excitatorySynapsesDistance,
                       CellClass.EXC.fzer_index)
                   ) \
            .select("t.*")
        return newTouches


# --------------------------------------------------------------------------------------------------------------------
class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """
    Reverse version of Bouton Distance filter, only keeping outliers.
    """
# --------------------------------------------------------------------------------------------------------------------

    def apply(self, neuronG, *args, **kw):
        neuronDF = neuronG.vertices
        touchDF = neuronG.edges

        new_touches = touchDF.alias("t").join(neuronDF.alias("n"), neuronDF.id == touchDF.dst) \
            .where("(t.distance_soma < %f AND n.syn_class_index = %d) OR "
                   "(t.distance_soma < %f AND n.syn_class_index = %d)" % (
                       self._bouton_distance_obj.inhibitorySynapsesDistance,
                       self.synapse_classes_indexes[CellClass.CLASS_INH],
                       self._bouton_distance_obj.excitatorySynapsesDistance,
                       self.synapse_classes_indexes[CellClass.CLASS_EXC])
                   ) \
            .select("t.*")
        return new_touches


# --------------------------------------------------------------------------------------------------------------------
class TouchRulesFilter(DataSetOperation):
    """
    Class implementing TouchRules filter.
    """
# --------------------------------------------------------------------------------------------------------------------

    def __init__(self, recipe_touch_rules):
        self._rules = recipe_touch_rules

    def apply(self, neuronG, *args, **kw):
        """ .apply() method (runner) of the filter
        """
        # For each neuron we require: preLayer, preMType, postLayer, postMType, postBranchIsSoma
        # The first four fields are properties of the neurons, part of neuronDF,
        #  while postBranchIsSoma is a property if the touch, checked by the index of the target neuron section (0->soma)
        sql_queries = []

        for rule in self._rules:
            rule_sqls = []
            if rule.fromLayer:
                rule_sqls.append("n1.layer={}".format(rule.fromLayer))
            if rule.toLayer:
                rule_sqls.append("n2.layer={}".format(rule.toLayer))
            if rule.fromMType:
                rule_sqls.append("n1.morphology LIKE '{}'".format(rule.fromMType.replace("*", "%")))
            if rule.toMType:
                rule_sqls.append("n2.morphology LIKE '{}'".format(rule.toMType.replace("*", "%")))
            if rule.type:
                rule_sqls.append("t.post_section {sign} 0".format(sign="=" if (rule.type == "soma") else ">"))

            if rule_sqls:
                sql_queries.append("(" + " AND ".join(rule_sqls) + ")")

        master_filter_sql = " OR ".join(sql_queries)

        new_touches = neuronG.find("(n1)-[t]->(n2)") \
            .where(master_filter_sql) \
            .select("t.*")

        return new_touches


# --------------------------------------------------------------------------------------------------------------------
class ReduceAndCut(DataSetOperation):
    """
    Class implementing ReduceAndCut
    """
# --------------------------------------------------------------------------------------------------------------------

    def __init__(self, conn_rules, stats, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.conn_rules = self.sc.broadcast(conn_rules)
        self.stats = stats

    # ---
    def apply(self, neuronG, *args, **kw):
        # Get and broadcast reduce and count
        logger.info("Computing Pathway stats...")
        rc_params_df = self.compute_reduce_cut_params()

        params_df = F.broadcast(rc_params_df.cache().checkpoint())

        # Debug params info ------------------------------------------------------------------------
        if _DEBUG and "mtypes" not in kw:
            logger.warning("Cant debug without mtypes df. Please provide as kw arg to apply()")
        if _DEBUG and _DEBUG_REDUCE:
            debug_info = params_df.select("pathway_i", "total_touches", "structural_mean",
                                          "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
            debug_info.show()
            debug_info.write.csv("_debug/pathway_params.csv", header=True, mode="overwrite")
        # -----------------------------------------------------------------------------------------

        # Flatten GraphFrame and include pathway (morpho>morpho) column
        # apply_reduce and apply_cut require touches with the schema as created here
        full_touches = (neuronG.find("(n1)-[t]->(n2)")
                        .withColumn("pathway_i", to_pathway_i("n1.morphology_i", "n2.morphology_i"))
                        .select("pathway_i", "t.*"))  # Only pathway and touch

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Reduce
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Reduce step...")
        reduced_touches = self.apply_reduce(full_touches, params_df)

        # PERSIST RESULTS (to parquet while checkpoint() is not an option)
        logger.debug(" -> Checkpointing...")
        reduced_touches.write.parquet("_tmp/cut_touches.parquet", mode="overwrite")
        reduced_touches = self.spark.read.parquet("_tmp/cut_touches.parquet")

        if _DEBUG and _DEBUG_REDUCE:
            (reduced_touches
                .groupBy("pathway_i").count()
                .coalesce(1)
                .write.csv("_debug/reduce_counts.csv", header=True, mode="overwrite"))

        logger.debug(" -> Computing reduced touch counts")
        reduced_touch_counts_connection = (
            reduced_touches
                .groupBy(F.col("pathway_i"),
                         F.col("src"),  # Pre Neuron id
                         F.col("dst"),  # Post Neuron id
                         )
                .count()
                .withColumnRenamed("count", "reduced_touch_counts_connection")
                .checkpoint()
        )

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Cut
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Cut step...")
        cut_touches, cut_touch_counts_connection = self.apply_cut(
            reduced_touches,
            params_df,
            reduced_touch_counts_connection,
            mtypes=kw["mtypes"])

        if _DEBUG and _DEBUG_CUT:
            cut_touch_counts_pathway = (cut_touch_counts_connection
                .groupBy("pathway_i")
                .agg(F.sum("reduced_touch_counts_connection").alias("cut_touch_counts_pathway")))
            pathway_i_to_str(cut_touch_counts_pathway, kw["mtypes"])\
                .coalesce(1)\
                .write.csv("_debug/cut_counts.csv", header=True, mode="overwrite")
            logger.warning("Debugging: Execution terminated for debugging Cut")
            sys.exit(0)

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # Cut 2: by Active Fraction
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        logger.info("Applying Cut step part 2: Active Fraction...")
        cut2AF_touches = self.apply_cut_active_fraction(cut_touches, params_df, cut_touch_counts_connection, mtypes=kw["mtypes"])

        if _DEBUG and _DEBUG_CUT2AF:
            # We only checkpoint here if debugging, otherwise processing goes on until saved as extended touches
            cut2AF_touches = cut2AF_touches.checkpoint(False)
            (cut2AF_touches
                .groupBy("pathway_i").count()
                .coalesce(1)
                .write.csv("_debug/cut2af_counts.csv", header=True, mode="overwrite"))

        # Only the touch fields
        return cut2AF_touches.select(neuronG.edges.schema.names)

    # ---
    def compute_reduce_cut_params(self):
        """
        Computes the pathway parameters, used by Reduce and Cut filters
        """
        # First obtain the pathway (morpho-morpho) stats dataframe
        mtype_stats = self.stats.mtype_touch_stats.repartition("pathway_i")

        # param create udf
        rc_param_maker = reduce_cut_parameter_udef(self.conn_rules)

        # Run UDF
        params_df = mtype_stats.select(
            "*",
            rc_param_maker(mtype_stats.pathway_i, mtype_stats.average_touches_conn)
                .alias("rc_params")
        )

        # Return the interesting params
        return params_df.select("pathway_i",
                                "total_touches",
                                F.col("average_touches_conn").alias("structural_mean"),
                                "rc_params.*")

    # ---
    @staticmethod
    def apply_reduce(all_touches, params_df):
        """Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a pathway (morpho-morpho association)
        logger.debug(" -> Building reduce fractions")
        fractions = params_df.select("pathway_i", "pP_A").rdd.collectAsMap()

        logger.debug(" -> Cutting touches")
        return all_touches.sampleBy("pathway_i", fractions)

    # ---
    @staticmethod
    def apply_cut(reduced_touches, params_df, reduced_touch_counts_connection, **kw):
        """
        Apply cut filter
        Cut computes a survivalRate and activeFraction for each post neuron
        And filters out those not passing the random test
        """
        params_df_sigma = (params_df
            .where(F.col("pMu_A").isNotNull())
            .select("pathway_i", "pMu_A")
            .withColumn("sigma", params_df.pMu_A / 4))

        connection_survival_rate = (
            reduced_touch_counts_connection
            # params_df_sigma is broadcasted -> ok the partitioning is different
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

        logger.debug(" -> Connections to cut according to survival_rate")
        shall_cut = (
            connection_survival_rate
            .where(F.rand() >= F.col("survival_rate"))
            .select("pathway_i", "src", "dst")
            .cache()  # This DF is used in two different calculations
        )

        cut_touches = (reduced_touches
                       .join(shall_cut, ["pathway_i", "src", "dst"], how="left_anti"))

        # Calc cut_touch_counts_connection
        cut_touch_counts_connection = (
            reduced_touch_counts_connection
            .join(shall_cut, ["pathway_i", "src", "dst"], how="left_anti")
        )

        return cut_touches, cut_touch_counts_connection

    # ----
    @staticmethod
    def apply_cut_active_fraction(cut_touches, params_df, cut_touch_counts_connection, **kw):
        """
        Performs the second part of the cut algorithm according to the active_fractions
        :param cut_touches: The cut (phase1) touches (probably not materialized yet)
        :param params_df: The parameters DF (pA, uA, active_fraction_legacy)
        :param cut_touch_counts_connection: The DF with the cut touch counts per connection (built previously in an optimized way)
        :return: The final cut touches
        """
        logger.debug(" -> Calculating connection counts after cut")
        cut_touch_counts_connection = cut_touch_counts_connection.localCheckpoint()

        active_fractions = (
            cut_touch_counts_connection
            .groupBy("pathway_i")
            .agg(F.sum("reduced_touch_counts_connection").alias("cut_touch_counts_pathway"))
            .join(params_df, "pathway_i")
            .withColumn("actual_reduction_factor",
                F.col("cut_touch_counts_pathway") / F.col("total_touches")
            )
            .withColumn("active_fraction",
                F.when(F.col("bouton_reduction_factor").isNull(),
                    F.col("active_fraction_legacy")
                ).otherwise(
                    F.when(F.col("bouton_reduction_factor") > F.col("actual_reduction_factor"),
                        F.lit(1.0)
                    ).otherwise(
                        F.col("bouton_reduction_factor") / F.col("actual_reduction_factor")
                    )
                )
            )
            .select("pathway_i", "active_fraction")
        )

        shall_cut2 = (
            cut_touch_counts_connection
            .join(F.broadcast(active_fractions), "pathway_i")
            .where(F.rand() >= F.col("active_fraction"))
            # Pathway_i is kept in order to make a join with the same keys as the partitioning
            .select("pathway_i", "src", "dst")
        )

        if _DEBUG and _DEBUG_CUT2AF:
            active_fractions = pathway_i_to_str(active_fractions, kw["mtypes"])
            active_fractions.show()
            active_fractions.coalesce(1).write.csv("_debug/active_fractions.csv", header=True, mode="overwrite")
            sys.exit(0)

        cut2AF_touches = (
            cut_touches
            .join(shall_cut2, ["pathway_i", "src", "dst"], how="left_anti")
        )

        return cut2AF_touches


# --------------------------------------------------------------------------------------------------------------------
class CumulativeDistanceFilter(DataSetOperation):
    """
    Filtering based on cumulative distances (By D. Keller - BLBLD-29)
    """
# --------------------------------------------------------------------------------------------------------------------

    def __init__(self, recipe, stats):
        self.recipe = recipe
        self.stats = stats

    def apply(self, neuronG, *args, **kw):
        touches = neuronG.edges
        return touches  # no-op

    def filterInhSynapsesBasedOnThresholdInPreSynapticData(self):
        pass

    def filterExcSynapsesBasedOnDistanceInPreSynapticData(self):
        pass
