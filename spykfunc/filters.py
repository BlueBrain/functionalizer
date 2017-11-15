from __future__ import division

import os
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel
from .definitions import CellClass
from ._filtering import DataSetOperation
from .utils import get_logger
from .filter_udfs import reduce_cut_parameter_udef

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = True

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

    def __init__(self, conn_rules, stats, spark_context):
        self.sc = spark_context
        self.conn_rules = spark_context.broadcast(conn_rules)
        self.stats = stats

    # ---
    def apply(self, neuronG, *args, **kw):
        rc_params_df = self.compute_reduce_cut_params()

        params_df = F.broadcast(rc_params_df
                                .withColumn("pathway", self.to_pathway_str("n1_morpho", "n2_morpho"))
                                .cache()).coalesce(1)

        # show params info
        if _DEBUG:
            debug_info = params_df.select("pathway", "total_touches", "structural_mean",
                                          "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
            debug_info.show(1000)
            debug_info.write.csv("_debug/pathway_params.csv", header=True, mode="overwrite")

        # Flatten GraphFrame and include pathway (morpho>morpho) column
        # apply_reduce and apply_cut require touches with the schema as created here
        full_touches = (neuronG.find("(n1)-[t]->(n2)")
                        .withColumn("pathway", self.to_pathway_str("n1.morphology", "n2.morphology"))
                        .select("pathway", "t"))  # Only pathway and touch

        # Reduce
        logger.info("Applying Reduce step...")
        reduced_touches = self.apply_reduce(full_touches, params_df)\
            .checkpoint()
        if _DEBUG: (reduced_touches
                    .groupBy("pathway").count()
                    .write.csv("_debug/reduce_counts.csv", header=True, mode="overwrite"))

        # Cut
        logger.info("Applying Cut step...")
        cut_touches = self.apply_cut(reduced_touches, params_df)
        if _DEBUG:
            cut_touches = (cut_touches
                # We only checkpoint here if debugging, otherwise processing goes on until saved as extended touches
                .checkpoint()
                .groupBy("pathway").count()
                .write.csv("_debug/reduce_counts.csv", header=True, mode="overwrite"))

        # Only the touch fields
        return cut_touches.select("t.*")

    # ---
    @staticmethod
    def to_pathway_str(col1, col2):
        # TODO: Instead of string, eventually compute an integer -> should compare faster
        return F.concat(F.col(col1), F.lit(">"), F.col(col2))

    # ---
    def compute_reduce_cut_params(self):
        # First obtain the morpho-morpho stats dataframe
        #   We use mtype_touch_stats which is cached. It requires a huge shuffle anyway
        #   so we can cache (considering we run on systems with much larger mem, e.g. 2M neurons produce a 40GB DF)
        mtype_stats = self.stats.mtype_touch_stats

        # Materializing MType Assoc counts now, it was cached
        logger.info("Computing Pathway stats...")
        len_mtype_stats = mtype_stats.count()
        logger.debug("Number of Mtype Associations: %d", len_mtype_stats)

        # param create udf
        rc_param_maker = reduce_cut_parameter_udef(self.conn_rules)

        # NOTE: Case of Pathway [MType-MType] not in the recipe
        # TODO: Apparently it doesn't cut. We should issue a warning!

        # Extract p_A, mu_A, activeFraction
        params_df = (
            mtype_stats
            .select("*",  # We can see here the fields of mtype_stats, which are used for rc_param_maker
                    rc_param_maker(mtype_stats.n1_morpho, mtype_stats.n2_morpho, mtype_stats.average_touches_conn)
                    .alias("rc_params"))
            .where(F.col("rc_params.pP_A").isNotNull())
        )

        # Return the params
        return params_df.select("n1_morpho", "n2_morpho", "total_touches",
                                F.col("average_touches_conn").alias("structural_mean"),
                                "rc_params.*")

    # ---
    @staticmethod
    def apply_reduce(all_touches, params_df):
        """Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a morpho-morpho association
        fractions = {record.pathway: record.pP_A
                     for record in params_df.select("pathway", "pP_A").collect()}

        return all_touches.sampleBy("pathway", fractions) \

    # ---
    @staticmethod
    def apply_cut(reduced_touches, params_df):
        """
        Apply cut filter
        Cut computes a survivalRate and activeFraction for each post neuron
        And filters out those not passing the random test
        """
        params_df_sigma = params_df.withColumn("sigma", params_df.pMu_A / 4)

        # Compute the touch_counts per morpho_assoc (aka pathway) in post-synaptic view  and related survivalRate
        # This triggers the calculation of reduced_touches
        # The groupBy will incur a large shuffle of the reduced_touches
        post_neuron_touch_counts = (
            reduced_touches
            .groupBy("pathway", F.col("t.dst").alias("post_neuron_id"))
            .count()
            .withColumnRenamed("count", "post_touches_count")
        )

        post_counts_survival_rate = (
            post_neuron_touch_counts
            # Add pMu_A, sigma to post_neuron info
            .join(params_df_sigma.select("pathway", "pMu_A", "sigma"), "pathway")
            # Calc survivalRate
            .withColumn("survival_rate", 1.0 /
                        (1.0 + F.exp((-4 / F.col("sigma")) * (F.col("post_touches_count") - F.col("pMu_A")))))
            .drop("sigma")
        )

        # Connections to keep according to survival_rate
        logger.info("Connections to keep according to survival_rate")
        shall_not_cut = (
            post_counts_survival_rate
            .where(F.col("survival_rate") > F.rand())
            .select("pathway", "post_neuron_id", "post_touches_count")
            .cache()  # This DF is used in two different calculations and occupies little space
        )

        # Calc currentTouchCount per pathway and ActiveFraction
        logger.info("Computing currentTouchCount per pathway and ActiveFraction...")
        updated_post_touch_count = (
            shall_not_cut
            .groupBy("pathway")
            .agg(F.sum(F.col("post_touches_count"))
                 .alias("updated_touches_count")
            )
        )

        logger.debug("Building active_fractions...")
        active_fractions = F.broadcast(updated_post_touch_count
            .join(params_df, "pathway")
            .withColumn("actual_reduction_factor",
                F.col("updated_touches_count") / F.col("total_touches")
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
            .select("pathway", "active_fraction")
        ).cache()

        # Materialize
        active_fractions.count()

        if _DEBUG:
            active_fractions.show(1000)
            active_fractions.write.csv("active_fractions.csv", header=True, mode="overwrite")

        # Connections to keep according to active_fraction
        # NOTE: Until now shall_not_cut is distributed, but the join with active_fractions should be Broadcasted
        logger.info("Building shall_not_cut2")
        shall_not_cut2 = F.broadcast(
            shall_not_cut
            .join(active_fractions, "pathway")
            .where(F.col("active_fraction") > F.rand())
            .select("pathway", "post_neuron_id")
        )

        logger.info("Cutting touches...")
        cut_touches = (
            reduced_touches
            .withColumn("post_neuron_id", F.col("t.dst"))
            .join(shall_not_cut2, ["post_neuron_id", "pathway"])
        )

        return cut_touches.select("pathway", "t")


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
