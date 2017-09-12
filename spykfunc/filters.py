from __future__ import division

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel
from math import exp
from .definitions import CellClass
from ._filtering import DataSetOperation
from .utils import get_logger
from filters_math import reduce_cut_parameter_udef

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False


# --------------------------------------------------------------------------------------------------------------------
class BoutonDistanceFilter(DataSetOperation):
    """
    Class implementing filtering by Bouton Distance (min. distance to soma)
    """
# --------------------------------------------------------------------------------------------------------------------
    synapse_classes_by_index_default = [CellClass.CLASS_INH, CellClass.CLASS_EXC]

    def __init__(self, bouton_distance_obj, synapse_classes_by_index=synapse_classes_by_index_default):
        self._bouton_distance_obj = bouton_distance_obj
        self.synapse_classes_indexes = {syn: index for index, syn in enumerate(synapse_classes_by_index)}

    def apply(self, neuronG, *args, **kw):
        # neuronDF = F.broadcast(neuronG.vertices)
        neuronDF = neuronG.vertices
        touchDF = neuronG.edges

        # Use broadcast of Neuron version
        newTouches = touchDF.alias("t").join(neuronDF.alias("n"), neuronDF.id == touchDF.dst) \
            .where("(t.distance_soma >= %f AND n.syn_class_index = %d) OR "
                   "(t.distance_soma >= %f AND n.syn_class_index = %d)" % (
                       self._bouton_distance_obj.inhibitory_synapses_distance,
                       self.synapse_classes_indexes[CellClass.CLASS_INH],
                       self._bouton_distance_obj.excitatory_synapses_distance,
                       self.synapse_classes_indexes[CellClass.CLASS_EXC])
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
                       self._bouton_distance_obj.inhibitory_synapses_distance,
                       self.synapse_classes_indexes[CellClass.CLASS_INH],
                       self._bouton_distance_obj.excitatory_synapses_distance,
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
            # Any rule?
            if rule_sqls:
                sql_queries.append("(" + " AND ".join(rule_sqls) + ")")

        # from pprint import pprint
        # pprint(sql_queries)
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

    def __init__(self, conn_rules, stats):
        self.conn_rules = self.spark.sparkContext.broadcast(conn_rules)
        self.stats = stats

    # ---
    def apply(self, neuronG, *args, **kw):
        params_df = F.broadcast(self.compute_reduce_cut_params()
                                .select(self._make_assoc_expr("n1_morpho", "n2_morpho"), "*")
                                .cache())

        # Flatten GraphFrame and include morpho>morpho row
        full_touches = neuronG.find("(n1)-[t]->(n2)") \
            .select(self._make_assoc_expr("n1.morphology", "n2.morphology"), "t")  # We only need the assoc and the touch

        # Reduce
        logger.info("Applying Reduce step...")
        reduced_touches = self.apply_reduce(full_touches, params_df)

        # Cache results so far (to disk - wont fit in mem!) - required otherwise they'r recomputed several times
        reduced_touches = reduced_touches.persist(StorageLevel.DISK_ONLY)

        # Cut
        logger.info("Applying Cut step...")
        cut_touches = self.apply_cut(reduced_touches, params_df)
        if _DEBUG: logger.info("Cut touch count:      %d", cut_touches.count())  # NOQA

        # TODO: The ActiveFraction is wrong because at some point in functionalizer they
        #       switched to compute it via a specific getActiveFraction() f.
        # TODO: We also need to implement filtering out by ActiveFraction

        return cut_touches

    @staticmethod
    def _make_assoc_expr(col1, col2):
        # TODO: Instead of string, eventually compute an integer -> should compare faster
        return F.concat(F.col(col1), F.lit(">"), F.col(col2)).alias("morpho_assoc")

    # ---
    def compute_reduce_cut_params(self):
        # First obtain the morpho-morpho stats dataframe
        #   We use mtype_touch_stats which is cached. It requires a huge shuffle anyway
        #   so we can cache (considering we run on systems with much larger mem, e.g. 2M neurons produce a 40GB DF)
        mtype_stats = self.stats.mtype_touch_stats

        # Materializing MType Assoc counts now, it was cached
        logger.info("Computing Pathway stats...")
        logger.debug("Number of Mtype Associations: %d", mtype_stats.count())

        # param create udf
        rc_param_maker = reduce_cut_parameter_udef(self.conn_rules)

        # NOTE: Case of Pathway [MType-MType] not in the recipe
        # TODO: Apparently it doesn't cut. We should issue a warning!

        # Extract p_A, mu_A, activeFraction
        # TODO: activeFraction is calculated the legacy way. We should change
        return (
            mtype_stats
            .select("*",  # We can see here the fields of mtype_stats, which are used for rc_param_maker
                    rc_param_maker(mtype_stats.n1_morpho, mtype_stats.n2_morpho, mtype_stats.average_touches_conn)
                    .alias("rc_params"))
            # Return the params (activeFraction is no longer calculated in this step)
            .select("n1_morpho", "n2_morpho", "total_touches", "rc_params.*"))

    # ---
    @staticmethod
    def apply_reduce(all_touches, params_df):
        """Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a morpho-morpho association
        fractions = {record.morpho_assoc: record.pP_A
                     for record in params_df.select("morpho_assoc", "pP_A").collect()}

        return all_touches.sampleBy("morpho_assoc", fractions)

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
            .groupBy("morpho_assoc", F.col("t.dst").alias("post_neuron_id"))
            .count().withColumnRenamed("count", "post_touches_count")
        )

        post_counts_survival_rate = (
            post_neuron_touch_counts
            # Add pMu_A, sigma to post_neuron info
            .join(params_df_sigma.select("morpho_assoc", "pMu_A", "sigma"), "morpho_assoc")
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
            .select("morpho_assoc", "post_neuron_id", "post_touches_count")
            .cache()  # This DF is used in two different calculations and occupies little space
        )

        # Calc currentTouchCount per pathway and ActiveFraction
        logger.info("Calc currentTouchCount per pathway and ActiveFraction")
        updated_post_touch_count = (
            shall_not_cut
            .groupBy("morpho_assoc")
            .agg(F.sum(F.col("post_touches_count"))
                 .alias("updated_touches_count")
            )
        )

        logger.info("Buildig active_fractions")
        active_fractions = F.broadcast(updated_post_touch_count
            .join(params_df, "morpho_assoc")
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
            .select("morpho_assoc", "active_fraction")
        ).cache()

        # Materialize
        active_fractions.count()

        # Connections to keep according to active_fraction
        logger.info("Building shall_not_cut2")
        shall_not_cut2 = F.broadcast(
            shall_not_cut
            .join(active_fractions, "morpho_assoc")
            .where(F.col("active_fraction") > F.rand())
            .select(F.col("morpho_assoc").alias("m"), F.col("post_neuron_id"))
        )

        logger.info("Cutting touches...")
        cut_touches = (
            reduced_touches
            .join(shall_not_cut2,
                  (F.col("t.dst") == F.col("post_neuron_id")) & (reduced_touches.morpho_assoc == shall_not_cut2.m))
            .select("t.*")
        )

        return cut_touches


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
