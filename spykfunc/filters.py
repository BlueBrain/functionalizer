from __future__ import division

from pyspark.sql import functions as F
from pyspark.sql import types as T
from math import exp
from .definitions import CellClass
from ._filtering import DataSetOperation
from .utils import get_logger
import logging  # For workers
if False: from .recipe import ConnectivityPathRule   # NOQA
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
        self.conn_rules = conn_rules
        self.stats = stats

    # ---
    def apply(self, neuronG, *args, **kw):

        params_df = F.broadcast(self.compute_reduce_cut_params()
                                .select(self._make_assoc_expr("n1_morpho", "n2_morpho"), "*"))

        # Flatten GraphFrame and include morpho>morpho row
        full_touches = neuronG.find("(n1)-[t]->(n2)") \
            .select(self._make_assoc_expr("n1.morphology", "n2.morphology"), "*")

        if _DEBUG:
            params_df.show()
            logger.info("Original touch count: %d", full_touches.count())

        # Reduce
        reduced_touches = self.apply_reduce(full_touches, params_df)
        if _DEBUG: logger.info("Reduce touch count:   %d", reduced_touches.count())  # NOQA

        # Cut
        cut_touches = self.apply_cut(reduced_touches, params_df)
        if _DEBUG: logger.info("Cut touch count:      %d", cut_touches.count())  # NOQA

        return cut_touches

    @staticmethod
    def _make_assoc_expr(col1, col2):
        return F.concat(F.col(col1), F.lit(">"), F.col(col2)).alias("morpho_assoc")

    # ---
    def compute_reduce_cut_params(self):
        # First obtain the morpho-morpho stats dataframe
        # We use mtype_touch_stats which is cached. It requires a huge shuffle
        #  anyway so we can cache (considering we run on systems with much larger mem, e.g. 2M neurons produce a 40GB DF)
        mtype_stats = self.stats.mtype_touch_stats

        # param create udf
        rc_param_maker = reduce_cut_parameter_udef(self.conn_rules)

        # Extract p_A, mu_A, activeFraction
        return (
            mtype_stats
            .select("*",  # We can see here the fields of mtype_stats, which are used for rc_param_maker
                    rc_param_maker(mtype_stats.n1_morpho, mtype_stats.n2_morpho, mtype_stats.average_touches_conn)
                    .alias("rc_params"))
            .select("n1_morpho", "n2_morpho", "total_touches", "rc_params.pP_A", "rc_params.pMu_A", "rc_params.pActiveFraction"))

    # ---
    @staticmethod
    def apply_reduce(all_touches, params_df):
        """Applying reduce as a sampling
        """
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a morpho-morpho association
        fractions = {record.n1_morpho + ">" + record.n2_morpho: record.pP_A
                     for record in params_df.select("n1_morpho", "n2_morpho", "pP_A").collect()}

        return all_touches.sampleBy("morpho_assoc", fractions)

    # ---
    @staticmethod
    def apply_cut(reduced_touches, params_df):
        """ Apply cut filter
        """
        params_df = params_df.withColumn("sigma", params_df.pMu_A / 4)

        # This will incur a large shuffle which triggers the calculation of reduced_touches
        post_neuron_touch_counts = (
            reduced_touches
            .groupBy("morpho_assoc", F.col("n2.id").alias("post_neuron_id"))
            .count().withColumnRenamed("count", "post_touches_count")
            # Add pMu_A, sigma to post_neuron info
            .join(params_df.select("morpho_assoc", "pMu_A", "sigma"), "morpho_assoc")
            # Calc survivalRate
            .withColumn("survivalRate", 1.0 /
                        (1.0 + F.exp((-4 / F.col("sigma")) * (F.col("post_touches_count") - F.col("pMu_A")))))
            .drop("sigma")
        )

        shall_not_cut = (
            post_neuron_touch_counts
            .withColumn("rand", F.rand())
            .where(F.col("rand") < F.col("survivalRate"))
            .select(F.col("morpho_assoc").alias("m"), F.col("post_neuron_id"))
        ).cache()

        # A small DF which is required entirely by all workers
        logger.info("Calculating cut touches...")
        logger.info("Cutting %d touches", shall_not_cut.count())  # Materialize it
        shall_not_cut = F.broadcast(shall_not_cut)

        cut_touches = (
            reduced_touches
            .join(shall_not_cut,
                  (F.col("t.dst") == F.col("post_neuron_id")) & (reduced_touches.morpho_assoc == shall_not_cut.m))
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
