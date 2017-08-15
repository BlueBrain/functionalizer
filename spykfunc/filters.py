from __future__ import division

from pyspark.sql import functions as F
from pyspark.sql import types as T
from math import exp
from .definitions import CellClass
from ._filtering import DataSetOperation
from .utils import get_logger
import logging  # For workers
if False: from .recipe import ConnectivityPathRule   # NOQA

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = True


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

        params_df = F.broadcast(self.compute_reduce_cut_params() \
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
        return F.concat(F.col(col1), F.lit(">"), F.col(col2)
                        ).alias("morpho_assoc")

    # ---
    def compute_reduce_cut_params(self):
        # First obtain the morpho-morpho stats dataframe
        
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
        params_df = F.broadcast(params_df.withColumn("sigma", params_df.pMu_A / 4))

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
        )
        
        # A small DF which is required entirely by all workers
        shall_not_cut = F.broadcast(shall_not_cut.cache())

        cut_touches = (
            reduced_touches
            .join(shall_not_cut,
                  (F.col("t.dst") == F.col("post_neuron_id")) & (reduced_touches.morpho_assoc == shall_not_cut.m))
            .select("t.*")
        )
        return cut_touches


_RC_params_schema = T.StructType([
    T.StructField("pP_A", T.FloatType()),
    T.StructField("pMu_A", T.FloatType()),
    T.StructField("pActiveFraction", T.FloatType())
])


def reduce_cut_parameter_udef(conn_rules_map):
    # structuralMean = globalConnectionMeans[comboIndex]
    # structuralProbability = 1.0

    # this f will be serialized and transmitted to workers
    def f(mtype_src, mtype_dst, structuralMean):
        """Calculates the parameters for R&C mtype-mtype
        :param structuralMean: The average of touches/connection for the given mtype-mtype rule
        :return: a tuple of (pP_A, pMu_A, pActiveFraction)
        """
        # If there are no connections for a pathway (mean=0), cannot compute valid numbers
        nil = (None, None, None)

        structuralProbability = 1.0
        if structuralMean == 0:
            return nil

        # conn_rules_map is optimized as a Broadcast variable
        # unfortunately in pyspark it is not transparent, we must use ".value"
        rule = conn_rules_map.value.get(mtype_src + ">" + mtype_dst)  # type: ConnectivityPathRule
        if not rule:
            return nil

        if rule.bouton_reduction_factor and rule.cv_syns_connection:
            cv_syns_connection = rule.cv_syns_connection
            boutonReductionFactor = rule.bouton_reduction_factor

            if rule.active_fraction:
                # 4.3 of s2f 2.0
                rt_star = boutonReductionFactor / rule.active_fraction
                p = 1.0 / structuralMean
                syn_pprime, p_A, mu_A, syn_R_actual = pprime_approximation(rt_star, cv_syns_connection, p)
                pActiveFraction = rule.active_fraction

            elif rule.mean_syns_connection:
                # 4.2 of s2f 2.0
                p = 1 / structuralMean
                sdt = rule.mean_syns_connection * cv_syns_connection
                if sdt > structuralMean - 0.5:
                    sdt = structuralMean - 0.5
                mu_A = 0.5 + rule.mean_syns_connection - sdt
                syn_pprime = 1 / (sdt + 0.5)
                p_A = p / (1 - p) * (1 - syn_pprime) / syn_pprime
                logging.warning("Active fraction not set when calculating R&C parameters from rule " + str(rule))
                pActiveFraction = None

            elif rule.probability:
                # unassigned in s2f 2.0
                logging.warning("Did the column assignment problem get solved?")
                # pActiveFraction = exp(...) * boutonReductionFactor*(structuralMean-1)/(structuralMean*modifier);
                # double modifier = exp(...) * boutonReductionFactor*(structuralMean-1)/(structuralMean*pActiveFraction);
                modifier = boutonReductionFactor * structuralProbability / rule.probability
                pActiveFraction = (exp((1 - cv_syns_connection) / cv_syns_connection) * boutonReductionFactor *
                                   (structuralMean - 1) / (structuralMean * modifier))
                p_A = modifier * cv_syns_connection
                if p_A > 1:
                    p_A = 1
                mu_A = (structuralMean - cv_syns_connection * structuralMean + cv_syns_connection) * modifier
                if mu_A < 1:
                    mu_A = 1

            else:
                logging.warning("Rule not supported")
                return nil

        elif rule.mean_syns_connection and rule.stdev_syns_connection and rule.active_fraction:
            # 4.1 of s2f 2.0
            logging.warning("Warning: method 4.1?")
            mu_A = 0.5 + rule.mean_syns_connection - rule.stdev_syns_connection
            pprime = 1 / (rule.stdev_syns_connection + 0.5)
            p = 1 / structuralMean
            f1 = p / (1 - p) * (1 - pprime) / pprime
            p_A = f1
            pActiveFraction = rule.active_fraction

        else:
            logging.warning("Rule not supported")
            return nil

        pMu_A = mu_A - 0.5
        return p_A, pMu_A, pActiveFraction

    # DEBUG
    def g(mtype_src, mtype_dst, structuralMean):
        # logging.debug(str(conn_rules_map.value))
        rule = conn_rules_map.value.get(mtype_src + ">" + mtype_dst)  # type: ConnectivityPathRule

        return rule.active_fraction

    return F.udf(f, _RC_params_schema)


#
def pprime_approximation(r, cv, p):
    """ Find good approximations for parameters of the s2f algorithm
    :param r:  bouton reduction
    :param cv: coefficient of variance
    :param p: inverse of mean number of structural touches per connection
    """
    epsilon = 0.00001
    step = 1.0
    mnprime = 1

    pprime = None
    f1 = None
    mu2 = None
    r_actual = None

    # be ready to use a fallback calculation method as an alternate resort
    hasFailedPrimary = False

    # to avoid division by zero, add small epsilon if p is 1
    if p == 1:
        p += epsilon

    for tryIndex in range(100):
        if not hasFailedPrimary:
            # calculate the actual reduction factor for the estimate
            pprime = 1 / mnprime
            f1 = (p / (1 - p)) * ((1 - pprime) / pprime)
            mu2 = 1 - 0.5 / cv + (1 / cv - 1) * 1 / pprime
        r_actual = (p / (1 - p)) * (mu2 * pow((1 - pprime), mu2) + (pow(1 - pprime, mu2 + 1) / pprime))

        # check if estimate is good enough
        if abs(r - r_actual) < epsilon:
            return pprime, f1, mu2, r_actual

        # which direction for a better estimate?
        if ((r > r_actual) != (step > 0)) != hasFailedPrimary:
            step = -step / 2

        if not hasFailedPrimary:
            # stay within boundaries
            if (mnprime + step) > (1 / p):
                step = (1 / p) - mnprime

            if (mnprime + step) < 1:
                step = 1 - mnprime

            # adjust mnprime
            mnprime = mnprime + step

            if step == 0:
                hasFailedPrimary = True
                step = 1
        else:
            if (mu2 + step) < 1:
                step = 1 - mu2
            mu2 = mu2 + step

    return pprime, f1, mu2, r_actual


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
