from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext
from math import exp, sqrt
import logging

# **************************************************
# Reduce and Cut parameters
# **************************************************

_RC_params_schema = T.StructType([
    T.StructField("pP_A", T.FloatType()),
    T.StructField("pMu_A", T.FloatType()),
    T.StructField("bouton_reduction_factor", T.FloatType()),
    T.StructField("active_fraction_legacy", T.FloatType()),
    T.StructField("_debug", T.StringType()),
])


def reduce_cut_parameter_udef(conn_rules_map, debug=False):
    # Defaults
    activeFraction_default = 0.5
    boutonReductionFactor_default = 0.04
    structuralProbability = 1.0

    # this f will be serialized and transmitted to workers
    def f(pathway_i, structuralMean):
        """Calculates the parameters for R&C mtype-mtype
        :param structuralMean: The average of touches/connection for the given mtype-mtype rule
        :return: a tuple of (pP_A, pMu_A, bouton_reduction_factor, activeFraction_legacy)
        """
        # If there are no connections for a pathway (mean=0), cannot compute valid numbers
        _debug = None
        nil = (None, None, None, None, _debug)

        if structuralMean == 0:
            return nil

        # conn_rules_map is optimized as a Broadcast variable
        # unfortunately in pyspark it is not transparent, we must use ".value"
        # key = mtype_src + ">" + mtype_dst
        rule = conn_rules_map.value.get(pathway_i)

        if not rule:
            return nil

        # Directly from recipe
        boutonReductionFactor = rule.bouton_reduction_factor

        if boutonReductionFactor and rule.cv_syns_connection:
            cv_syns_connection = rule.cv_syns_connection

            if rule.active_fraction:
                # 4.3 of s2f 2.0
                rt_star = boutonReductionFactor / rule.active_fraction
                p = 1.0 / structuralMean
                syn_pprime, p_A, mu_A, syn_R_actual = pprime_approximation(rt_star, cv_syns_connection, p)
                pActiveFraction = rule.active_fraction
                if debug: _debug = "s2f 4.3: (r=%.3f, cv=%.3f, p=%.3f) -> pprime=%.3f, pA=%.3f, mu_A=%.3f" % (rt_star, cv_syns_connection, p, syn_pprime, p_A, mu_A)

            elif rule.mean_syns_connection:
                # 4.2 of s2f 2.0
                p = 1.0 / structuralMean
                sdt = min(1.0 * rule.mean_syns_connection * cv_syns_connection, structuralMean - 0.5)
                mu_A = 0.5 + rule.mean_syns_connection - sdt
                syn_pprime = 1.0 / (sqrt(sdt*sdt + 0.25) + 0.5)
                p_A = (p / (1.0 - p) * (1.0 - syn_pprime) / syn_pprime) if (p != 1.0) else 1.0   # Control DIV/0
                pActiveFraction = activeFraction_default
                if debug: _debug = "s2f 4.2: p=%.3f, pprime=%.3f, pA=%.3f, mu_A=%.3f" % (p, syn_pprime, p_A, mu_A)

            elif rule.probability:
                # unassigned in s2f 2.0
                # pActiveFraction = exp(...) * boutonReductionFactor*(structuralMean-1)/(structuralMean*modifier);
                # double modifier = exp(...) * boutonReductionFactor*(structuralMean-1)/(structuralMean*pActiveFraction);
                modifier = boutonReductionFactor * structuralProbability / rule.probability
                pActiveFraction = (exp((1.0 - cv_syns_connection) / cv_syns_connection) * boutonReductionFactor *
                                   (structuralMean - 1.0) / (structuralMean * modifier))
                p_A = modifier * cv_syns_connection
                mu_A = (structuralMean - cv_syns_connection * structuralMean + cv_syns_connection) * modifier
                if mu_A < 1.0: mu_A = 1.0
                if debug: _debug = "s2f unassigned: modifier=%.3f, pA=%.3f, mu_A=%.3f" % (modifier, p_A, mu_A)

            else:
                logging.warning("Rule not supported")
                return nil

        elif rule.mean_syns_connection and rule.stdev_syns_connection:
            # 4.1 of s2f 2.0
            logging.warning("Warning: method 4.1?")
            mu_A = 0.5 + rule.mean_syns_connection - rule.stdev_syns_connection
            pprime = 1.0 / (rule.stdev_syns_connection + 0.5)
            p = 1.0 / structuralMean
            p_A = (p / (1.0 - p) * (1.0 - pprime) / pprime) if (p != 1.0) else 1.0
            pActiveFraction = rule.active_fraction
            if debug: _debug = "s2f 4.1: p=%.3f, pprime=%.3f, pA=%.3f, mu_A=%.3f" % (p, pprime, p_A, mu_A)

        else:
            logging.warning("Rule not supported")
            return nil

        pMu_A = mu_A - 0.5
        if p_A > 1.0: p_A = 1.0
        if pActiveFraction > 1.0: pActiveFraction = 1.0

        # ActiveFraction calculated here is legacy and only used if boutonReductionFactor is null
        return p_A, pMu_A, boutonReductionFactor, pActiveFraction, _debug

    return F.udf(f, _RC_params_schema)


def pprime_approximation(r, cv, p):
    """ Find good approximations for parameters of the s2f algorithm
    :param r:  bouton reduction
    :param cv: coefficient of variance
    :param p: inverse of mean number of structural touches per connection
    """
    epsilon = 0.00001
    step = 1.0
    mnprime = 1.0

    pprime = None
    f1 = None
    mu2 = None
    r_actual = None

    # be ready to use a fallback calculation method as an alternate resort
    hasFailedPrimary = False

    # to avoid division by zero, add small epsilon if p is 1
    if p == 1.0:
        p += epsilon

    for tryIndex in range(100):
        if not hasFailedPrimary:
            # calculate the actual reduction factor for the estimate
            pprime = 1.0 / mnprime
            f1 = (p / (1.0 - p)) * ((1.0 - pprime) / pprime)
            mu2 = 1.0 - 0.5 / cv + (1.0 / cv - 1.0) * 1.0 / pprime
        r_actual = (p / (1.0 - p)) * (mu2 * pow((1.0 - pprime), mu2) + (pow(1.0 - pprime, mu2 + 1.0) / pprime))

        # check if estimate is good enough
        if abs(r - r_actual) < epsilon:
            return pprime, f1, mu2, r_actual

        # which direction for a better estimate?
        if ((r > r_actual) != (step > 0)) != hasFailedPrimary:
            step = -step / 2.0

        if not hasFailedPrimary:
            # stay within boundaries
            if (mnprime + step) > (1.0 / p):
                step = (1.0 / p) - mnprime

            if (mnprime + step) < 1.0:
                step = 1.0 - mnprime

            # adjust mnprime
            mnprime = mnprime + step

            if step == .0:
                hasFailedPrimary = True
                step = 1.0
        else:
            if (mu2 + step) < 1.0:
                step = 1.0 - mu2
            mu2 = mu2 + step

    return pprime, f1, mu2, r_actual


# *********************************************************
# Synapse classification
# *********************************************************

def get_synapse_property_udf(syn_class_matrix, sc=None):
    if sc is None:
        sc = SparkContext.getOrCreate()

    # We need the matrix in all nodes, flattened
    syn_class_matrix_flat = sc.broadcast(syn_class_matrix.flatten())

    def syn_prop_udf(syn_prop_index):
        # Leaves are still tuple size2
        return syn_class_matrix_flat.value[syn_prop_index].tolist()

    return F.udf(syn_prop_udf, T.ShortType())
