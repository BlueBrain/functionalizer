from math import exp, sqrt
import logging
import pandas as pd


class ReduceAndCutParameters:
    # Defaults
    activeFraction_default = 0.5
    boutonReductionFactor_default = 0.04
    structuralProbability = 1.0

    _schema = [
        ("pathway_i", "long"),
        ("total_touches", "long"),
        ("structural_mean", "float"),
        ("pP_A", "float"),
        ("pMu_A", "float"),
        ("bouton_reduction_factor", "float"),
        ("active_fraction_legacy", "float"),
        ("debug", "string"),
    ]

    def __init__(self, connection_rules, connection_index):
        self.connection_rules = connection_rules
        self.connection_index = connection_index

    def __call__(self, iterator):
        for df in iterator:
            yield self.apply(df)

    def apply(self, df):
        data = (
            self.process(*args)
            for args in zip(df["pathway_i"], df["total_touches"], df["structural_mean"])
        )
        return pd.DataFrame.from_records(data, columns=[c for c, _ in self._schema])

    def process(self, pathway_i, total_touches, structuralMean):
        """Calculates the parameters for R&C

        Args
            structuralMean: The average of touches/connection for the given mtype-mtype rule
        Returns
            A tuple of
            `pathway_i, structuralMean, pP_A, pMu_A, bouton_reduction_factor, activeFraction_legacy`
        """

        def empty_result(reason):
            return (
                pathway_i,
                total_touches,
                structuralMean,
                1.0,
                None,
                None,
                None,
                reason,
            )

        debug = None

        # If there are no connections for a pathway (mean=0), cannot compute valid numbers
        if structuralMean == 0:
            return empty_result("no structural mean")

        # Map from pathway index to rule index, the grab the rule if
        # possible
        idx = self.connection_index[pathway_i]
        if idx < 0 or idx >= len(self.connection_rules):
            return empty_result("no corresponding rule")
        rule = self.connection_rules[idx]

        # Directly from recipe
        boutonReductionFactor = rule.bouton_reduction_factor

        if boutonReductionFactor and rule.cv_syns_connection:
            cv_syns_connection = rule.cv_syns_connection

            if rule.active_fraction:
                # 4.3 of s2f 2.0
                rt_star = boutonReductionFactor / rule.active_fraction
                p = 1.0 / structuralMean
                syn_pprime, p_A, mu_A, _ = pprime_approximation(rt_star, cv_syns_connection, p)
                pActiveFraction = rule.active_fraction
                debug = (
                    f"s2f 4.3: (r={rt_star:.3f}, cv={cv_syns_connection:.3f}, p={p:.3f}) "
                    f"-> pprime={syn_pprime:.3f}, pA={p_A:.3f}, mu_A={mu_A:.3f}"
                )

            elif rule.mean_syns_connection:
                # 4.2 of s2f 2.0
                p = 1.0 / structuralMean
                sdt = min(
                    1.0 * rule.mean_syns_connection * cv_syns_connection,
                    structuralMean - 0.5,
                )
                mu_A = 0.5 + rule.mean_syns_connection - sdt
                syn_pprime = 1.0 / (sqrt(sdt * sdt + 0.25) + 0.5)
                p_A = (
                    (p / (1.0 - p) * (1.0 - syn_pprime) / syn_pprime) if (p != 1.0) else 1.0
                )  # Control DIV/0
                pActiveFraction = self.activeFraction_default
                debug = (
                    f"s2f 4.2: p={p:.3f}, pprime={syn_pprime:.3f}, pA={p_A:.3f}, mu_A={mu_A:.3f}"
                )

            elif rule.probability:
                # unassigned in s2f 2.0
                # pActiveFraction =
                #   exp(...) * boutonReductionFactor*(structuralMean-1)/
                #   (structuralMean*modifier)
                # double modifier =
                #   exp(...) * boutonReductionFactor*(structuralMean-1)/
                #   (structuralMean*pActiveFraction)
                modifier = boutonReductionFactor * self.structuralProbability / rule.probability
                pActiveFraction = (
                    exp((1.0 - cv_syns_connection) / cv_syns_connection)
                    * boutonReductionFactor
                    * (structuralMean - 1.0)
                    / (structuralMean * modifier)
                )
                p_A = modifier * cv_syns_connection
                mu_A = max(
                    (structuralMean - cv_syns_connection * structuralMean + cv_syns_connection)
                    * modifier,
                    1.0,
                )
                debug = f"s2f unassigned: modifier={modifier:.3f}, pA={p_A:.3f}, mu_A={mu_A:.3f}"

            else:
                logging.warning("Rule not supported")
                return empty_result("unsupported rule w/ button reduction factor")

        elif boutonReductionFactor is not None and rule.p_A is not None and rule.pMu_A is not None:
            pActiveFraction = self.activeFraction_default
            mu_A = 0.5 + rule.pMu_A
            p_A = rule.p_A
            debug = f"s2f defaulted: pA={p_A:.3f}, mu_A={mu_A:.3f}"

        elif rule.mean_syns_connection and rule.stdev_syns_connection:
            # 4.1 of s2f 2.0
            logging.warning("Warning: method 4.1?")
            mu_A = 0.5 + rule.mean_syns_connection - rule.stdev_syns_connection
            pprime = 1.0 / (rule.stdev_syns_connection + 0.5)
            p = 1.0 / structuralMean
            p_A = (p / (1.0 - p) * (1.0 - pprime) / pprime) if (p != 1.0) else 1.0
            pActiveFraction = rule.active_fraction
            boutonReductionFactor = self.boutonReductionFactor_default
            debug = f"s2f 4.1: p={p:.3f}, pprime={pprime:.3f}, pA={p_A:.3f}, mu_A={mu_A:.3f}"

        else:
            logging.warning("Rule not supported")
            return empty_result("unsupported rule")

        pMu_A = mu_A - 0.5
        p_A = min(p_A, 1.0)
        pActiveFraction = min(pActiveFraction, 1.0)

        # ActiveFraction calculated here is legacy and only used if boutonReductionFactor is null
        return (
            pathway_i,
            total_touches,
            structuralMean,
            p_A,
            pMu_A,
            boutonReductionFactor,
            pActiveFraction,
            debug,
        )

    @classmethod
    def schema(cls):
        return ", ".join(f"{c} {t}" for (c, t) in cls._schema)


def pprime_approximation(r, cv, p):
    """Find good approximations for parameters of the s2f algorithm
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
    failed_primary = False

    # to avoid division by zero, add small epsilon if p is 1
    if p == 1.0:
        p += epsilon

    for _ in range(100):
        if not failed_primary:
            # calculate the actual reduction factor for the estimate
            pprime = 1.0 / mnprime
            f1 = (p / (1.0 - p)) * ((1.0 - pprime) / pprime)
            mu2 = 1.0 - 0.5 / cv + (1.0 / cv - 1.0) * 1.0 / pprime
        r_actual = (p / (1.0 - p)) * (
            mu2 * pow((1.0 - pprime), mu2) + (pow(1.0 - pprime, mu2 + 1.0) / pprime)
        )

        # check if estimate is good enough
        if abs(r - r_actual) < epsilon:
            return pprime, f1, mu2, r_actual

        # which direction for a better estimate?
        if ((r > r_actual) != (step > 0)) != failed_primary:
            step = -step / 2.0

        if not failed_primary:
            # stay within boundaries
            if (mnprime + step) > (1.0 / p):
                step = (1.0 / p) - mnprime

            if (mnprime + step) < 1.0:
                step = 1.0 - mnprime

            # adjust mnprime
            mnprime = mnprime + step

            if step == 0.0:
                failed_primary = True
                step = 1.0
        else:
            if (mu2 + step) < 1.0:
                step = 1.0 - mu2
            mu2 = mu2 + step

    return pprime, f1, mu2, r_actual
