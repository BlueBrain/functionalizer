"""A default filter plugin
"""
from __future__ import division
import fnmatch

import pandas

from pyspark.sql import functions as F

import sparkmanager as sm

from spykfunc.circuit import Circuit, touches_per_pathway
from spykfunc.definitions import CheckpointPhases
from spykfunc.filters import DatasetOperation, helpers
from spykfunc.filters.udfs import reduce_cut_parameter_udf
from spykfunc.schema import pathway_i_to_str, touches_with_pathway
from spykfunc.utils import get_logger
from spykfunc.utils.checkpointing import checkpoint_resume, CheckpointHandler
from spykfunc.utils.spark import cache_broadcast_single_part

from . import Seeds, add_random_column

logger = get_logger(__name__)


_KEY_REDUCE = 0x100
_KEY_CUT = 0x101
_KEY_ACTIVE = 0x102


class ConnectType:
    """Enum class for Connect Types"""

    InvalidConnect = 0
    MTypeConnect = 1  # <mTypeRule>
    LayerConnect = 2  # <layerRule>
    ClassConnect = 3  # <sClassRule>
    MaxConnectTypes = 4

    __rule_names = {
        "mTypeRule": MTypeConnect,
        "layerRule": LayerConnect,
        "sClassRule": ClassConnect
    }
    __names = {val: name for name, val in __rule_names.items()}

    @classmethod
    def from_type_name(cls, name):
        return cls.__rule_names.get(name, cls.InvalidConnect)

    @classmethod
    def to_str(cls, index):
        return cls.__names[index]


class ConnectivityPathRule(object):
    """Connectivity Pathway rule"""

    connect_type = None
    source = None
    destination = None

    probability = None
    active_fraction = None
    bouton_reduction_factor = None
    cv_syns_connection = None
    mean_syns_connection = None
    stdev_syns_connection = None

    # Possible field, currently not used by functionalizer
    distance_bin = None
    probability_bin = None
    reciprocal_bin = None
    reduction_min_prob = None
    reduction_max_prob = None
    multi_apposition_slope = None
    multi_apposition_offset = None

    _float_fields = ["probability", "mean_syns_connection", "stdev_syns_connection",
                     "active_fraction", "bouton_reduction_factor", "cv_syns_connection"]

    # ------
    def __init__(self, rule_type, rule_dict, rule_children=None):
        # type: (str, dict, list) -> None

        self.connect_type = ConnectType.from_type_name(rule_type)

        # Convert names
        self.source = rule_dict.pop("from")
        self.destination = rule_dict.pop("to")

        for prop_name, prop_val in rule_dict.items():
            if prop_name in self.__class__.__dict__:
                if prop_name in ConnectivityPathRule._float_fields:
                    setattr(self, prop_name, float(prop_val))
                else:
                    setattr(self, prop_name, prop_val)

        # Convert IDS
        if self.connect_type == ConnectType.LayerConnect:
            self.source = int(self.source) - 1
            self.destination = int(self.destination) - 1

        if rule_children:
            # simple probability
            self.distance_bin = []
            self.probability_bin = []
            self.reciprocal_bin = []
            for bin in rule_children:  # type:dict
                self.distance_bin.append(bin.get("distance", 0))
                self.probability_bin.append(bin.get("probability", 0))
                self.reciprocal_bin.append(bin.get("reciprocal", 0))

        if not self.is_valid():
            logger.error("Wrong number of params in Connection Rule: " + str(self))

    def is_valid(self):
        # Rule according to validation in ConnectivityPathway::getReduceAndCutParameters
        # Count number of rule params, must be 3
        n_set_params = sum(var is not None for var in (self.probability, self.mean_syns_connection,
                                                       self.stdev_syns_connection, self.active_fraction,
                                                       self.bouton_reduction_factor, self.cv_syns_connection))
        return n_set_params == 3

    def __repr__(self):
        return '<%s from="%s" to="%s">' % (ConnectType.to_str(self.connect_type), self.source, self.destination)

    @classmethod
    def load(cls, xml, src_mtypes, dst_mtypes):
        """Load connection rules

        Transform connection rules into concrete rule instances (without wildcards) and indexed by pathway

        Args:
            xml: The raw recipe XML to extract the data from
            src_mtypes: The morphology types associated with the source population
            dst_mtypes: The morphology types associated with the target population
        Returns:
            A dictionary of pathways (src, dst mtype) and corresponding rules
        """
        rules = []
        for rule in xml.find("ConnectionRules"):
            children = [child.attrib for child in rule]
            rules.append(ConnectivityPathRule(rule.tag, dict(rule.attrib), children))

        if len(rules) == 0:
            raise RuntimeError("No connection rules loaded. Please check the recipe.")

        src_mtypes_rev = {mtype: i for i, mtype in enumerate(src_mtypes)}
        dst_mtypes_rev = {mtype: i for i, mtype in enumerate(dst_mtypes)}
        concrete_rules = {}

        for rule in rules:
            srcs = fnmatch.filter(src_mtypes, rule.source)
            dsts = fnmatch.filter(dst_mtypes, rule.destination)
            if len(srcs) == 0:
                logger.warning(f"Connection rules can't match from='{rule.source}'")
            if len(dsts) == 0:
                logger.warning(f"Connection rules can't match to='{rule.destination}'")
            for src in srcs:
                for dst in dsts:
                    # key = src + ">" + dst
                    # Key is now an int
                    key = (src_mtypes_rev[src] << 16) + dst_mtypes_rev[dst]
                    if key in concrete_rules:
                        # logger.debug("Several rules applying to the same mtype connection: %s->%s [Rule: %s->%s]",
                        #                src, dst, rule.source, rule.destination)
                        prev_rule = concrete_rules[key]
                        # Overwrite if it is specific
                        if (('*' in prev_rule.source and '*' not in rule.source) or
                                ('*' in prev_rule.destination and '*' not in rule.destination)):
                            concrete_rules[key] = rule
                    else:
                        concrete_rules[key] = rule
        return concrete_rules


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

    def __init__(self, recipe, source, target, morphos):
        self.seed = Seeds.load(recipe.xml).synapseSeed
        logger.info("Using seed %d for reduce and cut", self.seed)
        self.conn_rules = ConnectivityPathRule.load(
            recipe.xml,
            source.mtypes,
            target.mtypes
        )

    def apply(self, circuit):
        """Filter the circuit according to the logic described in the
        class.
        """
        full_touches = Circuit.only_touch_columns(touches_with_pathway(circuit.df))
        src_mtypes = circuit.source.mtype_df
        dst_mtypes = circuit.target.mtype_df
        self.conn_rules = sm.broadcast(self.conn_rules)

        # Get and broadcast Pathway stats
        # NOTE we cache and count to force evaluation in N tasks, while sorting in a single task
        logger.debug("Computing Pathway stats...")
        _params = self.compute_reduce_cut_params(full_touches)
        params_df = F.broadcast(_params)

        # Params ouput for validation
        _params_out_csv(params_df, "pathway_params", src_mtypes, dst_mtypes)

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
            src_mtypes=src_mtypes,
            dst_mtypes=dst_mtypes
        )

        _connection_counts_out_csv(
            cut1_shall_keep_connections,
            "cut_counts.csv",
            src_mtypes,
            dst_mtypes
        )

        logger.info("Calculating CUT part 2: Active Fractions")
        cut_shall_keep_connections = self.calc_cut_active_fraction(
            cut1_shall_keep_connections,
            params_df,
            src_mtypes=src_mtypes,
            dst_mtypes=dst_mtypes
        ).select("src", "dst")

        with sm.jobgroup("Filtering touches CUT step", ""):
            cut2AF_touches = (
                reduced_touches.join(cut_shall_keep_connections, ["src", "dst"])
            )

            _touch_counts_out_csv(cut2AF_touches, "cut2af_counts.csv", src_mtypes, dst_mtypes)

        # Only the touch fields
        return Circuit.only_touch_columns(cut2AF_touches)

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
        pathway_stats = touches_per_pathway(full_touches).coalesce(_n_parts)

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
        return add_random_column(
            all_touches.join(fractions, "pathway_i"),
            "reduce_rand", self.seed, _KEY_REDUCE,
            F.col("synapse_id")
        ).where(F.col("pP_A") > F.col("reduce_rand")) \
         .drop("reduce_rand", "pP_A") \
         .repartition("src", "dst")

    # ---
    @sm.assign_to_jobgroup
    def calc_cut_survival_rate(self, reduced_touches, params_df, src_mtypes, dst_mtypes):
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
        _connection_counts_out_csv(
            reduced_touch_counts_connection,
            "reduced_touch_counts_pathway",
            src_mtypes,
            dst_mtypes
        )

        params_df_sigma = (
            params_df
            .where(F.col("pMu_A").isNotNull())
            .select("pathway_i", "pMu_A")
            .withColumn("sigma", params_df.pMu_A / 4)
        )
        # Debug
        _params_out_csv(params_df_sigma, "survival_params", src_mtypes, dst_mtypes)

        connection_survival_rate = (
            reduced_touch_counts_connection
            .join(params_df_sigma, "pathway_i")  # Fetch the pathway params
            .withColumn("survival_rate",  # Calc survivalRate
                        F.expr("1.0 / (1.0 + exp((-4.0/sigma) * (reduced_touch_counts_connection-pMu_A)))"))
            .drop("sigma", "pMu_A")
        )

        # Deactivated due to large output size.
        # _dbg_df = connection_survival_rate.select("pathway_i", "reduced_touch_counts_connection", "survival_rate")
        # helpers._write_csv(pathway_i_to_str(_dbg_df.groupBy("pathway_i").count(), mtypes),
        #            "connection_survival_rate.csv")

        logger.debug(" -> Computing connections to cut according to survival_rate")
        _df = connection_survival_rate
        cut_connections = add_random_column(
            _df, "cut_rand", self.seed, _KEY_CUT,
            F.col("synapse_id"),
        ).where((_df.survival_rate > .0) & (_df.survival_rate > F.col("cut_rand"))) \
         .select("src", "dst", "synapse_id", "pathway_i", "reduced_touch_counts_connection")
        # Much smaller data volume but we cant coealesce
        return cut_connections

    # ----
    @sm.assign_to_jobgroup
    @checkpoint_resume("shall_keep_connections", bucket_cols=("src", "dst"), child=True)
    def calc_cut_active_fraction(self, cut_touch_counts_connection, params_df, src_mtypes, dst_mtypes):
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

        helpers._write_csv(
            pathway_i_to_str(active_fractions, src_mtypes, dst_mtypes),
            "active_fractions.csv"
        )

        shall_keep_connections = add_random_column(
            cut_touch_counts_connection.join(active_fractions, "pathway_i"),
            "active_rand", self.seed, _KEY_ACTIVE,
            F.col("synapse_id"),
        ).where(F.col("active_rand") < F.col("active_fraction")) \
         .select("src", "dst")

        return shall_keep_connections


def _params_out_csv(df, filename, src_mtypes, dst_mtypes):
    of_interest = ("pathway_i", "total_touches", "structural_mean",
                   "pP_A", "pMu_A", "active_fraction_legacy", "_debug")
    cols = []
    for col in of_interest:
        if hasattr(df, col):
            cols.append(col)
    debug_info = df.select(*cols)
    helpers._write_csv(pathway_i_to_str(debug_info, src_mtypes, dst_mtypes), filename)


def _touch_counts_out_csv(df, filename, src_mtypes, dst_mtypes):
    helpers._write_csv(pathway_i_to_str(
        df.groupBy("pathway_i").count(),
        src_mtypes,
        dst_mtypes
    ), filename)


def _connection_counts_out_csv(df, filename, src_mtypes, dst_mtypes):
    helpers._write_csv(pathway_i_to_str(
        df.groupBy("pathway_i").sum("reduced_touch_counts_connection"),
        src_mtypes,
        dst_mtypes
    ), filename)
