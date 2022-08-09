"""Filter that matches distributions of synapses."""
from pyspark.sql import functions as F

import sparkmanager as sm

from spykfunc.circuit import Circuit, touches_per_pathway
from spykfunc.definitions import CheckpointPhases
from spykfunc.filters import DatasetOperation, helpers
from spykfunc.filters.udfs import ReduceAndCutParameters
from spykfunc.utils import get_logger
from spykfunc.utils.checkpointing import checkpoint_resume, CheckpointHandler
from spykfunc.utils.spark import cache_broadcast_single_part

from . import add_random_column

logger = get_logger(__name__)


_KEY_REDUCE = 0x100
_KEY_CUT = 0x101
_KEY_ACTIVE = 0x102


class ReduceAndCut(DatasetOperation):
    """Reduce and cut touch distributions.

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
        """Initializes the filter.

        Uses the synapse seed from the recipe to initialize random number generators, and
        extracts the connection rules from the recipe.
        """
        super().__init__(recipe, source, target, morphos)
        self.seed = recipe.seeds.synapseSeed
        logger.info("Using seed %d for reduce and cut", self.seed)
        self.columns = recipe.connection_rules.required
        logger.info(
            "Using the following columns for reduce and cut: %s",
            ", ".join(self.columns),
        )
        self.connection_rules = recipe.connection_rules
        self.connection_index = recipe.connection_rules.to_matrix(
            {n: v for n, _, _, v in Circuit.expand(self.columns, source, target)}
        )

    def apply(self, circuit):
        """Filter the circuit according to the logic described in the class."""
        with circuit.pathways(self.columns):
            full_touches = Circuit.only_touch_columns(circuit.with_pathway())

            # Get and broadcast Pathway stats
            #
            # NOTE we should cache and count to be evaluated with N tasks,
            # while sorting in a single task
            #
            # Spark optimizes better for >2000 shuffle partitions, and
            # should do the exchanges with >1 partitions. If not,
            # computation will slow down and one may have to force the hand
            # of Spark by using
            #
            #     with number_shuffle_partitions(_n_parts):
            #
            # also, too many partitions will lead to a slow-down when
            # aggregating!
            logger.debug("Computing Pathway stats...")
            params_df = F.broadcast(self.compute_reduce_cut_params(full_touches))
            _params_out_csv(params_df, "pathway_params")  # Params ouput for validation

            # Reduce
            logger.info("Applying Reduce step...")
            reduced_touches = self.apply_reduce(full_touches, params_df)

            # Cut
            logger.info("Calculating CUT part 1: survival rate")
            cut1_shall_keep_connections = self.calc_cut_survival_rate(reduced_touches, params_df)

            _connection_counts_out_csv(cut1_shall_keep_connections, "cut_counts.csv")

            logger.info("Calculating CUT part 2: Active Fractions")
            cut_shall_keep_connections = self.calc_cut_active_fraction(
                cut1_shall_keep_connections, params_df
            ).select("src", "dst")

            with sm.jobgroup("Filtering touches CUT step", ""):
                cut2AF_touches = reduced_touches.join(cut_shall_keep_connections, ["src", "dst"])

                _touch_counts_out_csv(cut2AF_touches, "cut2af_counts.csv")

            # Only the touch fields
            return Circuit.only_touch_columns(cut2AF_touches).drop("pathway_i")

    # ---
    @sm.assign_to_jobgroup
    @checkpoint_resume("pathway_stats", child=True)
    def compute_reduce_cut_params(self, full_touches):
        """Computes pathway parameters for reduce and cut.

        Based on the number of touches per pathway and the total number of
        connections (unique pathways).

        :param full_touches: touches with a pathway column
        :return: a dataframe containing reduce and cut parameters
        """
        pathway_stats = touches_per_pathway(full_touches)
        param_maker = ReduceAndCutParameters(self.connection_rules, self.connection_index)
        return pathway_stats.mapInPandas(param_maker, param_maker.schema()).coalesce(1)

    # ---
    @sm.assign_to_jobgroup
    @checkpoint_resume(
        CheckpointPhases.FILTER_REDUCED_TOUCHES.name,
        bucket_cols=("src", "dst"),
        # Even if we change to not break exec plan we always keep only touch cols
        handlers=[CheckpointHandler.before_save(Circuit.only_touch_columns)],
        child=True,
    )
    def apply_reduce(self, all_touches, params_df):
        """Applying reduce as a sampling."""
        # Reducing touches on a single neuron is equivalent as reducing on
        # the global set of touches within a pathway (morpho-morpho association)
        logger.debug(" -> Building reduce fractions")
        fractions = F.broadcast(params_df.select("pathway_i", "pP_A"))

        logger.debug(" -> Cutting touches")
        return (
            add_random_column(
                all_touches.join(fractions, "pathway_i"),
                "reduce_rand",
                self.seed,
                _KEY_REDUCE,
            )
            .where(F.col("pP_A") > F.col("reduce_rand"))
            .drop("reduce_rand", "pP_A")
            .repartition("src", "dst")
        )

    # ---
    @sm.assign_to_jobgroup
    def calc_cut_survival_rate(self, reduced_touches, params_df):
        """Apply cut filter.

        Cut computes a survivalRate and activeFraction for each post neuron
        And filters out those not passing the random test
        """
        logger.info("Computing reduced touch counts")
        reduced_touch_counts_connection = reduced_touches.groupBy("src", "dst").agg(
            F.first("pathway_i").alias("pathway_i"),
            F.min("synapse_id").alias("synapse_id"),
            F.count("src").alias("reduced_touch_counts_connection"),
        )
        # Debug
        _connection_counts_out_csv(reduced_touch_counts_connection, "reduced_touch_counts_pathway")

        params_df_sigma = (
            params_df.where(F.col("pMu_A").isNotNull())
            .select("pathway_i", "pMu_A")
            .withColumn("sigma", params_df.pMu_A / 4)
        )

        # Debug
        _params_out_csv(params_df_sigma, "survival_params")

        connection_survival_rate = (
            reduced_touch_counts_connection.join(
                params_df_sigma, "pathway_i"
            )  # Fetch the pathway params
            .withColumn(
                "survival_rate",  # Calc survivalRate
                F.when(F.col("sigma") == 0, F.lit(1.0)).otherwise(
                    F.expr(
                        "1.0 / (1.0 + exp((-4.0/sigma) * (reduced_touch_counts_connection-pMu_A)))"
                    )
                ),
            )
            .drop("sigma", "pMu_A")
        )

        # Deactivated due to large output size.
        # _dbg_df = connection_survival_rate.select(
        #     "pathway_i", "reduced_touch_counts_connection", "survival_rate")
        # helpers._write_csv(Circuit.pathway_to_str(_dbg_df.groupBy("pathway_i").count()),
        #            "connection_survival_rate.csv")

        logger.debug(" -> Computing connections to cut according to survival_rate")
        _df = connection_survival_rate
        cut_connections = (
            add_random_column(_df, "cut_rand", self.seed, _KEY_CUT)
            .where((F.col("survival_rate") > 0.0) & (F.col("survival_rate") > F.col("cut_rand")))
            .select(
                "src",
                "dst",
                "synapse_id",
                "pathway_i",
                "reduced_touch_counts_connection",
            )
        )
        # Much smaller data volume but we cant coealesce
        return cut_connections

    # ----
    @sm.assign_to_jobgroup
    @checkpoint_resume("shall_keep_connections", bucket_cols=("src", "dst"), child=True)
    def calc_cut_active_fraction(self, cut_touch_counts_connection, params_df):
        """Cut according to the active_fractions.

        Args:
            params_df: the parameters DF (pA, uA, active_fraction_legacy)
            cut_touch_counts_connection: the DF with the cut touch counts
                per connection (built previously in an optimized way)

        Returns:
            The final cut touches
        """
        cut_touch_counts_pathway = cut_touch_counts_connection.groupBy("pathway_i").agg(
            F.sum("reduced_touch_counts_connection").alias("cut_touch_counts_pathway")
        )

        active_fractions = (
            cut_touch_counts_pathway.join(params_df, "pathway_i")
            .withColumn(
                "actual_reduction_factor",
                F.col("cut_touch_counts_pathway") / F.col("total_touches"),
            )
            .withColumn(
                "active_fraction",
                F.when(
                    F.col("bouton_reduction_factor").isNull(),
                    F.col("active_fraction_legacy"),
                ).otherwise(
                    F.when(
                        F.col("bouton_reduction_factor") > F.col("actual_reduction_factor"),
                        F.lit(1.0),
                    ).otherwise(F.col("bouton_reduction_factor") / F.col("actual_reduction_factor"))
                ),
            )
            .select("pathway_i", "active_fraction")
        )

        logger.debug("Computing Active Fractions")
        # On both ends data is small (500x less) so we can reduce parallelism
        _n_parts = max(cut_touch_counts_connection.rdd.getNumPartitions() // 50, 50)
        # Result is a minimal DS so we cache and broadcast in single partition
        active_fractions = cache_broadcast_single_part(active_fractions, parallelism=_n_parts)

        helpers._write_csv(Circuit.pathway_to_str(active_fractions), "active_fractions.csv")

        shall_keep_connections = (
            add_random_column(
                cut_touch_counts_connection.join(active_fractions, "pathway_i"),
                "active_rand",
                self.seed,
                _KEY_ACTIVE,
            )
            .where(F.col("active_rand") < F.col("active_fraction"))
            .select("src", "dst")
        )

        return shall_keep_connections


def _params_out_csv(df, filename):
    of_interest = (
        "pathway_i",
        "total_touches",
        "structural_mean",
        "survival_rate",
        "pP_A",
        "pMu_A",
        "active_fraction_legacy",
        "debug",
    )
    cols = []
    for col in of_interest:
        if hasattr(df, col):
            cols.append(col)
    debug_info = df.select(*cols)
    helpers._write_csv(Circuit.pathway_to_str(debug_info), filename)


def _touch_counts_out_csv(df, filename):
    helpers._write_csv(Circuit.pathway_to_str(df.groupBy("pathway_i").count()), filename)


def _connection_counts_out_csv(df, filename):
    helpers._write_csv(
        Circuit.pathway_to_str(
            df.groupBy("pathway_i").sum("reduced_touch_counts_connection"),
        ),
        filename,
    )
