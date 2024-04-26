"""Filters to add properties to synapses."""

import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

logger = get_logger(__name__)


class SynapseProperties(DatasetOperation):
    """Assign synapse properties.

    This "filter" augments touches with properties of synapses by adding
    the fields

    - `conductance` following a Gamma-distribution
    - `depression_time` following a Gamma-distribution
    - `facilitation_time` following a Gamma-distribution
    - `u_syn` following a truncated Normal-distribution
    - `decay_time` following a truncated Normal-distribution
    - `n_rrp_vesicles` following a Poisson-distribution

    - `conductance_scale_factor`, taken verbatim from the recipe
    - `u_hill_coefficient`, also taken verbatim  from the recipe

    as specified by the `synapse_properties.classes` part of the recipe.

    To draw from the distributions, a seed derived from the `seed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = False
    _reductive = False

    _columns = [
        (None, "conductance"),
        (None, "u_syn"),
        (None, "depression_time"),
        (None, "facilitation_time"),
        (None, "decay_time"),
        (None, "n_rrp_vesicles"),
        ("distance_soma", "delay"),
        (None, "syn_type_id"),
        (None, "syn_property_rule"),
    ]

    def __init__(self, recipe, source, target):
        """Initialize the filter.

        Uses the synapse seed of the recipe to generate random numbers that are drawn when
        generating the synapse properties. Also uses the classification and property
        specification part of the recipe.
        """
        super().__init__(recipe, source, target)
        self.seed = recipe.get("seed")
        logger.info("Using seed %d for synapse properties", self.seed)

        classes = recipe.as_pandas("synapse_properties.classes")
        rules = recipe.as_pandas("synapse_properties.rules")
        # Save all numerical columns (these are directional ones used for filtering)
        self.columns = sorted(c for c in rules.columns if c.endswith("_i"))
        values = recipe.get_values(self.columns)
        self.factors = [len(values[c]) for c in self.columns]

        classes["class_i"] = classes.index
        rules["rule_i"] = rules.index

        self.rules = sm.broadcast(rules[self.columns])
        self.classification = sm.createDataFrame(
            rules[[c for c in rules.columns if c not in self.columns]]
        )
        self.classes = sm.createDataFrame(classes)

        for optional in ("conductance_scale_factor", "u_hill_coefficient"):
            if optional in classes.columns:
                self._columns.append((None, optional))

    def apply(self, circuit):
        """Add properties to the circuit."""
        add_pathway = self.pathway_functions(self.columns, self.factors)
        circuit_with_pathway = add_pathway(circuit.df)

        pathways = (
            circuit_with_pathway.select(self.columns + ["pathway_i"])
            .groupBy(["pathway_i"] + self.columns)
            .count()
            .mapInPandas(_assign_rules(self.rules), "pathway_i long, rule_i long")
            .join(F.broadcast(self.classification), "rule_i")
            .join(F.broadcast(self.classes), "class")
        )

        connections = (
            circuit_with_pathway.groupBy("src", "dst")
            .agg(
                F.min("pathway_i").alias("pathway_i"),
                F.min("synapse_id").alias("synapse_id"),
            )
            .join(F.broadcast(pathways), "pathway_i")
            .drop("pathway_i")
        )
        connections = _add_randomized_connection_properties(connections, self.seed)

        touches = (
            circuit.df.alias("c")
            .join(
                connections.withColumnRenamed("src", "_src").withColumnRenamed("dst", "_dst"),
                [F.col("c.src") == F.col("_src"), F.col("c.dst") == F.col("_dst")],
            )
            .drop("_src", "_dst")
        )

        # Compute delaySomaDistance
        if "distance_soma" in touches.columns:
            touches = touches.withColumn(
                "delay",
                F.expr(
                    "neural_transmitter_release_delay + distance_soma / axonal_conduction_velocity"
                ).cast(T.FloatType()),
            ).drop(
                "distance_soma",
                "axonal_conduction_velocity",
                "neural_transmitter_release_delay",
            )
        else:
            logger.warning("Generating the 'delay' property requires the 'distance_soma' field")
            touches = touches.drop(
                "axonal_conduction_velocity",
                "neural_transmitter_release_delay",
            )

        # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
        t = (
            touches.withColumn(
                "syn_type_id",
                (F.when(F.col("class").substr(0, 1) == F.lit("E"), 100).otherwise(0)).cast(
                    T.ShortType()
                ),
            )
            .withColumn("syn_property_rule", F.col("class_i").cast(T.ShortType()))
            .drop("class", "class_i", "rule_i")
        )
        return t


def _assign_rules(spark_rules):
    """Assign rule indices to a series of dataframes."""

    def f(dfs):
        def _assign_rule(row):
            """Return the last matching rule index for row."""
            rules = spark_rules.value
            for col in rules.columns:
                sel = (rules[col] == row[col]) | (rules[col] == -1)
                rules = rules[sel]
            if len(rules) == 0:
                msg = " ".join(f"{col}: {row[col]}" for col in rules.columns)
                raise KeyError(msg)
            return rules.index[-1]

        for df in dfs:
            df["rule_i"] = df.apply(_assign_rule, axis=1)
            yield df[["pathway_i", "rule_i"]]

    return f


def _add_randomized_connection_properties(connections, seed: int):
    """Add connection properties drawn from random distributions."""

    def __generate(data):
        import spykfunc.filters.udfs as fcts

        for df in data:
            df["conductance"] = fcts.gamma(
                seed,
                0x1001,
                df["synapse_id"],
                df["conductance_mu"],
                df["conductance_sd"],
            )
            df["depression_time"] = fcts.gamma(
                seed,
                0x1002,
                df["synapse_id"],
                df["depression_time_mu"],
                df["depression_time_sd"],
            )
            df["facilitation_time"] = fcts.gamma(
                seed,
                0x1003,
                df["synapse_id"],
                df["facilitation_time_mu"],
                df["facilitation_time_sd"],
            )
            df["u_syn"] = fcts.truncated_normal(
                seed, 0x1004, df["synapse_id"], df["u_syn_mu"], df["u_syn_sd"]
            )
            df["decay_time"] = fcts.truncated_normal(
                seed, 0x1005, df["synapse_id"], df["decay_time_mu"], df["decay_time_sd"]
            )
            df["n_rrp_vesicles"] = fcts.poisson(
                seed, 0x1006, df["synapse_id"], df["n_rrp_vesicles_mu"]
            ).astype("int16")
            yield df
            del df

    fields = []
    to_drop = []
    for field in connections.schema:
        if field.name.endswith("_mu"):
            to_drop.append(field.name)
            if field.name == "n_rrp_vesicles_mu":
                fields.append(T.StructField(field.name[:-3], T.ShortType(), False))
            else:
                fields.append(T.StructField(field.name[:-3], T.DoubleType(), False))
        elif field.name.endswith("_sd"):
            to_drop.append(field.name)
        fields.append(field)

    return (
        connections.sortWithinPartitions("src")
        .mapInPandas(__generate, T.StructType(fields))
        .drop("synapse_id", *to_drop)
    )
