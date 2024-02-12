"""Filters to add properties to synapses."""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.circuit import Circuit
from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

import sparkmanager as sm

logger = get_logger(__name__)


class SynapseProperties(DatasetOperation):
    """Assign synapse properties.

    This "filter" augments touches with properties of synapses by adding
    the fields

    - `gsyn` following a Gamma-distribution
    - `d` following a Gamma-distribution
    - `f` following a Gamma-distribution
    - `u` following a truncated Normal-distribution
    - `dtc` following a truncated Normal-distribution
    - `nrrp` following a Poisson-distribution

    - `gsynSRSF`, taken verbatim from the recipe
    - `uHillCoefficient`, also taken verbatim  from the recipe

    as specified by the `SynapsesClassification` part of the recipe.

    To draw from the distributions, a seed derived from the `synapseSeed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = False
    _reductive = False

    _columns = [
        (None, "gsyn"),
        (None, "u"),
        (None, "d"),
        (None, "f"),
        (None, "dtc"),
        (None, "nrrp"),
        ("distance_soma", "delay"),
        (None, "syn_type_id"),
        (None, "syn_property_rule"),
    ]

    _optimal_group_by = {"src_hemisphere", "src_region", "src_mtype", "src_etype", "src_sclass"}

    def __init__(self, recipe, source, target, morphos):
        """Initialize the filter.

        Uses the synapse seed of the recipe to generate random numbers that are drawn when
        generating the synapse properties. Also uses the classification and property
        specification part of the recipe.
        """
        super().__init__(recipe, source, target, morphos)
        self.seed = recipe.seeds.synapseSeed
        logger.info("Using seed %d for synapse properties", self.seed)

        self.columns_recipe = recipe.synapse_properties.rules.required

        mapping = {}
        renaming = {}
        self.columns = []
        for name, mapped, _, values in Circuit.expand(self.columns_recipe, source, target):
            mapping[name] = values
            renaming[f"{name}_i"] = f"{mapped}_i"
            self.columns.append(f"{mapped}_i")

        raw_rules = recipe.synapse_properties.rules.to_pandas(mapping).rename(columns=renaming)
        raw_rules["rule_i"] = raw_rules.index

        self.rules = sm.broadcast(raw_rules[self.columns])

        columns = [
            c
            for c in raw_rules.columns
            if not (
                c.startswith("src_")
                or c.startswith("dst_")
                or c.startswith("to")
                or c.startswith("from")
            )
        ]

        self.classification = sm.createDataFrame(raw_rules[columns])

        classes = recipe.synapse_properties.classes.to_pandas().rename(columns={"id": "type"})
        classes["class_i"] = classes.index
        for optional in ("gsynSRSF", "uHillCoefficient"):
            if any(classes[optional].isna()):
                if not all(classes[optional].isna()):
                    raise ValueError(f"inconsistent values for {optional}")
                del classes[optional]
            else:
                self._columns.append((None, optional))

        self.classes = sm.createDataFrame(classes)

    def apply(self, circuit):
        """Add properties to the circuit."""
        with circuit.pathways(self.columns_recipe):
            pathways = (
                circuit.with_pathway()
                .select(self.columns + ["pathway_i"])
                .groupBy(["pathway_i"] + self.columns)
                .count()
                .mapInPandas(_assign_rules(self.rules), "pathway_i long, rule_i long")
                .join(F.broadcast(self.classification), "rule_i")
                .join(F.broadcast(self.classes), "type")
            )

            connections = (
                circuit.with_pathway()
                .groupBy("src", "dst")
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
                        "neuralTransmitterReleaseDelay + distance_soma / axonalConductionVelocity"
                    ).cast(T.FloatType()),
                ).drop(
                    "distance_soma",
                    "axonalConductionVelocity",
                    "neuralTransmitterReleaseDelay",
                )
            else:
                logger.warning(
                    "Generating the 'axonal_delay' property requires the 'distance_soma' field"
                )
                touches = touches.drop("axonalConductionVelocity", "neuralTransmitterReleaseDelay")

            # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
            t = (
                touches.withColumn(
                    "syn_type_id",
                    (F.when(F.col("type").substr(0, 1) == F.lit("E"), 100).otherwise(0)).cast(
                        T.ShortType()
                    ),
                )
                .withColumn("syn_property_rule", F.col("class_i").cast(T.ShortType()))
                .drop("type", "class_i", "rule_i")
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
            df["gsyn"] = fcts.gamma(seed, 0x1001, df["synapse_id"], df["gsyn"], df["gsynSD"])
            df["d"] = fcts.gamma(seed, 0x1002, df["synapse_id"], df["d"], df["dSD"])
            df["f"] = fcts.gamma(seed, 0x1003, df["synapse_id"], df["f"], df["fSD"])
            df["u"] = fcts.truncated_normal(seed, 0x1004, df["synapse_id"], df["u"], df["uSD"])
            df["dtc"] = fcts.truncated_normal(
                seed, 0x1005, df["synapse_id"], df["dtc"], df["dtcSD"]
            )
            df["nrrp"] = fcts.poisson(seed, 0x1006, df["synapse_id"], df["nrrp"])
            yield df
            del df

    return (
        connections.sortWithinPartitions("src")
        .mapInPandas(__generate, connections.schema)
        .drop("gsynSD", "dSD", "fSD", "uSD", "dtcSD", "synapse_id")
    )
