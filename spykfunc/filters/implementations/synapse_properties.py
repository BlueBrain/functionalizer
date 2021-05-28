"""Filters to add properties to synapses
"""
from collections import defaultdict, OrderedDict
from typing import Dict, List
import fnmatch
import numpy as np
import pandas as pd

import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

from spykfunc import schema
from spykfunc.circuit import Circuit
from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

logger = get_logger(__name__)


class SynapseProperties(DatasetOperation):
    """Assign synapse properties

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

    _checkpoint = True

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
        (None, "edge_type_id"),
    ]

    def __init__(self, recipe, source, target, morphos):
        self.seed = recipe.seeds.synapseSeed
        logger.info("Using seed %d for synapse properties", self.seed)

        self.columns = recipe.synapse_properties.rules.required
        logger.info(
            "Using the following columns for synapse properties: %s",
            ", ".join(self.columns)
        )

        # A Pandas dataframe mapping from pathway_i to properties
        self.properties = self.convert_properties(
            recipe.synapse_properties,
            self.columns,
            source,
            target
        )

        if "gsynSRSF" in self.properties.columns:
            self._columns.append((None, "gsynSRSF"))
        if "uHillCoefficient" in self.properties.columns:
            self._columns.append((None, "uHillCoefficient"))

    def apply(self, circuit):
        """Add properties to the circuit
        """
        with circuit.pathways(self.columns):
            connections = (
                circuit
                .with_pathway()
                .groupBy("src", "dst")
                .agg(
                    F.min("pathway_i").alias("pathway_i"),
                    F.min("synapse_id").alias("synapse_id")
                )
                .join(F.broadcast(self.properties), "pathway_i")
                .drop("pathway_i")
            )
            connections = _add_randomized_connection_properties(connections, self.seed)

            touches = (
                circuit
                .df
                .alias("c")
                .join(
                    connections
                    .withColumnRenamed("src", "_src")
                    .withColumnRenamed("dst", "_dst"),
                    [F.col("c.src") == F.col("_src"),
                     F.col("c.dst") == F.col("_dst")]
                )
                .drop("_src", "_dst")
            )

            # Compute delaySomaDistance
            if "distance_soma" in touches.columns:
                touches = (
                    touches
                    .withColumn(
                        "delay",
                        F.expr("neuralTransmitterReleaseDelay + distance_soma / axonalConductionVelocity")
                        .cast(T.FloatType())
                    )
                    .drop(
                        "distance_soma",
                        "axonalConductionVelocity",
                        "neuralTransmitterReleaseDelay",
                    )
                )
            else:
                logger.warning("Generating the 'axonal_delay' property requires the 'distance_soma' field")
                touches = touches.drop("axonalConductionVelocity", "neuralTransmitterReleaseDelay")

            # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
            t = (
                touches
                .withColumn(
                    "syn_type_id",
                    (F.when(F.col("type").substr(0, 1) == F.lit('E'), 100).otherwise(0)
                     ).cast(T.ShortType())
                )
                .withColumn(
                    "syn_property_rule",
                     F.col("_prop_i").cast(T.ShortType())
                )
                .drop("type", "_prop_i")
            )

            # Required for SONATA support
            if not hasattr(t, "edge_type_id"):
                t = t.withColumn("edge_type_id", F.lit(0))
            return t

        extended_touches = compute_additional_h5_fields(
            circuit.df,
            circuit.reduced,
            self.rules,
            self.properties,
            self.seed
        )
        return extended_touches

    @staticmethod
    def convert_properties(properties, columns, source, target):
        """Merges synapse class assignment and properties
        """
        flattened_pathways = properties.rules.to_matrix({
            n: v
            for n, _, _, v in Circuit.expand(columns, source, target)
        })
        pathways = sm.createDataFrame(pd.DataFrame({
            "pathway_i": np.arange(flattened_pathways.size),
            "_class_i": flattened_pathways,
        }))

        classification = _load_from_recipe(properties.rules, schema.SYNAPSE_CLASSIFICATION_SCHEMA) \
            .rename(columns={"_i": "_class_i"})
        properties = _load_from_recipe(properties.classes, schema.SYNAPSE_PROPERTY_SCHEMA, trim=True) \
            .rename(columns={"_i": "_prop_i"}) \
            .set_index("id")

        merged_properties = sm.createDataFrame(
            classification
            .join(properties, on="type")
        )
        logger.info("Found %d synapse property entries", merged_properties.count())

        return (
            pathways
            .join(merged_properties, pathways._class_i == merged_properties._class_i)
            .drop("_class_i")
        )
        # "_prop_i" could also be dropped, but is required for debug output
        # later


_spark_t_to_py = {
    T.ShortType: int,
    T.IntegerType: int,
    T.LongType: int,
    T.FloatType: float,
    T.DoubleType: float,
    T.StringType: str,
    T.BooleanType: bool
}


def cast_in_eq_py_t(val, spark_t):
    return _spark_t_to_py[spark_t.__class__](val)


def _load_from_recipe(recipe_group, group_schema, *, trim: bool = False) -> pd.DataFrame:
    if trim:
        fields = []
        for field in reversed(list(group_schema.fields)):
            haves = [hasattr(entry, field.name) for entry in recipe_group]
            if all(haves):
                fields.append(field)
            else:
                logger.warning(
                    "Field %s not present in all rules, skipping conversion",
                    field.name
                )
        group_schema = T.StructType(fields)
    data = [tuple(cast_in_eq_py_t(getattr(entry, field.name), field.dataType)
                  for field in group_schema)
            for entry in recipe_group]
    return pd.DataFrame.from_records(data, columns=[f.name for f in group_schema])


def _add_randomized_connection_properties(connections: DataFrame, seed: int) -> DataFrame:
    """Add connection properties drawn from random distributions
    """
    def __generate(data):
        import spykfunc.filters.udfs as fcts
        for df in data:
            df["gsyn"] = fcts.gamma(seed, 0x1001, df["synapse_id"], df["gsyn"], df["gsynSD"])
            df["d"] = fcts.gamma(seed, 0x1002, df["synapse_id"], df["d"], df["dSD"])
            df["f"] = fcts.gamma(seed, 0x1003, df["synapse_id"], df["f"], df["fSD"])
            df["u"] = fcts.truncated_normal(seed, 0x1004, df["synapse_id"], df["u"], df["uSD"])
            df["dtc"] = fcts.truncated_normal(seed, 0x1005, df["synapse_id"], df["dtc"], df["dtcSD"])
            df["nrrp"] = fcts.poisson(seed, 0x1006, df["synapse_id"], df["nrrp"])
            yield df
            del df

    return (
        connections
        .sortWithinPartitions("src")
        .mapInPandas(__generate, connections.schema)
        .drop("gsynSD", "dSD", "fSD", "uSD", "dtcSD", "synapse_id")
    )
