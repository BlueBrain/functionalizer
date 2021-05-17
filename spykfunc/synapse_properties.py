"""
Additional "Synapse property fields
"""
from __future__ import absolute_import
import numpy as np
import pandas as pd
import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from .utils import get_logger

logger = get_logger(__name__)


def compute_additional_h5_fields(circuit, reduced, classification_matrix, properties_df, seed):
    """Compute randomized properties of touches based on their classification.

    :param circuit: the full synapse connection circuit
    :param reduced: a reduced connection count with only one pair of src, dst each
    :param classification_matrix: a matrix associating connection properties with a class
    :param properties_df: a dataframe containing the properties of connection classes
    :param seed: the seed to use for the random numbers underlying the properties
    """
    syn_class_dims = classification_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]


    # Compute the index for the matrix as in a flat array
    connections = reduced.withColumn("classification_index",
                                     reduced.src_mtype_i * index_length[1] +
                                     reduced.src_etype_i * index_length[2] +
                                     reduced.src_syn_class_i * index_length[3] +
                                     reduced.dst_mtype_i * index_length[4] +
                                     reduced.dst_etype_i * index_length[5] +
                                     reduced.dst_syn_class_i)

    # Convert the numpy matrix into a dataframe and join to get the right
    # property index
    syn_class_raw = pd.DataFrame({
        "classification_index": np.arange(classification_matrix.size),
        "classification_i": classification_matrix.flatten()
    })
    syn_class = F.broadcast(sm.createDataFrame(syn_class_raw).coalesce(1))
    connections = connections.join(syn_class, "classification_index").drop("classification_index")

    # Join with Syn Prop Class
    properties_df = properties_df.alias("synprop")  # Synprops is globally cached and broadcasted

    connections = (
        connections
        .join(
            F.broadcast(properties_df),
            connections.classification_i == properties_df._class_i
        )
        .drop(
            "classification_i",
            "_class_i"
        )
    )

    connections = _add_randomized_connection_properties(connections, seed)

    # Identical columns should be dropped later
    overlap = set(connections.columns) & set(circuit.columns)
    for col in overlap:
        connections = connections.withColumnRenamed(col, f"_{col}")

    touches = (
        circuit
        .alias("c")
        .join((
                connections
                .drop(
                    "id",
                    "fromEType",
                    "fromMType",
                    "fromSClass",
                    "toEType",
                    "toMType",
                    "toSClass",
                )
                .alias("conn")
            ),
            [F.col("c.src") == F.col("_src"),
             F.col("c.dst") == F.col("_dst")]
        )
        .drop(*[f"_{col}" for col in overlap])
    )

    # Compute #1: delaySomaDistance
    if "distance_soma" in touches.columns:
        touches = (
            touches
            .withColumn(
                "delay",
                F.expr("conn.neuralTransmitterReleaseDelay + distance_soma / conn.axonalConductionVelocity")
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
            (F.when(F.col("conn.type").substr(0, 1) == F.lit('E'), 100).otherwise(0)
             ).cast(T.ShortType())
        )
        .withColumn(
            "syn_property_rule",
             F.col("conn._prop_i").cast(T.ShortType())
        )
        .drop(
            "type",
            "_prop_i",
        )
    )

    # Required for SONATA support
    if not hasattr(t, "edge_type_id"):
        t = t.withColumn(
            "edge_type_id",
            F.lit(0)
        )

    return t


def _add_randomized_connection_properties(connections: DataFrame, seed: int) -> DataFrame:
    """Add connection properties drawn from random distributions

    The following properties will be generated in place:

    * ``gsyn`` is the conductance of the synapse (nanosiemens) (float, Gamma)
    * ``u`` is the `u` parameter in the TM model (0-1) (float, truncated Gaussian)
    * ``d`` is the time constant of depression (milliseconds) (int, Gamma)
    * ``f`` is the time constant of facilitation (milliseconds) (int, Gamma)
    * ``dtc`` is the decay time constant (milliseconds) (float, truncated Gaussian)
    * ``nrrp`` is the number of readily releasable pool vesicles (int, Poisson)

    Args:
        connections: The dataframe to add the connections to.
        seed: Basic seed for the random number distributions.  Every
              property operates in a separate random number subspace.
    Returns:
        The input dataframe with synaptic properties set to randomly drawn
        values, and additional standard deviation columns removed
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

    return (
        connections
        .sortWithinPartitions("src")
        .mapInPandas(__generate, connections.schema)
        .drop("gsynSD", "dSD", "fSD", "uSD", "dtcSD")
    )
