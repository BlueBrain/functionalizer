"""Synapse filters relating to synapse ids.

The synapse ids are used to generate random numbers for properties and
cutting synapses to match biological distributions.
"""
from pyspark.sql import functions as F
from pyspark.sql import Window

from spykfunc.filters import DatasetOperation


class AddIDFilter(DatasetOperation):
    """Adds a column `synapse_id` to a circuit.

    .. note:: The synapse id added by this filter will yield reproducible
              synapse properties, but should not be used for any processing
              involving probabilities, i.e., reduce and cut.
    """

    _checkpoint = True

    _columns = [(None, "synapse_id")]

    def apply(self, circuit):
        """Add a `synapse_id` field to `circuit`."""
        touches = circuit.df.repartition(circuit.df.rdd.getNumPartitions(), "src", "dst").cache()

        gid_window = Window.orderBy("src").rangeBetween(Window.unboundedPreceding, 0)

        gid_offsets = (
            touches.groupby("src")
            .count()
            .withColumn("gid_offset", F.sum("count").over(gid_window) - F.col("count"))
            .drop("count")
            .cache()
        )

        window = Window.partitionBy("src").orderBy("dst").rangeBetween(Window.unboundedPreceding, 0)

        offsets = (
            touches.join(F.broadcast(gid_offsets), "src")
            .groupby("src", "dst")
            .agg(
                F.count("gid_offset").alias("count"),
                F.first("gid_offset").alias("gid_offset"),
            )
            .withColumn(
                "offset",
                F.sum("count").over(window) - F.col("count") + F.col("gid_offset") - F.lit(1),
            )
            .drop("count", "gid_offset")
            .withColumnRenamed("src", "source")  # weird needed workaround
            .withColumnRenamed("dst", "target")  # weird needed workaround
            .cache()
        )

        return (
            touches.join(
                offsets,
                (touches.src == offsets.source) & (touches.dst == offsets.target),
            )
            .drop("source", "target")  # drop workaround column
            .withColumnRenamed("offset", "synapse_id")
        )


class DenseIDFilter(DatasetOperation):
    """Makes the `synapse_id` column continuous.

    This filter should be used if the range of the synapse id field exceeds
    numerical limitiations when generating output files.

    In particular, this filter should be included before
    :class:`~GapJunctionFilter` to make the gap junction ids fit into the
    numerical range accepted by neuron.
    """

    _checkpoint = True

    def apply(self, circuit):
        """Condense the synapse id field used to match gap-junctions."""
        assert "synapse_id" in circuit.df.columns, "DenseID must be called before GapJunction"

        touches = circuit.df.repartition(circuit.df.rdd.getNumPartitions(), "src", "dst").cache()

        gid_window = Window.orderBy("src").rangeBetween(Window.unboundedPreceding, 0)

        gid_offsets = (
            touches.groupby("src")
            .count()
            .withColumn("gid_offset", F.sum("count").over(gid_window) - F.col("count"))
            .drop("count")
            .cache()
        )

        window = Window.partitionBy("src").orderBy("dst").rangeBetween(Window.unboundedPreceding, 0)

        offsets = (
            touches.join(F.broadcast(gid_offsets), "src")
            .groupby("src", "dst")
            .agg(
                F.count("gid_offset").alias("count"),
                F.first("gid_offset").alias("gid_offset"),
            )
            .withColumn(
                "offset",
                F.sum("count").over(window) - F.col("count") + F.col("gid_offset") - F.lit(1),
            )
            .drop("count", "gid_offset")
            .withColumnRenamed("src", "source")  # weird needed workaround
            .withColumnRenamed("dst", "target")  # weird needed workaround
            .cache()
        )

        id_window = Window.partitionBy("src", "dst").orderBy("old_synapse_id")

        return (
            touches.join(
                offsets,
                (touches.src == offsets.source) & (touches.dst == offsets.target),
            )
            .drop("source", "target")  # drop workaround column
            .withColumnRenamed("synapse_id", "old_synapse_id")
            .withColumn("synapse_id", F.row_number().over(id_window) + F.col("offset"))
            .drop("old_synapse_id", "offset")
        )
