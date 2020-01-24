"""A default filter plugin
"""
import math
import numpy

from pyspark.sql import functions as F
from pyspark.sql import Window

from spykfunc.filters import DatasetOperation
from spykfunc.filters.udfs import match_dendrites
from spykfunc.recipe import GenericProperty
from spykfunc.utils import get_logger

logger = get_logger(__name__)

class GapJunctionFilter(DatasetOperation):
    """Synchronize gap junctions

    Ensures that:

    * Dendro-dentro and dendro-soma touches are present as src-dst and the
      corresponding dst-src pair.  The sections of the touches have to be
      aligned exactly, while the segment may deviate to neighboring ones.

    * Dendro-somatic touches: the structure of the neuron morphology is
      traversed and from all touches that are within a distance of 3 soma
      radii on the same branch only the "parent" ones are kept.
    """

    _checkpoint = True

    def __init__(self, recipe, source, target, morphos):
        self.__morphos = morphos

    def apply(self, circuit):
        """Apply both the dendrite-soma and dendrite-dendrite filters.
        """
        touches = circuit.df.withColumn('pre_junction', F.col('synapse_id')) \
                            .withColumn('post_junction', F.col('synapse_id'))

        trim = self._create_soma_filter_udf(touches)
        match = self._create_dendrite_match_udf(touches)

        touches = touches.groupby(F.least(F.col("src"),
                                          F.col("dst")),
                                  F.shiftRight(F.greatest(F.col("src"),
                                                          F.col("dst")),
                                               15)) \
                         .apply(match)
        dendrites = touches.where("post_section > 0 and pre_section > 0")
        somas = touches.where("post_section == 0 or pre_section == 0") \
                       .groupby(F.shiftRight(F.col("src"), 4)) \
                       .apply(trim)

        return somas.union(dendrites) \
                    .repartition("src", "dst")

    def _create_soma_filter_udf(self, circuit):
        """Produce UDF to filter soma touches/gap junctions

        Filter dendrite to soma gap junctions, removing junctions that are
        on parent branches of the dendrite and closer than 3 times the soma
        radius.

        :param circuit: a dataframe whose schema to re-use for the UDF
        """
        @F.pandas_udf(circuit.schema, F.PandasUDFType.GROUPED_MAP)
        def trim_touches(data):
            """
            :param data: a Pandas dataframe
            """
            if len(data) == 0:
                return data

            src = data.src.values
            dst = data.dst.values
            sec = data.pre_section.values
            seg = data.pre_segment.values
            soma = data.post_section.values

            jid1 = data.pre_junction.values
            jid2 = data.post_junction.values

            # This may be passed to us from pyspark as object type,
            # breaking numpy.unique.
            morphos = numpy.asarray(data.src_morphology.values, dtype="U")
            activated = numpy.zeros_like(src, dtype=bool)
            distances = numpy.zeros_like(src, dtype=float)

            connections = numpy.stack((src, dst, morphos)).T
            unique_conns = numpy.unique(connections, axis=0)
            unique_morphos = numpy.unique(connections[:, 2])

            for m in unique_morphos:
                # Work one morphology at a time to conserve memory
                mdist = 3 * self.__morphos.soma_radius(m)

                # Resolve from indices matching morphology to connections
                idxs = numpy.where(unique_conns[:, 2] == m)[0]
                conns = unique_conns[idxs]
                for conn in conns:
                    # Indices where the connections match
                    idx = numpy.where((connections[:, 0] == conn[0]) &
                                      (connections[:, 1] == conn[1]))[0]
                    # Match up gap-junctions that are reverted at the end
                    if len(idx) == 0 or soma[idx[0]] != 0:
                        continue
                    for i in idx:
                        distances[i] = self.__morphos.distance_to_soma(m, sec[i], seg[i])
                        path = self.__morphos.ancestors(m, sec[i])
                        for j in idx:
                            if i == j:
                                break
                            if activated[j] and sec[j] in path and \
                                    abs(distances[i] - distances[j]) < mdist:
                                activated[j] = False
                        activated[i] = True
            # Activate reciprocal connections
            activated[numpy.isin(jid1, jid2[activated])] = True
            return data[activated]
        return trim_touches

    def _create_dendrite_match_udf(self, circuit):
        """Produce UDF to match dendrite touches/gap junctions

        Filter dendrite to dendrite junctions, keeping only junctions that
        have a match in both directions, with an optional segment offset of
        one.

        :param circuit: a dataframe whose schema to re-use for the UDF
        """
        @F.pandas_udf(circuit.schema, F.PandasUDFType.GROUPED_MAP)
        def match_touches(data):
            """
            :param data: a Pandas dataframe
            """
            if len(data) == 0:
                return data
            accept = match_dendrites(data.src.values,
                                     data.dst.values,
                                     data.pre_section.values,
                                     data.pre_segment.values,
                                     data.pre_junction.values,
                                     data.post_section.values,
                                     data.post_segment.values,
                                     data.post_junction.values).astype(bool)
            return data[accept]
        return match_touches


class DenseIDFilter(DatasetOperation):

    _checkpoint = True

    def apply(self, circuit):
        """Condense the synapse id field used to match gap-junctions
        """
        assert "synapse_id" in circuit.df.columns, \
            "DenseID must be called before GapJunction"

        touches = circuit.df.repartition(
            circuit.df.rdd.getNumPartitions(),
            "src",
            "dst"
        ).cache()

        gid_window = (
            Window
            .orderBy("src")
            .rangeBetween(Window.unboundedPreceding, 0)
        )

        gid_offsets = (
            touches
            .groupby("src")
            .count()
            .withColumn("gid_offset", F.sum("count").over(gid_window) - F.col("count"))
            .drop("count")
            .cache()
        )

        window = (
            Window
            .partitionBy("src")
            .orderBy("dst")
            .rangeBetween(Window.unboundedPreceding, 0)
        )

        offsets = (
            touches
            .join(F.broadcast(gid_offsets), "src")
            .groupby("src", "dst")
            .agg(
                F.count("gid_offset").alias("count"),
                F.first("gid_offset").alias("gid_offset")
            )
            .withColumn(
                "offset",
                F.sum("count").over(window) - F.col("count") + F.col("gid_offset") - F.lit(1)
            )
            .drop("count", "gid_offset")
            .withColumnRenamed("src", "source")  # weird needed workaround
            .withColumnRenamed("dst", "target")  # weird needed workaround
            .cache()
        )

        id_window = (
            Window
            .partitionBy("src", "dst")
            .orderBy("old_synapse_id")
        )

        return (
            touches
            .join(offsets, (touches.src == offsets.source) & (touches.dst == offsets.target))
            .drop("source", "target")  # drop workaround column
            .withColumnRenamed("synapse_id", "old_synapse_id")
            .withColumn("synapse_id", F.row_number().over(id_window) + F.col("offset"))
            .drop("old_synapse_id", "offset")
        )

class GapJunctionProperty(GenericProperty):
    """Class representing a gap-junction property"""
    _supported_attrs = {"gsyn"}
    gsyn = 0.2


class GapJunctionProperties(DatasetOperation):
    """Assign gap-junction properties

    This "filter" augments touches with properties of gap-junctions by adding
    the field

    - `gsyn` representing the conductance of the gap-junction with a
      default value of 0.2

    as specified by the `GapJunctionProperties` part of the recipe.

    """

    def __init__(self, recipe, source, target, morphos):
        self.conductance = GapJunctionProperty.load_one(recipe.xml).gsyn

    def apply(self, circuit):
        """Add properties to the circuit
        """

        touches = (
            circuit
            .df
            .withColumn("gsyn", F.lit(self.conductance))
        )

        return touches
