"""A default filter plugin
"""
import numpy

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.filters.udfs import match_dendrites


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
        touches = circuit.df.withColumnRenamed('synapse_id', 'pre_junction') \
                            .withColumn('post_junction', F.col('pre_junction'))

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
