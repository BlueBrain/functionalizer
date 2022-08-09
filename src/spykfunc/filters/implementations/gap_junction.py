"""Collection of filters for gap junctions."""
import numpy as np
import pandas as pd

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

logger = get_logger(__name__)


class GapJunctionFilter(DatasetOperation):
    """Synchronize gap junctions.

    Ensures that:

    * Dendro-dentro and dendro-soma touches are present as src-dst and the
      corresponding dst-src pair.  The sections of the touches have to be
      aligned exactly, while the segment may deviate to neighboring ones.

    * Dendro-somatic touches: the structure of the neuron morphology is
      traversed and from all touches that are within a distance of 3 soma
      radii on the same branch only the "parent" ones are kept.
    """

    _checkpoint = True

    _columns = [
        (None, "efferent_junction_id"),
        (None, "afferent_junction_id"),
    ]

    def __init__(self, recipe, source, target, morphos):
        """Initialize the filter, using the morphology database."""
        super().__init__(recipe, source, target, morphos)
        self.__morphos = morphos

    def apply(self, circuit):
        """Apply both the dendrite-soma and dendrite-dendrite filters."""
        touches = circuit.df.withColumn("efferent_junction_id", F.col("synapse_id")).withColumn(
            "afferent_junction_id", F.col("synapse_id")
        )

        touches = touches.groupby(
            F.least(F.col("src"), F.col("dst")),
            F.shiftRight(F.greatest(F.col("src"), F.col("dst")), 15),
        ).applyInPandas(self._dendrite_match, touches.schema)
        dendrites = touches.where("afferent_section_id > 0 and efferent_section_id > 0")
        somas = (
            touches.where("afferent_section_id == 0 or efferent_section_id == 0")
            .groupby(F.shiftRight(F.col("src"), 4))
            .applyInPandas(self._soma_filter, touches.schema)
        )

        return somas.union(dendrites).repartition("src", "dst")

    @property
    def _soma_filter(self):
        """Removes junctions close to the soma.

        Filters for dendrite to soma gap junctions, removing junctions that are on parent
        branches of the dendrite and closer than 3 times the soma radius.
        """

        def _filter(df: pd.DataFrame) -> pd.DataFrame:
            if len(df) == 0:
                return df

            src = df.src.values
            dst = df.dst.values
            sec = df.efferent_section_id.values
            seg = df.efferent_segment_id.values
            soma = df.afferent_section_id.values

            jid1 = df.efferent_junction_id.values
            jid2 = df.afferent_junction_id.values

            # This may be passed to us from pyspark as object type,
            # breaking np.unique.
            morphos = np.asarray(df.src_morphology.values, dtype="U")
            activated = np.zeros_like(src, dtype=bool)
            distances = np.zeros_like(src, dtype=float)

            connections = np.stack((src, dst, morphos)).T
            unique_conns = np.unique(connections, axis=0)
            unique_morphos = np.unique(connections[:, 2])

            for m in unique_morphos:
                # Work one morphology at a time to conserve memory
                mdist = 3 * self.__morphos.soma_radius(m)

                # Resolve from indices matching morphology to connections
                idxs = np.where(unique_conns[:, 2] == m)[0]
                conns = unique_conns[idxs]
                for conn in conns:
                    # Indices where the connections match
                    idx = np.where((connections[:, 0] == conn[0]) & (connections[:, 1] == conn[1]))[
                        0
                    ]
                    # Match up gap-junctions that are reverted at the end
                    if len(idx) == 0 or soma[idx[0]] != 0:
                        continue
                    for i in idx:
                        distances[i] = self.__morphos.distance_to_soma(m, sec[i], seg[i])
                        path = self.__morphos.ancestors(m, sec[i])
                        for j in idx:
                            if i == j:
                                break
                            if (
                                activated[j]
                                and sec[j] in path
                                and abs(distances[i] - distances[j]) < mdist
                            ):
                                activated[j] = False
                        activated[i] = True
            # Activate reciprocal connections
            activated[np.isin(jid1, jid2[activated])] = True
            return df[activated]

        return _filter

    @staticmethod
    def _dendrite_match(df: pd.DataFrame) -> pd.DataFrame:
        """Match up dendrite gap junctions.

        Filters dendrite to dendrite junctions, keeping only junctions that have a match
        in both directions, with an optional segment offset of one.
        """
        if len(df) == 0:
            return df
        from spykfunc.filters.udfs import match_dendrites

        accept = match_dendrites(
            df.src.values,
            df.dst.values,
            df.efferent_section_id.values,
            df.efferent_segment_id.values,
            df.efferent_junction_id.values,
            df.afferent_section_id.values,
            df.afferent_segment_id.values,
            df.afferent_junction_id.values,
        ).astype(bool)
        return df[accept]


class GapJunctionProperties(DatasetOperation):
    """Assign gap-junction properties.

    This "filter" augments touches with properties of gap-junctions by adding
    the field

    - `gsyn` representing the conductance of the gap-junction with a
      default value of 0.2

    as specified by the `GapJunctionProperties` part of the recipe.

    """

    _reductive = False

    _columns = [
        (None, "gsyn"),
    ]

    def __init__(self, recipe, source, target, morphos):
        """Initialize the filter, extracting the conductance setting from the recipe."""
        super().__init__(recipe, source, target, morphos)
        self.conductance = recipe.gap_junction_properties.gsyn

    def apply(self, circuit):
        """Add properties to the circuit."""
        touches = circuit.df.withColumn("gsyn", F.lit(self.conductance))
        return touches
