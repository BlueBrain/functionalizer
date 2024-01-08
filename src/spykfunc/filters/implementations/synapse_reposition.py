"""Shift synapses."""
import fnmatch
import itertools
import numpy as np

from pyspark.sql import functions as F

from spykfunc import schema
from spykfunc.filters import DatasetOperation

import sparkmanager as sm


class SynapseReposition(DatasetOperation):
    """Reposition synapses.

    Shifts the post-section of synapses for ChC and SpAA cells to the soma
    according to the `SynapsesReposition` rules of the recipe.
    """

    _reductive = False
    _required = False

    def __init__(self, recipe, source, target, morphos):
        """Initialize the filter, extracting the reposition part of the recipe."""
        super().__init__(recipe, source, target, morphos)
        self.reposition = self.convert_reposition(source, target, recipe.synapse_reposition)

    def apply(self, circuit):
        """Actually reposition the synapses."""
        axon_shift = _create_axon_section_udf(circuit.morphologies)

        with circuit.pathways(["fromMType", "toMType"]):
            circuit_w_reposition = circuit.with_pathway().join(
                self.reposition, "pathway_i", "left_outer"
            )

            patched = circuit_w_reposition.mapInPandas(
                axon_shift, circuit_w_reposition.schema
            ).drop("pathway_i", "reposition")

            return patched

    @staticmethod
    def convert_reposition(source, target, reposition):
        """Loader for pathways that need synapses to be repositioned."""
        src_mtype = source.mtype_values
        src_mtype_rev = {name: i for i, name in enumerate(src_mtype)}
        dst_mtype = target.mtype_values
        dst_mtype_rev = {name: i for i, name in enumerate(dst_mtype)}

        factor = len(src_mtype)

        paths = []
        for shift in reposition:
            src = src_mtype_rev.values()
            dst = dst_mtype_rev.values()
            if shift.type != "AIS":
                continue
            if shift.fromMType:
                src = [src_mtype_rev[m] for m in fnmatch.filter(src_mtype, shift.fromMType)]
            if shift.toMType:
                dst = [dst_mtype_rev[m] for m in fnmatch.filter(dst_mtype, shift.toMType)]
            paths.extend(itertools.product(src, dst))
        pathways = sm.createDataFrame(
            [(s + factor * d, True) for s, d in paths], schema.SYNAPSE_REPOSITION_SCHEMA
        )
        return F.broadcast(pathways)


def _create_axon_section_udf(morphology_db):
    """Creates a UDF to look up the first axon section in a morphology.

    Args:
        morphology_db: the morphology db

    Returns:
        a function than can be used by ``mapInPandas`` to shift synapses
    """

    def _shift_to_axon_section(dfs):
        """Shifts synapses to the first axon section."""
        for df in dfs:
            set_section_fraction = "afferent_section_pos" in df.columns
            set_soma_distance = "distance_soma" in df.columns
            for i in np.nonzero(df.reposition.values)[0]:
                morpho = df.dst_morphology.iloc[i]
                (idx, dist, frac, soma) = morphology_db.first_axon_section(morpho)
                df.afferent_section_id.iloc[i] = idx
                df.afferent_section_type.iloc[i] = 1  # Axon. Soma is 0, dendrites are higher
                df.afferent_segment_id.iloc[i] = 0  # First segment on the axon
                df.afferent_segment_offset.iloc[i] = dist
                if set_section_fraction:
                    df.afferent_section_pos.iloc[i] = frac
                if set_soma_distance:
                    df.distance_soma.iloc[i] = soma
            yield df

    return _shift_to_axon_section
