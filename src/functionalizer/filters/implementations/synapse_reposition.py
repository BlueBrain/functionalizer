"""Shift synapses."""

import numpy as np
import pandas as pd
import sparkmanager as sm

from functionalizer.filters import DatasetOperation, FilterInitializationError


class SynapseReposition(DatasetOperation):
    """Reposition synapses.

    Shifts the post-section of synapses for ChC and SpAA cells to the soma
    according to the `SynapsesReposition` rules of the recipe.
    """

    _reductive = False
    _required = False

    def __init__(self, recipe, source, target):
        """Initialize the filter, extracting the reposition part of the recipe."""
        super().__init__(recipe, source, target)
        if not recipe.get("synapse_reposition"):
            raise FilterInitializationError("'synapse_reposition' not in recipe")
        self.columns, self.reposition = recipe.as_matrix("synapse_reposition")
        self.unset_value = len(recipe.get("synapse_reposition"))

    def apply(self, circuit):
        """Actually reposition the synapses."""
        axon_shift = _create_axon_section_udf(circuit.target.morphologies)

        reposition_np = self.reposition.flatten() != self.unset_value
        reposition_pd = pd.DataFrame(
            {"pathway_i": np.arange(len(reposition_np)), "reposition": reposition_np}
        )

        add_pathway = self.pathway_functions(self.columns, self.reposition.shape)
        circuit_w_reposition = add_pathway(circuit.df).join(
            sm.createDataFrame(reposition_pd), "pathway_i", "left_outer"
        )

        patched = circuit_w_reposition.mapInPandas(axon_shift, circuit_w_reposition.schema).drop(
            "pathway_i", "pathway_str", "reposition"
        )

        return patched


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
