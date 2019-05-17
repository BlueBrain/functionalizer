"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation


class SynapseProperties(DatasetOperation):
    """Assign synapse properties

    This "filter" augments touches with properties of synapses by

    * shifting the post-section of synapses for ChC and SpAA cells to the
      soma according to the `SynapsesReposition` rules of the recipe.
    * adding the fields

      - `gsyn` following a Gamma-distribution,
      - `d` following a Gamma-distribution,
      - `f` following a Gamma-distribution,
      - `u` following a truncated Normal-distribution,
      - `dtc` following a truncated Normal-distribution,
      - `nrrp` following a Poisson-distribution

      as specified by the `SynapsesClassification` part of the recipe.

    To draw from the distributions, a seed derived from the `synapseSeed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = True
    _morphologies = True

    def __init__(self, recipe, morphos, stats):
        self.recipe = recipe

    def apply(self, circuit):
        from spykfunc.synapse_properties import patch_ChC_SPAA_cells
        from spykfunc.synapse_properties import compute_additional_h5_fields

        if self._morphologies:
            circuit.df = patch_ChC_SPAA_cells(circuit.df,
                                              circuit.morphologies,
                                              circuit.synapse_reposition_pathways)

        extended_touches = compute_additional_h5_fields(
            circuit.df,
            circuit.reduced,
            circuit.synapse_class_matrix,
            circuit.synapse_class_properties,
            self.recipe.seeds.synapseSeed
        )
        return extended_touches
