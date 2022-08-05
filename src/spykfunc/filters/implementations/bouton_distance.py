"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, source, target, morphos):
        super().__init__(recipe, source, target, morphos)
        self.distances = recipe.bouton_distances

    def apply(self, circuit):
        """Apply filter"""

        def pos(cls):
            """Save index function returning -1 if not found"""
            try:
                return circuit.target.sclass_values.index(cls)
            except ValueError:
                return -1

        # Use broadcast of Neuron version
        return circuit.df.where(
            f"(distance_soma >= {self.distances.inhibitorySynapsesDistance:f} AND"
            f" dst_sclass_i = {pos('INH'):d})"
            " OR "
            f"(distance_soma >= {self.distances.excitatorySynapsesDistance:f} AND"
            f" dst_sclass_i = {pos('EXC'):d})"
        )
