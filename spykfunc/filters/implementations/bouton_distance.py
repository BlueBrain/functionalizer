"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation
from spykfunc.definitions import CellClass


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, source, target, morphos):
        self.distances = recipe.bouton_distances

    def apply(self, circuit):
        """Apply filter
        """
        def pos(cls):
            """Save index function returning -1 if not found
            """
            try:
                return circuit.target.cell_classes.index(cls)
            except ValueError:
                return -1

        # Use broadcast of Neuron version
        return circuit.df.where(
            "(distance_soma >= {:f} AND dst_syn_class_i = {:d}) OR "
            "(distance_soma >= {:f} AND dst_syn_class_i = {:d})".format(
                self.distances.inhibitorySynapsesDistance,
                pos("INH"),
                self.distances.excitatorySynapsesDistance,
                pos("EXC")
            )
        )
