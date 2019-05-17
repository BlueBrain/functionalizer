"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation
from spykfunc.definitions import CellClass


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, morphos, stats):
        self.synapses_distance = recipe.synapses_distance

    def apply(self, circuit):
        """Apply filter
        """
        # Use broadcast of Neuron version
        return circuit.df.where("(distance_soma >= %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma >= %f AND dst_syn_class_i = %d)" % (
                                    self.synapses_distance.inhibitorySynapsesDistance,
                                    CellClass.INH.fzer_index,
                                    self.synapses_distance.excitatorySynapsesDistance,
                                    CellClass.EXC.fzer_index)
                                )


# -------------------------------------------------------------------------------------------------
class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """
    Reverse version of Bouton Distance filter, only keeping outliers.
    """

    _visible = False

    def apply(self, circuit):
        return circuit.df.where("(distance_soma < %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma < %f AND dst_syn_class_i = %d)" % (
                                    self.synapses_distance.inhibitorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_INH],
                                    self.synapses_distance.excitatorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_EXC])
                                )
