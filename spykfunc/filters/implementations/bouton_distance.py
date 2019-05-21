"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty
from spykfunc.definitions import CellClass


class InitialBoutonDistance(GenericProperty):
    """Info/filter for Synapses Bouton Distance"""
    # implies _supported_attrs
    _map_attrs = {
        'defaultInhSynapsesDistance': 'inhibitorySynapsesDistance',
        'defaultExcSynapsesDistance': 'excitatorySynapsesDistance'
    }
    inhibitorySynapsesDistance = 5.0
    excitatorySynapsesDistance = 25.0

    def is_distance_valid(self, cell_class, soma_axon_distance):
        if cell_class == CellClass.CLASS_INH:
            return soma_axon_distance >= self.inhibitorySynapsesDistance
        if cell_class == CellClass.CLASS_EXC:
            return soma_axon_distance >= self.excitatorySynapsesDistance


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, morphos, stats):
        fragment = recipe.xml.find("InitialBoutonDistance")
        self.distances = self.convert_info(fragment)

    @classmethod
    def convert_info(cls, fragment):
        if hasattr(fragment, "items"):
            infos = {k: v for k, v in fragment.items()}
            return InitialBoutonDistance(**infos)
        else:
            return InitialBoutonDistance()

    def apply(self, circuit):
        """Apply filter
        """
        # Use broadcast of Neuron version
        return circuit.df.where("(distance_soma >= %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma >= %f AND dst_syn_class_i = %d)" % (
                                    self.distances.inhibitorySynapsesDistance,
                                    CellClass.INH.fzer_index,
                                    self.distances.excitatorySynapsesDistance,
                                    CellClass.EXC.fzer_index)
                                )


class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """
    Reverse version of Bouton Distance filter, only keeping outliers.
    """

    _visible = False

    def apply(self, circuit):
        return circuit.df.where("(distance_soma < %f AND dst_syn_class_i = %d) OR "
                                "(distance_soma < %f AND dst_syn_class_i = %d)" % (
                                    self.distances.inhibitorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_INH],
                                    self.distances.excitatorySynapsesDistance,
                                    self.synapse_classes_indexes[CellClass.CLASS_EXC])
                                )
