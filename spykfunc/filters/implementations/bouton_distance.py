"""A default filter plugin
"""
from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty
from spykfunc.definitions import CellClass


class InitialBoutonDistance(GenericProperty):
    """Info/filter for Synapses Bouton Distance
    """

    # implies _supported_attrs
    _map_attrs = {
        "defaultInhSynapsesDistance": "inhibitorySynapsesDistance",
        "defaultExcSynapsesDistance": "excitatorySynapsesDistance",
    }
    inhibitorySynapsesDistance = 5.0
    excitatorySynapsesDistance = 25.0

    def is_distance_valid(self, cell_class, soma_axon_distance):
        if cell_class == CellClass.CLASS_INH:
            return soma_axon_distance >= self.inhibitorySynapsesDistance
        if cell_class == CellClass.CLASS_EXC:
            return soma_axon_distance >= self.excitatorySynapsesDistance

    @classmethod
    def load(cls, xml):
        """Extract a bouton distance classification object from XML
        """
        fragment = xml.find("InitialBoutonDistance")
        if hasattr(fragment, "items"):
            infos = {k: v for k, v in fragment.items()}
            return cls(**infos)
        return cls()


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, neurons, morphos):
        self.distances = InitialBoutonDistance.load(recipe.xml)

    def apply(self, circuit):
        """Apply filter
        """
        # Use broadcast of Neuron version
        return circuit.df.where(
            "(distance_soma >= {:f} AND dst_syn_class_i = {:d}) OR "
            "(distance_soma >= {:f} AND dst_syn_class_i = {:d})".format(
                self.distances.inhibitorySynapsesDistance,
                getattr(CellClass.INH, "index", -1),
                self.distances.excitatorySynapsesDistance,
                getattr(CellClass.EXC, "index", -1),
            )
        )


class BoutonDistanceReverseFilter(BoutonDistanceFilter):
    """Reverse version of Bouton Distance filter, only keeping outliers.
    """

    _visible = False

    def apply(self, circuit):
        return circuit.df.where(
            "(distance_soma < {:f} AND dst_syn_class_i = {:d}) OR "
            "(distance_soma < {:f} AND dst_syn_class_i = {:d})".format(
                self.distances.inhibitorySynapsesDistance,
                getattr(CellClass.INH, "index", -1),
                self.distances.excitatorySynapsesDistance,
                getattr(CellClass.EXC, "index", -1),
            )
        )
