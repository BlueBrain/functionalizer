"""A default filter plugin."""

from functionalizer.filters import DatasetOperation


class BoutonDistanceFilter(DatasetOperation):
    """Filter synapses based on the distance from the soma.

    This filter reads distances for inhibitory and excitatory synapses from
    the recipe definition and filters out all synapses closer to the soma.
    """

    def __init__(self, recipe, source, target):
        """Initializes the filter, extracting the bouton distance data from the recipe."""
        super().__init__(recipe, source, target)
        self._exc_distance = recipe.get("bouton_distances.excitatory_synapse_distance")
        self._inh_distance = recipe.get("bouton_distances.inhibitory_synapse_distance")

    def apply(self, circuit):
        """Apply filter."""

        def pos(cls):
            """Save index function returning -1 if not found."""
            try:
                return circuit.target.synapse_class_values.index(cls)
            except ValueError:
                return -1

        # Use broadcast of Neuron version
        return circuit.df.where(
            f"(distance_soma >= {self._inh_distance:f} AND"
            f" dst_synapse_class_i = {pos('INH'):d})"
            " OR "
            f"(distance_soma >= {self._exc_distance:f} AND"
            f" dst_synapse_class_i = {pos('EXC'):d})"
        )
