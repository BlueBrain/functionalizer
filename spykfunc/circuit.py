"""Module for circuit related classes, functions
"""

import sparksetup


class Circuit(object):
    """Reprensentation of a circuit

    Simple data container to simplify and future-proof the API.
    """

    __neuron_data = None
    """:property: neuron data"""

    _touches = None
    """:property: touches as a spark dataframe"""

    _initial_touches = None
    """:property: the original touch dataframe"""

    synapse_class_matrix = None
    """:property: synapse class matrix"""

    synapse_class_properties = None
    """:property: synapse properties"""

    def __init__(self, neurons, touches, recipe):
        """Construct a new circuit

        :param neurons: a :py:class:`~spykfunc.data_loader.NeuronDataSpark` object
        :param touches: a Spark dataframe with touch data
        :param recipe: a :py:class:`~spykfunc.recipe.Recipe` object
        """
        self.__neuron_data = neurons
        self.synapse_class_matrix = neurons.load_synapse_prop_matrix(recipe)
        self.synapse_class_properties = neurons.load_synapse_properties_and_classification(recipe)

        self._touches = touches
        self._initial_touches = touches

        sparksetup.context.broadcast(self.neurons.collect())

    @property
    def neurons(self):
        """:property: neuron data as a Spark dataframe
        """
        return self.__neuron_data.neuronDF

    @property
    def neuron_count(self):
        """:property: the number of neurons loaded
        """
        return int(self.__neuron_data.nNeurons)

    @property
    def morphologies(self):
        """:property: morphologies as a Spark RDD
        """
        return self.__neuron_data.morphologyRDD

    @property
    def morphology_types(self):
        """:property: types of the morphologies
        """
        return self.__neuron_data.mTypes

    @property
    def touches(self):
        """:property: touch data as a Spark dataframe"""
        return self._touches

    @touches.setter
    def touches(self, new_touches):
        if self._initial_touches is None:
            raise ValueError("Circuit was not properly initialized!")
        self._touches = new_touches
