"""Module for circuit related classes, functions
"""

from pyspark.sql import functions as F


class Circuit(object):
    """Reprensentation of a circuit

    Simple data container to simplify and future-proof the API.
    """

    def __init__(self, neurons, touches, recipe):
        """Construct a new circuit

        :param neurons: a :py:class:`~spykfunc.data_loader.NeuronDataSpark` object
        :param touches: a Spark dataframe with touch data
        :param recipe: a :py:class:`~spykfunc.recipe.Recipe` object
        """
        self.__neuron_data = neurons
        self.__synapse_reposition_pathways = neurons.load_synapse_reposition_pathways(recipe)
        self.__synapse_class_matrix = neurons.load_synapse_prop_matrix(recipe)
        self.__synapse_class_properties = neurons.load_synapse_properties_and_classification(recipe)
        self.__touch_rules_matrix = neurons.load_touch_rules_matrix(recipe)

        self._touches = touches
        self._initial_touches = touches

        # The circuit will be constructed (and grouped by src, dst
        self.__circuit = None
        self.__reduced = None

    @property
    def touch_rules(self):
        """:property: rules for touches
        """
        return self.__touch_rules_matrix

    @property
    def synapse_class_matrix(self):
        """:property: synapse class matrix
        """
        return self.__synapse_class_matrix

    @property
    def synapse_class_properties(self):
        """:property: synapse class properties
        """
        return self.__synapse_class_properties

    @property
    def synapse_reposition_pathways(self):
        """:property: synapse pathways to be repositioned
        """
        return self.__synapse_reposition_pathways

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
        """:property: morphology DB
        """
        return self.__neuron_data.morphologyDB

    @property
    def morphology_types(self):
        """:property: morphology names used in the circuit
        """
        return self.__neuron_data.mTypes

    def prefixed(self, pre):
        """Returns the neurons with all columns prefixed

        :param pre: the prefix to use
        :type pre: string
        """
        tmp = self.neurons
        for col in tmp.schema.names:
            tmp = tmp.withColumnRenamed(col, pre if col == "id" else "{}_{}".format(pre, col))
        return tmp

    @property
    def df(self):
        """:property" shortcut for `dataframe`.
        """
        return self.dataframe

    @df.setter
    def df(self, df):
        self.dataframe = df

    @property
    def dataframe(self):
        """:property: return a dataframe representing the circuit
        """
        if self.__circuit:
            return self.__circuit

        self.__circuit = self._touches.alias("t") \
                                      .join(F.broadcast(self.prefixed("src")), "src") \
                                      .join(F.broadcast(self.prefixed("dst")), "dst")
        return self.__circuit

    @property
    def reduced(self):
        """:property: a reduced circuit with only one connection per src, dst
        """
        if self.__reduced:
            return self.__reduced
        self.__reduced = self._touches.alias("t") \
                                      .groupBy("src", "dst") \
                                      .agg(F.min("synapse_id").alias("synapse_id")) \
                                      .join(F.broadcast(self.prefixed("src")), "src") \
                                      .join(F.broadcast(self.prefixed("dst")), "dst")
        return self.__reduced

    @dataframe.setter
    def dataframe(self, dataframe):
        if self._initial_touches is None:
            raise ValueError("Circuit was not properly initialized!")
        self._touches = self.only_touch_columns(dataframe)
        if any(n.startswith('src_') or n.startswith('dst_') for n in dataframe.schema.names):
            self.__circuit = dataframe
            self.__reduced = None
        else:
            self.__circuit = None
            self.__reduced = None

    @property
    def touches(self):
        """:property: touch data as a Spark dataframe
        """
        return self._touches

    @staticmethod
    def only_touch_columns(df):
        """Remove neuron columns from a dataframe

        :param df: a dataframe to trim
        """
        def belongs_to_neuron(col):
            return col.startswith("src_") or col.startswith("dst_")
        return df.select([col for col in df.schema.names if not belongs_to_neuron(col)])

    @property
    def data(self):
        """Direct access to the base data object
        """
        return self.__neuron_data

    def __getattr__(self, item):
        """Direct access to some interesting data attributes, namely existing dataframes
        """
        if item in ("sclass_df", "mtype_df", "etype_df"):
            return getattr(self.__neuron_data, item)
        raise AttributeError("Attribute {} not accessible.".format(item))
