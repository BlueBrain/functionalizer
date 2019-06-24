"""Module for circuit related classes, functions
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.schema import touches_with_pathway
from spykfunc.utils import get_logger

logger = get_logger(__name__)


def touches_per_pathway(touches):
    """Calculate touch statistics for every pathway (src-dst mtype)

    Args:
        touches: A DataFrame with touch columns
    Returns:
        A dataframe containing, per pathway:
        * number of touches
        * connections (unique src/dst)
        * the mean (touches/connection)
    """
    def pathway_connection_counts(touches):
        """Get connections (src/dst) counts
        """
        connections_counts = (
            touches
            .groupBy("pathway_i", "src", "dst")
            .agg(F.count("*").cast(T.IntegerType()).alias("count"))
        )
        return connections_counts

    def pathway_statistics(counts):
        """Gather statistics
        """
        return (
            counts
            .groupBy("pathway_i").agg(
                F.sum("count").alias("total_touches"),
                F.count("*").alias("total_connections")
            )
            .withColumn(
                "average_touches_conn",
                F.col("total_touches") / F.col("total_connections")
            )
        )

    if 'pathway_i' not in touches.columns:
        touches = touches_with_pathway(touches)
    logger.debug("Computing Pathway stats...")
    return pathway_statistics(pathway_connection_counts(touches))


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

        self._touches = touches
        self._initial_touches = touches

        # The circuit will be constructed (and grouped by src, dst
        self.__circuit = None
        self.__reduced = None

    @property
    def cell_classes(self):
        """:property: cell names used in the circuit
        """
        return self.__neuron_data.cellClasses

    @property
    def electrophysiology_types(self):
        """:property: electrophysiology names used in the circuit
        """
        return self.__neuron_data.eTypes

    @property
    def layers(self):
        """:property: the layers of the current circuit
        """
        return self.__neuron_data.layers

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
