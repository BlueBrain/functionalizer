"""Module for circuit related classes, functions
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.data_loader import NeuronData
from spykfunc.dataio.morphologies import MorphologyDB
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

    def __init__(
        self,
        source: NeuronData,
        target: NeuronData,
        touches,
        recipe,
        morphologies: str
    ):
        """Construct a new circuit

        :param source: the source neuron population
        :param target: the target neuron population
        :param touches: a Spark dataframe with touch data
        :param recipe: a :py:class:`~spykfunc.recipe.Recipe` object
        :param morphologies: the path to morphologies used in this circuit
        """
        self.source = source
        self.target = target

        self.morphologies = MorphologyDB(morphologies)

        self._touches = touches
        self._initial_touches = touches

        # The circuit will be constructed (and grouped by src, dst
        self.__circuit = None
        self.__reduced = None

    @property
    def __src(self):
        tmp = self.source.df
        for col in tmp.schema.names:
            tmp = tmp.withColumnRenamed(col, "src" if col == "id" else f"src_{col}")
        return tmp

    @property
    def __dst(self):
        tmp = self.target.df
        for col in tmp.schema.names:
            tmp = tmp.withColumnRenamed(col, "dst" if col == "id" else f"dst_{col}")
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
                                      .join(F.broadcast(self.__src), "src") \
                                      .join(F.broadcast(self.__dst), "dst")
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
                                      .join(F.broadcast(self.__src), "src") \
                                      .join(F.broadcast(self.__dst), "dst")
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
