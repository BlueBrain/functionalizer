"""Module for circuit related classes, functions
"""

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.data_loader import NeuronData, TouchData
from spykfunc.dataio.morphologies import MorphologyDB
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

    logger.debug("Computing Pathway stats...")
    return pathway_statistics(pathway_connection_counts(touches))


class Circuit(object):
    """Representation of a circuit

    Simple data container to simplify and future-proof the API.  Objects of
    this class will hold both nodes and edges of the initial brain
    connectivity.

    Access to both node populations is provided via :attr:`.Circuit.source`
    and :attr:`.Circuit.target`.  Likewise, the current edges can be
    obtained via :attr:`.Circuit.touches`.

    The preferred access to the circuit is through
    :attr:`.Circuit.dataframe`, or, shorter, :attr:`.Circuit.df`.  These
    two object properties provide the synapses of the circuit joined with
    both neuron populations for a full set of properties.  The source and
    target neuron populations attributes are prefixed with `src_` and
    `dst_`, respectively.  The identification of the neurons will be plain
    `src` and `dst`.

    The shorter :attr:`.Circuit.df` should also be used to update the
    connectivity.

    For convenience, the attribute :attr:`.Circuit.reduced` will provide
    the circuits connectivity reduced to only one connection per (`src`,
    `dst`) identifier pair.  Each connection will provide the miniumum of
    the `synapse_id` to be able to generate reproducible random numbers.

    Arguments
    ---------
    source
        the source neuron population
    target
        the target neuron population
    touches
        the synaptic connections
    morphologies
        the path to the morphology data of the neurons
    """

    def __init__(
        self,
        source: NeuronData,
        target: NeuronData,
        touches: TouchData,
        morphologies: str
    ):
        """Construct a new circuit
        """
        #: :property: the source neuron population
        self.source = source
        #: :property: the target neuron population
        self.target = target

        #: :property: a wrapper around the morphology storage
        self.morphologies = MorphologyDB(morphologies)

        self._touches = None
        self._touch_loader = touches

        # The circuit will be constructed (and grouped by src, dst
        self.__circuit = None
        self.__reduced = None

        self.__pathway_columns = None
        self.__pathway_offsets = []

    def with_pathway(self, df=None):
        if df is None:
            df = self.df

        def to_pathway_i(col1, col2):
            """
            Expression to calculate the pathway index based on the morphologies' index
            :param col1: The dataframe src morphology index field
            :param col2: The dataframe dst morphology index field
            """
            return (F.shiftLeft(col1, 16) + F.col(col2)).cast(T.IntegerType()).alias("pathway_i")

        return df.withColumn(
            "pathway_i",
            to_pathway_i("src_mtype_i", "dst_mtype_i")
        )

    @staticmethod
    def pathway_to_str(df_pathway_i, src_mtypes, dst_mtypes):
        """
        Expression to calculate the pathway name from the pathway index.

        Args:
            df_pathway_i: Pathway index column
            src_mtypes: mtypes dataframe, with two fields: (index: int, name: str)
            dst_mtypes: mtypes dataframe, with two fields: (index: int, name: str)
        """
        col = "pathway_i"
        return (
            df_pathway_i
            .withColumn("src_morpho_i", F.shiftRight(col, 16))
            .withColumn("dst_morpho_i", F.col(col).bitwiseAND((1 << 16)-1))
            .join(src_mtypes.toDF("src_morpho_i", "src_morpho"), "src_morpho_i")
            .join(dst_mtypes.toDF("dst_morpho_i", "dst_morpho"), "dst_morpho_i")
            .withColumn("pathway_str", F.concat("src_morpho", F.lit('->'), "dst_morpho"))
            .drop("src_morpho_i", "src_morpho", "dst_morpho_i", "dst_morpho")
        )

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
        """:property: shortcut for :prop:`dataframe`.
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

        self.__circuit = self.touches.alias("t") \
                                     .join(F.broadcast(self.__src), "src") \
                                     .join(F.broadcast(self.__dst), "dst")
        return self.__circuit

    @property
    def reduced(self):
        """:property: a reduced circuit with only one connection per src, dst
        """
        if self.__reduced:
            return self.__reduced

        self.__reduced = self.touches.alias("t") \
                                     .groupBy("src", "dst") \
                                     .agg(F.min("synapse_id").alias("synapse_id")) \
                                     .join(F.broadcast(self.__src), "src") \
                                     .join(F.broadcast(self.__dst), "dst")
        return self.__reduced

    @dataframe.setter
    def dataframe(self, dataframe):
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
        if not self._touches:
            self._touches = self._touch_loader.df
        return self._touches

    @staticmethod
    def only_touch_columns(df):
        """Remove neuron columns from a dataframe

        :param df: a dataframe to trim
        """
        def belongs_to_neuron(col):
            return col.startswith("src_") or col.startswith("dst_")
        return df.select([col for col in df.schema.names if not belongs_to_neuron(col)])
