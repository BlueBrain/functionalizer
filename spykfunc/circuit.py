"""Module for circuit related classes, functions
"""
import contextlib

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql import types as T

import sparkmanager as sm

from spykfunc.data_loader import NeuronData, EdgeData
from spykfunc.dataio.morphologies import MorphologyDB
from spykfunc.utils import get_logger
from spykfunc.utils.spark import cache_broadcast_single_part
from spykfunc import schema

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
                "structural_mean",
                F.col("total_touches") / F.col("total_connections")
            )
        )

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

    __pathways_defined = False

    def __init__(
        self,
        source: NeuronData,
        target: NeuronData,
        touches: EdgeData,
        morphologies: str
    ):
        """Construct a new circuit
        """
        #: :property: the source neuron population
        self.source = source
        #: :property: the target neuron population
        self.target = target

        #: :property: a wrapper around the morphology storage
        self.morphologies = MorphologyDB(morphologies) if morphologies else None

        # The circuit will be constructed (and grouped by src, dst
        self.__circuit = None
        self.__reduced = None

        self.__touches = touches.df
        self.__length = self.__touches.count()
        logger.info(f"Touch count after reading: {self.__length:,d}")
        self.__metadata = touches.metadata

    def with_pathway(self, df=None):
        raise RuntimeError("with_patway can only be used in a pathway context")

    @staticmethod
    def pathway_to_str(df_pathway_i, src_mtypes, dst_mtypes):
        raise RuntimeError("with_patway can only be used in a pathway context")

    @staticmethod
    def expand(columns, source, target):
        for col in columns:
            if col.startswith("to"):
                stem = col.lower()[2:]
                yield col, f"dst_{stem}", f"dst_{stem}_i", getattr(target, f"{stem}_values")
            elif col.startswith("from"):
                stem = col.lower()[4:]
                yield col, f"src_{stem}", f"src_{stem}_i", getattr(source, f"{stem}_values")
            else:
                raise RuntimeError(f"cannot determine node column from '{col}'")

    @contextlib.contextmanager
    def pathways(self, columns):
        if not columns:
            columns = ["fromMType", "toMType"]

        if Circuit.__pathways_defined:
            raise RuntimeError("cannot define pathway columns multiple times")

        sizes = []
        names = []
        numerics = []
        tables = []
        for _, name, numeric, vals in self.expand(columns, self.source, self.target):
            names.append(name)
            numerics.append(numeric)
            sizes.append(len(vals))
            tables.append(
                cache_broadcast_single_part(
                    sm.createDataFrame(enumerate(vals), schema.indexed_strings([numeric, name]))
                )
            )

        def _w_pathway(self, df=None):
            if df is None:
                df = self.df
            factor = 1
            result = F.lit(0)
            for numeric, size in zip(numerics, sizes):
                result += F.lit(factor) * F.col(numeric)
                factor *= size
            return df.withColumn("pathway_i", result)

        def _w_pathway_str(df):
            col = F.col("pathway_i")
            strcol = None
            factor = 1
            result = F.lit(0)
            to_drop = []
            for name, numeric, table, size in zip(names, numerics, tables, sizes):
                df = (
                    df
                    .withColumn(numeric, col % size)
                    .join(table, numeric)
                    .drop(numeric)
                )
                col = (col / size).cast("long")
                if strcol is not None:
                    strcol = F.concat(strcol, F.lit(f" {name}="), name)
                else:
                    strcol = F.concat(F.lit(f"{name}="), name)
                to_drop.append(name)
            return (
                df
                .withColumn("pathway_str", strcol)
                .drop(*to_drop)
            )

        old_w_pathway = Circuit.with_pathway
        old_w_pathway_str = Circuit.pathway_to_str
        Circuit.with_pathway = _w_pathway
        Circuit.pathway_to_str = staticmethod(_w_pathway_str)
        Circuit.__pathways_defined = True
        try:
            yield
        finally:
            Circuit.with_pathway = old_w_pathway
            Circuit.pathway_to_str = old_w_pathway_str
            Circuit.__pathways_defined = False

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
        """:property: shortcut for :attr:`dataframe`.
        """
        return self.dataframe

    @df.setter
    def df(self, df):
        self.dataframe = df

    @property
    def metadata(self):
        """:property: metadata associated with the connections
        """
        return self.__metadata

    @property
    def dataframe(self):
        """:property: return a dataframe representing the circuit
        """
        if self.__circuit:
            return self.__circuit

        self.__circuit = self.touches.alias("t") \
                                     .join(self.__src, "src") \
                                     .join(self.__dst, "dst")
        return self.__circuit

    @dataframe.setter
    def dataframe(self, dataframe):
        self.__length = dataframe.count()
        self.__touches = self.only_touch_columns(dataframe)
        if any(n.startswith('src_') or n.startswith('dst_') for n in dataframe.schema.names):
            self.__circuit = dataframe
            self.__reduced = None
        else:
            self.__circuit = None
            self.__reduced = None

    @property
    def reduced(self):
        """:property: a reduced circuit with only one connection per src, dst
        """
        if self.__reduced:
            return self.__reduced

        self.__reduced = self.touches.alias("t") \
                                     .groupBy("src", "dst") \
                                     .agg(F.min("synapse_id").alias("synapse_id")) \
                                     .join(self.__src, "src") \
                                     .join(self.__dst, "dst")
        return self.__reduced

    @property
    def touches(self):
        """:property: touch data as a Spark dataframe
        """
        return self.__touches

    def __len__(self):
        return self.__length

    @staticmethod
    def only_touch_columns(df):
        """Remove neuron columns from a dataframe

        :param df: a dataframe to trim
        """
        def belongs_to_neuron(col):
            return col.startswith("src_") or col.startswith("dst_")
        return df.select([col for col in df.schema.names if not belongs_to_neuron(col)])
