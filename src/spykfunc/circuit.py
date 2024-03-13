"""Module for circuit related classes, functions."""

import contextlib
from typing import Iterable

from pyspark.sql import functions as F
from pyspark.sql import types as T

import pandas as pd

from spykfunc.io import NodeData, EdgeData, MorphologyDB
from spykfunc.utils import get_logger

logger = get_logger(__name__)


def touches_per_pathway(touches):
    """Calculate touch statistics for every pathway (src-dst mtype).

    Args:
        touches: A DataFrame with touch columns
    Returns:
        A dataframe containing, per pathway:
        * number of touches
        * connections (unique src/dst)
        * the mean (touches/connection)
    """

    def pathway_connection_counts(touches):
        """Get connections (src/dst) counts."""
        connections_counts = touches.groupBy("pathway_i", "src", "dst").agg(
            F.count("*").cast(T.IntegerType()).alias("count")
        )
        return connections_counts

    def pathway_statistics(counts):
        """Gather statistics."""
        return (
            counts.groupBy("pathway_i")
            .agg(
                F.sum("count").alias("total_touches"),
                F.count("*").alias("total_connections"),
            )
            .withColumn("structural_mean", F.col("total_touches") / F.col("total_connections"))
        )

    return pathway_statistics(pathway_connection_counts(touches))


class Circuit:
    """Representation of a circuit.

    Simple data container to simplify and future-proof the API.  Objects of
    this class will hold both nodes and edges of the initial brain
    connectivity.

    Access to both node populations is provided via :attr:`.Circuit.source`
    and :attr:`.Circuit.target`.  Likewise, the current edges can be
    obtained via :attr:`.Circuit.touches`.

    The preferred access to the circuit is through :attr:`.Circuit.df`.
    This object property provides the synapses of the circuit joined with
    both neuron populations for a full set of properties.  The source and
    target neuron populations attributes are prefixed with `src_` and
    `dst_`, respectively.  The identification of the neurons will be plain
    `src` and `dst`.

    The :attr:`.Circuit.df` property should also be used to update the
    connectivity.

    Args:
        source: the source neuron population
        target: the target neuron population
        touches: the synaptic connections
        morphologies: a iterable containing the storage for node morphologies, and,
            optionally, for spine morphologies
    """

    __pathways_defined = False

    def __init__(
        self,
        source: NodeData,
        target: NodeData,
        touches: EdgeData,
        morphologies: Iterable[str],
    ):
        """Construct a new circuit."""
        #: :property: the source neuron population
        self.source = source
        #: :property: the target neuron population
        self.target = target

        #: :property: a wrapper around the morphology storage
        self.morphologies = None
        if morphologies:
            self.morphologies = MorphologyDB(*morphologies)

        # The circuit will be constructed (and grouped by src, dst)
        self.__touches = touches
        self.__circuit = self.build_circuit(touches.df)

        self.__input_size = touches.input_size

    def with_pathway(self, df=None):
        """Stub to add a pathway column to a PySpark dataframe."""
        raise RuntimeError("with_patway can only be used in a pathway context")

    @staticmethod
    def pathway_to_str(df_pathway_i):
        """Stub to convert a pathway index to its string representation."""
        raise RuntimeError("with_patway can only be used in a pathway context")

    @staticmethod
    def _internal_mapping(col, source, target):
        """Transform a name from recipe notation to Spykfunc's internal one.

        Returns the property in lower case, the internal naming, as well as the
        corresponding node population.
        """
        if col.startswith("to"):
            stem = col.lower()[2:]
            name = f"dst_{stem}"
            if hasattr(target, f"{stem}_values"):
                return stem, name, target
        elif col.startswith("from"):
            stem = col.lower()[4:]
            name = f"src_{stem}"
            if hasattr(source, f"{stem}_values"):
                return stem, name, source
        return None, None

    @staticmethod
    def expand(columns, source, target):
        """Expand recipe-convention `columns` to names and data from dataframes.

        For each column name in `columns`, given in the convention of the recipe, returns a tuple
        with:

        * the recipe names
        * the appropriate `source` or `target` name
        * the appropriate `source` or `target` name containing indices to library values
        * the library values to be used with the indexed column
        """
        for col in columns:
            stem, name, nodes = Circuit._internal_mapping(col, source, target)
            if stem and name:
                yield col, name, f"{name}_i", getattr(nodes, f"{stem}_values")
            else:
                raise RuntimeError(f"cannot determine node column from '{col}'")

    @contextlib.contextmanager
    def pathways(self, columns):
        """Context manager to set up pathway related facilities.

        Will change `with_pathway` and `pathway_to_str` to implementations that add a
        ``pathway`` column based on the passed `columns`, and translate said column back
        into a string.
        """
        if not columns:
            columns = ["fromMType", "toMType"]

        if Circuit.__pathways_defined:
            raise RuntimeError("cannot define pathway columns multiple times")

        names = []
        numerics = []
        values = []
        for _, name, numeric, vals in self.expand(columns, self.source, self.target):
            names.append(name)
            numerics.append(numeric)
            values.append(vals)

        def _w_pathway(self, df=None):
            if df is None:
                df = self.df
            factor = 1
            result = F.lit(0)
            for numeric, vals in zip(numerics, values):
                result += F.lit(factor) * F.col(numeric)
                factor *= len(vals)
            return df.withColumn("pathway_i", result)

        def _w_pathway_str(df):
            col = F.col("pathway_i")
            mapping = {}
            to_drop = []
            for numeric, vals in zip(numerics, values):
                df = df.withColumn(numeric, col % len(vals))
                to_drop.append(numeric)
                col = (col / len(vals)).cast("long")
                mapping[numeric] = pd.Series(vals)

            def _mapper(dfs):
                for df in dfs:
                    cols = [
                        df[numeric].map(vals).map(f"{numeric}={{}}".format)
                        for numeric, vals in mapping.items()
                    ]

                    df["pathway_str"] = cols[0].str.cat(others=cols[1:], sep=" ")
                    yield df

            df = df.withColumn("pathway_str", F.lit(""))
            return df.mapInPandas(_mapper, df.schema).drop(*list(mapping.keys()))

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
    def input_size(self):
        """:property: the original input size in bytes."""
        return self.__input_size

    @property
    def metadata(self):
        """:property: metadata associated with the connections."""
        return self.__touches.metadata

    def build_circuit(self, touches):
        """Joins `touches` with the node tables."""
        if self.source and self.target:
            return touches.alias("t").join(self.__src, "src").join(self.__dst, "dst").cache()
        return touches.alias("t")

    @property
    def df(self):
        """:property: return a dataframe representing the circuit."""
        return self.__circuit

    @df.setter
    def df(self, dataframe):
        if any(n.startswith("src_") or n.startswith("dst_") for n in dataframe.schema.names):
            self.__circuit = dataframe
        else:
            self.__circuit = self.build_circuit(dataframe)

    @property
    def touches(self):
        """:property: The touches originally used to construct the circuit."""
        return self.__touches

    def __len__(self):
        """The number of touches currently present in the circuit."""
        return self.__circuit.count()

    @staticmethod
    def only_touch_columns(df):
        """Remove neuron columns from a dataframe.

        :param df: a dataframe to trim
        """

        def belongs_to_neuron(col):
            return col.startswith("src_") or col.startswith("dst_")

        return df.select([col for col in df.schema.names if not belongs_to_neuron(col)])
