"""Module for circuit related classes, functions."""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.io import NodeData, EdgeData
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
        connections_counts = touches.groupBy("pathway_i", "pathway_str", "src", "dst").agg(
            F.count("*").cast(T.IntegerType()).alias("count")
        )
        return connections_counts

    def pathway_statistics(counts):
        """Gather statistics."""
        return (
            counts.groupBy("pathway_i", "pathway_str")
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
    """

    def __init__(
        self,
        source: NodeData,
        target: NodeData,
        touches: EdgeData,
    ):
        """Construct a new circuit."""
        #: :property: the source neuron population
        self.source = source
        #: :property: the target neuron population
        self.target = target

        # The circuit will be constructed (and grouped by src, dst)
        self.__touches = touches
        self.__circuit = self.build_circuit(touches.df)

        self.__input_size = touches.input_size

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
