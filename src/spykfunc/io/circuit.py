"""Data loading for nodes, edges."""
import glob
import hashlib
import math
import os
import re
from typing import Iterable, List

from packaging.version import Version
import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

import sparkmanager as sm

from spykfunc import schema
from spykfunc.utils import get_logger
from spykfunc.utils.filesystem import adjust_for_spark


BASIC_EDGE_SCHEMA = ["source_node_id long", "target_node_id long", "synapse_id long"]
BASIC_NODE_SCHEMA = ["id long"]
VERSION_SCHEMA = re.compile(r"\d+(\.\d+)+(-\d+)?")

# Globals
logger = get_logger(__name__)

# Widen unsigned data types to prevent potential dataloss during
# conversions.
#
# FIXME we blatantly assume that objects are strings
_numpy_to_spark = {
    "int8": "byte",
    "int16": "short",
    "int32": "int",
    "int64": "long",
    "uint8": "short",
    "uint16": "int",
    "uint32": "long",
    "uint64": "long",
    "float32": "float",
    "float64": "double",
    "object": "string",
}

PARTITION_SIZE = 500_000
# Internal calculations rely on branch types being 0-based. Input should
# follow the SONATA conversion, inherited from MorphIO, where values are
# 1-based. Thus this offset...
BRANCH_OFFSET: int = 1
BRANCH_COLUMNS: List[str] = ["afferent_section_type", "efferent_section_type"]
BRANCH_SHIFT_MINIMUM_VERSION: Version = Version("0.6.1-2")


def shift_branch_type(df: DataFrame, shift: int = BRANCH_OFFSET) -> DataFrame:
    """Shift branch/section types from 1-based to 0-based."""
    for attr in BRANCH_COLUMNS:
        tmp_attr = f"__tmp__{attr}"
        if hasattr(df, attr):
            df = (
                df.withColumnRenamed(attr, tmp_attr)
                .withColumn(attr, F.col(tmp_attr) + F.lit(shift))
                .drop(tmp_attr)
            )
    return df


def _accept_node_enumeration(attr: str) -> bool:
    """Select which enumerations to use, will fall back to attributes otherwise."""
    if attr == "morphology":
        return False
    if attr == "morph_class":
        return False
    return True


def _accept_node_attribute(attr: str) -> bool:
    """Accepts all node attributes except for position, rotation ones."""
    if attr.startswith("rotation_"):
        return False
    if attr.startswith("orientation_"):
        return False
    if attr in ("x", "y", "z"):
        return False
    return True


def _translate_attribute(attr: str) -> str:
    """Map attribute names.

    Translates certain attributes from the SONATA convention to the nomenclature used in
    the recipe to account for any mismatch between the two.
    """
    if attr == "synapse_class":
        return "sclass"
    return attr


def _get_enumerations(population):
    """Returns the enumerations for a population."""
    return sorted(filter(_accept_node_enumeration, population.enumeration_names))


def _get_pure_attributes(population):
    """Returns the attributes for a population that are not enumerations."""
    return sorted(
        filter(
            _accept_node_attribute,
            population.attribute_names - set(_get_enumerations(population)),
        )
    )


def _column_type(population, column, accessor):
    """Helper to determine column types.

    Determine the Spark datatype for `column` from `population` by getting data for an
    empty selection via the `accessor` method of `population`.
    """
    import libsonata

    data = getattr(population, accessor)(column, libsonata.Selection([]))
    return _numpy_to_spark[data.dtype.name]


def _add_all_attributes(dataframe, population, selection):
    """Helper to add SONATA attributes to Spark dataframes.

    Adds all enumeration and pure attributes for a given `selection` of a `population` to
    the `dataframe` passed in.
    """
    for column in _get_pure_attributes(population):
        name = _translate_attribute(column)
        dataframe[name] = population.get_attribute(column, selection)
    for column in _get_enumerations(population):
        name = _translate_attribute(column)
        dataframe[f"{name}_i"] = population.get_enumeration(column, selection)
    return dataframe


def _types(population):
    """Helper to create Spark type conventions.

    Generates a sequence of schema strings for Spark corresponding to the attributes and
    enumerations of `population`.
    """
    for column in _get_pure_attributes(population):
        kind = _column_type(population, column, "get_attribute")
        name = _translate_attribute(column)
        yield f"{name} {kind}"
    for column in _get_enumerations(population):
        kind = _column_type(population, column, "get_enumeration")
        name = _translate_attribute(column)
        yield f"{name}_i {kind}"


def _create_neuron_loader(filename, population):
    """Create a UDF to load neurons from SONATA.

    Args:
        filename: The name of the circuit file
        population: The population to load
    Returns:
        A Pandas UDF to be used over a group by
    """

    def loader(data: pd.DataFrame) -> pd.DataFrame:
        assert len(data) == 1
        import libsonata

        nodes = libsonata.NodeStorage(filename)
        pop = nodes.open_population(population)

        (_, ids) = data.iloc[0]
        selection = libsonata.Selection(ids)
        data = _add_all_attributes(dict(id=ids), pop, selection)
        return pd.DataFrame(data)

    return loader


def _create_touch_loader(filename: str, population: str):
    """Create a UDF to load touches from SONATA.

    Args:
        filename: The name of the touches file
        population: The population to load
    Returns:
        A Pandas UDF to be used over a group by
    """

    def loader(data: pd.DataFrame) -> pd.DataFrame:
        assert len(data) == 1
        start, end = data.iloc[0]

        import libsonata

        pop = libsonata.EdgeStorage(filename).open_population(population)
        selection = libsonata.Selection([(start, end)])
        data = dict(
            source_node_id=pop.source_nodes(selection),
            target_node_id=pop.target_nodes(selection),
            synapse_id=selection.flatten(),
        )
        data = _add_all_attributes(data, pop, selection)
        return pd.DataFrame(data)

    return loader


class NodeData:
    """Neuron data loading facilities.

    This class represent neuron populations, lazily loaded.  After the
    construction, general properties of the neurons, such as the unique
    values of the
    :attr:`.NodeData.mtype_values`,
    :attr:`.NodeData.etype_values`, or
    :attr:`.NodeData.sclass_values`
    present can be accessed.
    """

    def __init__(self, population: Iterable[str], nodeset: Iterable[str], cache: str):
        """Construct a new neuron loader.

        To load neuron-specific information, access the property
        :attr:`.NodeData.df`, data will be loaded lazily.

        Args:
            population: a tuple with the path to the neuron storage and population name
            nodeset: a tuple with the path to the nodesets JSON file and nodeset name
            cache: a directory name to use for caching generated Parquet
        """
        self._cache = cache
        self._df = None
        (self._filename, self._population) = population
        (self._ns_filename, self._ns_nodeset) = nodeset

        import libsonata

        pop = libsonata.NodeStorage(self._filename).open_population(self._population)
        self._size = len(pop)
        self._columns = ", ".join(BASIC_NODE_SCHEMA + list(_types(pop)))
        self._enumerations = {
            f"{_translate_attribute(attr)}_values": pop.enumeration_values(attr)
            for attr in _get_enumerations(pop)
        }

        if not os.path.isdir(self._cache):
            os.makedirs(self._cache)

    def __getattr__(self, attr):
        """Attribute access is defaulted to SONATA enumerations."""
        return self._enumerations[attr]

    def __len__(self):
        """The number of nodes in the cell dataframe."""
        return self._size

    @property
    def population(self):
        """The population name."""
        return self._population

    @property
    def df(self):
        """The PySpark dataframe with the neuron data."""
        if not self._df:
            self._df = F.broadcast(self._load_neurons())
        return self._df

    def _load_neurons(self):
        fn = self._filename
        sha = hashlib.sha256()
        sha.update(os.path.realpath(fn).encode())
        sha.update(self._population.encode())
        sha.update(str(os.stat(fn).st_size).encode())
        sha.update(str(os.stat(fn).st_mtime).encode())
        if self._ns_filename and self._ns_nodeset:
            sha.update(self._ns_filename.encode())
            sha.update(self._ns_nodeset.encode())
        digest = sha.hexdigest()[:8]

        logger.info("Total neurons: %d", len(self))
        mvd_parquet = os.path.join(
            self._cache, f"neurons_{len(self) / 1000.0:.1f}k_{digest}.parquet"
        )

        if os.path.exists(mvd_parquet):
            logger.info("Loading circuit from parquet")
            mvd = sm.read.parquet(adjust_for_spark(mvd_parquet, local=True))
            df = F.broadcast(mvd.coalesce(1)).cache()
            df.count()  # force materialize
        else:
            logger.info("Building nodes from SONATA")

            # Create a default selection, or load it from the NodeSets
            if not self._ns_filename:
                ids = list(range(0, len(self)))
            else:
                import libsonata

                nodesets = libsonata.NodeSets.from_file(self._ns_filename)
                population = libsonata.NodeStorage(self._filename).open_population(self._population)
                selection = nodesets.materialize(self._ns_nodeset, population)
                ids = selection.flatten().tolist()

            total_parts = math.ceil(len(ids) / PARTITION_SIZE)
            logger.debug("Partitions: %d", total_parts)

            def generate_ids():
                for n in range(total_parts):
                    start = PARTITION_SIZE * n
                    end = min(PARTITION_SIZE * (n + 1), len(ids))
                    yield n, ids[start:end]

            parts = sm.createDataFrame(generate_ids(), ["row", "ids"])

            # Create DF
            logger.info("Creating neuron data frame...")
            raw_mvd = parts.groupby("row").applyInPandas(
                _create_neuron_loader(self._filename, self._population), self._columns
            )

            # Evaluate (build partial NameMaps) and store
            mvd = raw_mvd.write.mode("overwrite").parquet(adjust_for_spark(mvd_parquet, local=True))
            raw_mvd.unpersist()

            # Mark as "broadcastable" and cache
            df = F.broadcast(sm.read.parquet(adjust_for_spark(mvd_parquet))).cache()
        return df


def _get_size(files):
    """Returns the total size of a list of filenames or directories."""
    size = 0

    def _add_size(fn):
        nonlocal size
        if fn.endswith(".parquet") or fn.endswith(".h5"):
            size += os.path.getsize(fn)

    for path in files:
        if os.path.isfile(path):
            _add_size(path)
        else:
            for root, _, filenames in os.walk(path):
                for fn in filenames:
                    _add_size(os.path.join(root, fn))
    return size


def _grab_parquet(files):
    """Returns as many parquet files from the front of `files` as possible."""
    parquets = []
    while files and files[0].endswith(".parquet"):
        if os.path.isdir(files[0]):
            if parquets:
                return parquets
            return [files.pop(0)]
        parquets.append(files.pop(0))
    return parquets


def _grab_sonata_population(filename):
    """Retrieve the default population in a SONATA files.

    Raise an exception if no population present or more than one population is found.
    """
    import libsonata

    populations = libsonata.EdgeStorage(filename).population_names
    if len(populations) == 1:
        return next(iter(populations))
    if len(populations) > 1:
        raise ValueError(f"More than one population in '{filename}'")
    raise ValueError(f"No population in '{filename}'")


def _grab_sonata(files):
    """Returns a possible SONATA file from the front of `files`."""
    if not files:
        return None
    if not files[0].endswith(".h5"):
        return None
    filename = files.pop(0)
    if files and not any(files[0].endswith(ext) for ext in (".h5", ".parquet")):
        population = files.pop(0)
    else:
        population = _grab_sonata_population(filename)
    return (filename, population)


class EdgeData:
    """Edge data loading facilities.

    This class represent the connectivity between cell populations, lazily
    loaded.  Access the property :attr:`.EdgeData.df`, to load the data.
    """

    def __init__(self, *paths):
        """Initialize the loader.

        Args:
            A list of edge files.
        """
        files = []
        for path in paths:
            files.extend(glob.glob(path) or [path])
        metadata = []
        self._size = _get_size(files)
        self._loaders = []
        while files:
            if parquet := _grab_parquet(files):
                local_metadata = self._load_parquet_metadata(*parquet)
                metadata.append(local_metadata)
                self._loaders.append(self._load_parquet(local_metadata, *parquet))
            elif sonata := _grab_sonata(files):
                metadata.append(self._load_sonata_metadata(*sonata))
                self._loaders.append(self._load_sonata(*sonata))
            else:
                raise ValueError(f"cannot process file(s) {files[0]}")
        if len(set(frozenset(m.items()) for m in metadata)) == 1:
            self._metadata = metadata[0]
        elif metadata:
            logger.debug("Detected multiple different inputs, prefixing metadata")
            self._metadata = {}
            for key in schema.METADATA_FIXED_KEYS:
                for m in metadata:
                    if key not in m:
                        continue
                    value = m.pop(key)
                    if self._metadata.setdefault(key, value) != value:
                        raise ValueError(
                            "conflicting values for metadata "
                            f"{key}: {self._metadata[key]}, {value}"
                        )
            for n, m in enumerate(metadata):
                self._metadata.update({f"merge{n}_{k}": v for k, v in m.items()})
        else:
            raise ValueError("need to provide at least one file with edges")

    @property
    def df(self):
        """The PySpark dataframe with the edge data."""
        df = self._loaders[0]()
        for loader in self._loaders[1:]:
            df = df.union(loader())
        return df.withColumnRenamed("source_node_id", "src").withColumnRenamed(
            "target_node_id", "dst"
        )

    @property
    def input_size(self):
        """The initial size (in bytes) of the input data."""
        return self._size

    @property
    def metadata(self):
        """The metadata associated with the input data."""
        return self._metadata

    @staticmethod
    def _load_sonata_metadata(filename, population):
        # Could be (and save us the dependency on h5py):
        # import libsonata
        # p = libsonata.EdgeStorage(filename).open_population(population)
        # return {n: p.get_metadata(n) for n in p.metadata_names}
        import h5py

        with h5py.File(filename) as f:
            return dict(f[f"/edges/{population}"].attrs)

    @staticmethod
    def _load_sonata(filename, population):
        def _loader():
            import libsonata

            p = libsonata.EdgeStorage(filename).open_population(population)

            total_parts = p.size // PARTITION_SIZE + 1
            logger.debug("Partitions: %d", total_parts)

            parts = sm.createDataFrame(
                (
                    (PARTITION_SIZE * n, min(PARTITION_SIZE * (n + 1), p.size))
                    for n in range(total_parts)
                ),
                "start: long, end: long",
            )
            columns = ", ".join(BASIC_EDGE_SCHEMA + list(_types(p)))

            logger.info("Creating edge data frame...")
            edges = (
                parts.groupby("start", "end")
                .applyInPandas(_create_touch_loader(filename, population), columns)
                .cache()
            )
            values = set()
            for col in BRANCH_COLUMNS:
                for v in edges.select(col).distinct().collect():
                    values.add(v)
            if 0 in values and 5 in values:
                raise RuntimeError("Cannot determine section type convention")
            if 5 in values:
                return shift_branch_type(edges, -BRANCH_OFFSET)
            return edges

        return _loader

    @staticmethod
    def _load_parquet_metadata(*args):
        if len(args) == 1:
            args = args[0]
        else:
            args = list(args)
        # This breaks the added metadata, convert to arrow to preserve data
        # meta = pq.ParquetDataset(args, use_legacy_dataset=False).schema.metadata
        meta = pq.ParquetDataset(args).schema.to_arrow_schema().metadata
        return {
            k.decode(): v.decode()
            for (k, v) in (meta or {}).items()
            if not k.startswith(b"org.apache.spark")
        }

    @staticmethod
    def _load_parquet(metadata, *args):
        raw_version = metadata.get("touch2parquet_version", "0.0.0")
        if m := VERSION_SCHEMA.search(raw_version):
            t2p_version = Version(m.group(0))
        else:
            raise RuntimeError(f"Can't determine touch2parquet version from {raw_version}")
        shift = t2p_version >= BRANCH_SHIFT_MINIMUM_VERSION

        def _loader():
            files = [adjust_for_spark(f) for f in args]
            edges = sm.read.parquet(*files)
            for old, new in schema.LEGACY_MAPPING.items():
                if old in edges.columns:
                    edges = edges.withColumnRenamed(old, new)
            if shift:
                return shift_branch_type(edges, -BRANCH_OFFSET)
            return edges

        return _loader
