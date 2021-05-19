from __future__ import absolute_import

import glob
import hashlib
import logging
import math
import os
import pandas as pd
import pyarrow.parquet as pq
import sparkmanager as sm

from pyspark.sql import functions as F

from . import schema
from .definitions import MType
from .utils import get_logger, make_slices, to_native_str
from .utils.spark import cache_broadcast_single_part
from .utils.filesystem import adjust_for_spark


BASIC_EDGE_SCHEMA = ["source_node_id long", "target_node_id long", "synapse_id long"]


# Globals
logger = get_logger(__name__)

_numpy_to_spark = {
    "int16": "short",
    "int32": "int",
    "int64": "long",
    "float32": "float",
    "float64": "double",
}

PARTITION_SIZE = 50000


def _create_neuron_loader(filename, population):
    """Create a UDF to load neurons from Sonata

    Args:
        filename: The name of the circuit file
        population: The population to load
    Returns:
        A Pandas UDF to be used over a group by
    """

    @F.pandas_udf(schema.NEURON_SCHEMA, F.PandasUDFType.GROUPED_MAP)
    def loader(data):
        assert len(data) == 1
        import libsonata

        nodes = libsonata.NodeStorage(filename)
        pop = nodes.open_population(population)

        (_,ids) = data.iloc[0]
        selection = libsonata.Selection(ids)
        return pd.DataFrame(
            dict(
                id=ids,
                etype_i=pop.get_enumeration("etype", selection),
                mtype_i=pop.get_enumeration("mtype", selection),
                region_i=pop.get_enumeration("region", selection),
                syn_class_i=pop.get_enumeration("synapse_class", selection),
                morphology=pop.get_attribute("morphology", selection),
            )
        )

    return loader


def _create_touch_loader(filename: str, population: str):
    """Create a UDF to load touches from SONATA

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

        p = libsonata.EdgeStorage(filename).open_population(population)
        selection = libsonata.Selection([(start, end)])

        data = dict(
            source_node_id=p.source_nodes(selection),
            target_node_id=p.target_nodes(selection),
            synapse_id=selection.flatten(),
        )

        for name in p.attribute_names:
            data[name] = p.get_attribute(name, selection)
        return pd.DataFrame(data)
    return loader


class NeuronData:
    """Neuron data loading facilities

    This class represent neuron populations, lazily loaded.  After the
    construction, general properties of the neurons, such as the unique
    values of the
    :attr:`.NeuronData.mtype_values`,
    :attr:`.NeuronData.etype_values`, or
    :attr:`.NeuronData.sclass_values`
    present can be accessed.

    To load neuron-specific information, access the property
    :attr:`.NeuronData.df`, data will be loaded lazily.

    Arguments
    ---------
    population
        a tuple with the path to the neuron storage container and population name
    nodeset
        a tuple with the path to the nodesets JSON file and nodeset name
    cache
        a directory name to use for caching generated Parquet
    """

    def __init__(self, population: [str,str], nodeset: [str,str], cache: str):
        self._cache = cache
        self._df = None
        (self._filename, self._population) = population
        (self._ns_filename, self._ns_nodeset) = nodeset

        import libsonata
        nodes = libsonata.NodeStorage(self._filename)
        self._pop = nodes.open_population(self._population)

        if not os.path.isdir(self._cache):
            os.makedirs(self._cache)

    population = property(lambda self: self._pop.name)
    mtype_values = property(lambda self: self._pop.enumeration_values("mtype"))
    etype_values = property(lambda self: self._pop.enumeration_values("etype"))
    region_values = property(lambda self: self._pop.enumeration_values("region"))
    sclass_values = property(lambda self: self._pop.enumeration_values("synapse_class"))

    def __len__(self):
        return len(self._pop)

    @staticmethod
    def _to_df(vec, field_names):
        """Transforms a small string vector into an indexed dataframe.

        The dataframe is immediately cached and broadcasted as single partition
        """
        return cache_broadcast_single_part(
            sm.createDataFrame(enumerate(vec), schema.indexed_strings(field_names))
        )

    @property
    def df(self):
        if not self._df:
            self._df = self._load_neurons()
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
            self._cache, "neurons_{:.1f}k_{}.parquet".format(len(self) / 1000.0, digest)
        )

        if os.path.exists(mvd_parquet):
            logger.info("Loading circuit from parquet")
            mvd = sm.read.parquet(adjust_for_spark(mvd_parquet, local=True))
            df = F.broadcast(mvd.coalesce(1)).cache()
            df.count()  # force materialize
        else:
            logger.info("Building circuit from SONATA")

            # Create a default selection, or load it from the NodeSets
            if not self._ns_filename:
                ids = [i for i in range(0, len(self))]
            else:
                import libsonata
                nodesets = libsonata.NodeSets.from_file(self._ns_filename)
                selection = nodesets.materialize(self._ns_nodeset, self._pop)
                ids = selection.flatten().tolist()

            total_parts = math.ceil(len(ids) / PARTITION_SIZE)
            logger.debug("Partitions: %d", total_parts)
            parts = sm.createDataFrame(
                enumerate(
                    ids[PARTITION_SIZE * n :
                        min(PARTITION_SIZE * (n + 1), len(ids))]
                    for n in range(total_parts)
                ),
                ['row','ids']
            )

            # Create DF
            logger.info("Creating neuron data frame...")
            raw_mvd = (
                parts
                .groupby("row")
                .apply(_create_neuron_loader(self._filename, self._population))
            )

            # Evaluate (build partial NameMaps) and store
            mvd = raw_mvd.write.mode("overwrite").parquet(
                adjust_for_spark(mvd_parquet, local=True)
            )
            raw_mvd.unpersist()

            # Mark as "broadcastable" and cache
            df = F.broadcast(sm.read.parquet(adjust_for_spark(mvd_parquet))).cache()
        return df


def _grab_parquet(files):
    """Returns as many parquet files from the front of `files` as possible.
    """
    parquets = []
    while files and files[0].endswith(".parquet") :
        if os.path.isdir(files[0]):
            if parquets:
                return parquets
            return [files.pop(0)]
        parquets.append(files.pop(0))
    return parquets


def _grab_sonata_population(filename):
    """Retrieve the default population in a SONATA files.  Raise an
    exception if no population present or more than one population is
    found.
    """
    import libsonata
    populations = libsonata.EdgeStorage(filename).population_names
    if len(populations) == 1:
        return populations[0]
    elif len(populations) > 1:
        raise ValueError(f"More than one population in '{filename}'")
    else:
        raise ValueError(f"No population in '{filename}'")


def _grab_sonata(files):
    """Returns a possible SONATA file from the front of `files`.
    """
    if not files:
        return
    if not files[0].endswith(".h5"):
        return
    filename = files.pop(0)
    if files and not any(files[0].endswith(ext) for ext in (".h5", ".parquet")):
        population = files.pop(0)
    else:
        population = _grab_sonata_population(filename)
    return (filename, population)


class EdgeData:
    """Edge data loading facilities

    This class represent the connectivity between cell populations, lazily
    loaded.  Access the property :attr:`.NeuronData.df`, to load the data.

    Args:
        A list of edge files.
    """

    def __init__(self, *paths):
        files = []
        for path in paths:
            files.extend(glob.glob(path) or [path])
        metadata = []
        self._loaders = []
        while files:
            if parquet := _grab_parquet(files):
                metadata.append(self._load_parquet_metadata(*parquet))
                self._loaders.append(self._load_parquet(*parquet))
            elif sonata := _grab_sonata(files):
                metadata.append(self._load_sonata_metadata(*sonata))
                self._loaders.append(self._load_sonata(*sonata))
            else:
                raise ValueError(f"cannot process {files[0]}")
        if len(set(frozenset(m.items()) for m in metadata)) == 1:
            self._metadata = metadata[0]
        elif metadata:
            logger.debug("Detected multiple different inputs, prefixing metadata")
            self._metadata = dict()
            for key in schema.METADATA_FIXED_KEYS:
                for m in metadata:
                    if key not in m:
                        continue
                    value = m.pop(key)
                    if self._metadata.setdefault(key, value) != value:
                        raise ValueError("conflicting values for metadata " \
                                        f"{key}: {self._metadata[key]}, {value}")
            for n, m in enumerate(metadata):
                self._metadata.update({f"merge{n}_{k}": v for k, v in m.items()})
        else:
            raise ValueError("need to provide at least one file with edges")

    @property
    def df(self):
        df = self._loaders[0]()
        for loader in self._loaders[1:]:
            df = df.union(loader())
        return (
            df
            .withColumnRenamed("source_node_id", "src")
            .withColumnRenamed("target_node_id", "dst")
        )

    @property
    def metadata(self):
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

            def _type(col):
                data = p.get_attribute(col, libsonata.Selection([]))
                return _numpy_to_spark[data.dtype.name]

            def _types(pop):
                for name in pop.attribute_names:
                    yield f"{name} {_type(name)}"

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
            edges = parts.groupby("start", "end").applyInPandas(
                _create_touch_loader(filename, population),
                columns
            )
            logger.info("Total edge count in %s: %d", filename, edges.count())
            return edges
        return _loader

    @staticmethod
    def _load_parquet_metadata(*args):
        if len(args) == 1:
            args = args[0]
        else:
            args = list(args)
        schema = pq.ParquetDataset(args).schema.to_arrow_schema()
        return {
            k.decode(): v.decode()
            for (k, v) in (schema.metadata or dict()).items()
            if not k.startswith(b"org.apache.spark")
        }

    @staticmethod
    def _load_parquet(*args):
        def _loader():
            files = [adjust_for_spark(f) for f in args]
            edges = sm.read.parquet(*files)
            for old, new in schema.LEGACY_MAPPING.items():
                if old in edges.columns:
                    edges = edges.withColumnRenamed(old, new)
            edges = edges.cache()
            logger.info("Total edge count in %s: %d", ", ".join(files), edges.count())
            return edges
        return _loader
