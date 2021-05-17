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


class TouchData:
    """Touch data loading facilities

    This class represent the connectivity between cell populations, lazily
    loaded.  Access the property :attr:`.NeuronData.df`, to load the data.

    Arguments
    ---------
    parquet
        A list of touch files; a single globbing expression can be
        specified as well
    sonata
        Alternative touch representation, consisting of edge population
        path and name
    """

    def __init__(self, parquet, sonata):
        if parquet:
            all_parquet = []
            for path in parquet:
                all_parquet.extend(glob.glob(path))
            self._loader = self._load_parquet
            self._metadata = self._load_parquet_metadata(*all_parquet)
            self._args = all_parquet
        elif sonata:
            self._loader = self._load_sonata
            self._metadata = self._load_sonata_metadata(*sonata)
            self._args = sonata
        else:
            raise ValueError("TouchData needs to be initialized with touch data")

    @property
    def df(self):
        return (
            self._loader(*self._args)
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

        logger.info("Creating touch data frame...")
        touches = parts.groupby("start", "end").applyInPandas(
            _create_touch_loader(filename, population),
            columns
        )
        logger.info("Total touches: %d", touches.count())
        return touches

    @staticmethod
    def _load_parquet_metadata(*files):
        schema = pq.ParquetDataset(list(files)).schema.to_arrow_schema()
        return {
            k: v
            for (k, v) in (schema.metadata or dict()).items()
            if not k.startswith(b"org.apache.spark")
        }

    @staticmethod
    def _load_parquet(*files):
        files = [adjust_for_spark(f) for f in files]
        touches = sm.read.parquet(*files)
        for old, new in schema.LEGACY_MAPPING.items():
            if old in touches.columns:
                touches = touches.withColumnRenamed(old, new)
        touches = touches.cache()
        logger.info("Total touches: %d", touches.count())
        return touches
