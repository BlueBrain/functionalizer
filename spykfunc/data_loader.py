from __future__ import absolute_import

import hashlib
import os
import sparkmanager as sm
import logging
import pandas as pd
from lazy_property import LazyProperty
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import schema
from .definitions import MType
from .utils import get_logger, make_slices, to_native_str
from .utils.spark import cache_broadcast_single_part
from .utils.filesystem import adjust_for_spark


# Globals
logger = get_logger(__name__)


def _create_loader(filename, population):
    """Create a UDF to load neurons from MVD3

    Args:
        filename: The name of the circuit file
        population: The population to load
    Returns:
        A Pandas UDF to be used over a group by
    """
    @F.pandas_udf(schema.NEURON_SCHEMA, F.PandasUDFType.GROUPED_MAP)
    def loader(data):
        assert(len(data) == 1)
        import mvdtool
        fd = mvdtool.open(filename, population)
        start, end = data.iloc[0]
        count = end - start
        return pd.DataFrame(
            dict(
                id=range(start, end),
                mtype_i=fd.raw_mtypes(start, count),
                etype_i=fd.raw_etypes(start, count),
                morphology=fd.morphologies(start, count),
                syn_class_i=fd.raw_synapse_classes(start, count),
            )
        )
    return loader


class NeuronData:
    """Neuron data loading facilities
    """

    PARTITION_SIZE = 20000

    def __init__(self, filename, population, cache):
        import mvdtool
        self._cache = cache
        self._filename = filename
        self._population = population
        self._mvd = mvdtool.open(self._filename, self._population)

        if not os.path.isdir(self._cache):
            os.makedirs(self._cache)

    @LazyProperty
    def mtypes(self):
        """All morphology types present in the circuit
        """
        return self._mvd.all_mtypes

    @LazyProperty
    def etypes(self):
        """All electrophysiology types present in the circuit
        """
        return self._mvd.all_etypes

    @LazyProperty
    def cell_classes(self):
        """All cell classes present in the circuit
        """
        return self._mvd.all_synapse_classes

    def __len__(self):
        return len(self._mvd)

    @staticmethod
    def _to_df(vec, field_names):
        """Transforms a small string vector into an indexed dataframe.

        The dataframe is immediately cached and broadcasted as single partition
        """
        return cache_broadcast_single_part(
            sm.createDataFrame(enumerate(vec), schema.indexed_strings(field_names)))

    @LazyProperty
    def sclass_df(self):
        return self._to_df(self.cell_classes, ["sclass_i", "sclass_name"])

    @LazyProperty
    def mtype_df(self):
        return self._to_df(self.mtypes, ["mtype_i", "mtype_name"])

    @LazyProperty
    def etype_df(self):
        return self._to_df(self.etypes, ["etype_i", "etype_name"])

    def load_neurons(self):
        fn = self._filename
        sha = hashlib.sha256()
        sha.update(os.path.realpath(fn).encode())
        sha.update(self._population.encode())
        sha.update(str(os.stat(fn).st_size).encode())
        sha.update(str(os.stat(fn).st_mtime).encode())
        digest = sha.hexdigest()[:8]

        logger.info("Total neurons: %d", len(self))
        mvd_parquet = os.path.join(
            self._cache,
            "neurons_{:.1f}k_{}.parquet".format(len(self) / 1000.0, digest)
        )

        if os.path.exists(mvd_parquet):
            logger.info("Loading circuit from parquet")
            mvd = sm.read.parquet(adjust_for_spark(mvd_parquet, local=True)).cache()
            self.df = F.broadcast(mvd)
            self.df.count()  # force materialize
        else:
            logger.info("Building circuit from raw mvd files")
            total_parts = len(self) // self.PARTITION_SIZE + 1
            logger.debug("Partitions: %d", total_parts)

            parts = sm.createDataFrame(
                (
                    (self.PARTITION_SIZE * n,
                     min(self.PARTITION_SIZE * (n + 1), len(self)))
                    for n in range(total_parts)
                ),
                "start: int, end: int"
            )

            # Create DF
            logger.info("Creating data frame...")
            raw_mvd = (
                parts
                .groupby("start", "end")
                .apply(_create_loader(self._filename, self._population))
            )

            # Evaluate (build partial NameMaps) and store
            mvd = (
                raw_mvd
                .write.mode('overwrite')
                .parquet(adjust_for_spark(mvd_parquet, local=True))
            )
            raw_mvd.unpersist()

            # Mark as "broadcastable" and cache
            self.df = F.broadcast(
                sm.read.parquet(adjust_for_spark(mvd_parquet))
            ).cache()

    @staticmethod
    def load_touch_parquet(*files):
        def compatible(s1: T.StructType, s2: T.StructType) -> bool:
            """Test schema compatibility
            """
            if len(s1.fields) != len(s2.fields):
                return False
            for f1, f2 in zip(s1.fields, s2.fields):
                if f1.name != f2.name:
                    return False
            return True
        files = [adjust_for_spark(f) for f in files]
        have = sm.read.load(files[0]).schema
        try:
            want, = [s for s in schema.TOUCH_SCHEMAS if compatible(s, have)]
        except ValueError:
            logger.error("Incompatible schema of input files")
            raise RuntimeError("Incompatible schema of input files")
        touches = sm.read.schema(want).parquet(*files)
        logger.info("Total touches: %d", touches.count())
        return touches
