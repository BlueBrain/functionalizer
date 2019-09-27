from __future__ import print_function
import h5py
import os
from sys import stderr
from os import path
import glob
import numpy
import sparkmanager as sm

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T
from . import schema
from . import utils
from .definitions import SortBy
from .utils import spark_udef as spark_udef_utils
from .utils.filesystem import adjust_for_spark

logger = utils.get_logger(__name__)


class NeuronExporter(object):
    def __init__(self, output_path):
        self.output_path = path.realpath(output_path)
        # Get the concat_bin agg function form the java world
        _j_conc_udaf = sm._jvm.spykfunc.udfs.BinaryConcat().apply
        self.concat_bin = spark_udef_utils.wrap_java_udf(sm.sc, _j_conc_udaf)

    def ensure_file_path(self, filename):
        path.exists(self.output_path) or os.makedirs(self.output_path)
        return path.join(self.output_path, filename)

    def export(self, df, filename="circuit.parquet", order: SortBy=SortBy.POST):
        """ Exports the results to parquet, following the transitional SYN2 spec
        with support for legacy NRN fields, e.g.: morpho_segment_id_post
        """
        output_path = os.path.join(self.output_path, filename)
        # Sorting will always incur a shuffle, so we sort by post-pre, as accessed in most cases
        df_output = df.select(*self.get_syn2_parquet_fields(df)) \
                      .sort(*(order.value))
        df_output.write.parquet(adjust_for_spark(output_path, local=True), mode="overwrite")

    @staticmethod
    def get_syn2_parquet_fields(df):
        # Transitional SYN2 spec fields
        for field, alias, cast in schema.OUTPUT_COLUMN_MAPPING:
            if hasattr(df, field):
                if cast:
                    yield getattr(df, field).cast(cast).alias(alias)
                else:
                    yield getattr(df, field).alias(alias)
