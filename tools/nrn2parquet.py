"""Convert neuron files to parquet
"""
from __future__ import print_function

import argparse
import glob
import h5py
import numpy
import os
import sparkmanager as sm

from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import BinaryType

from spykfunc import utils


class _ConfDumpAction(argparse._HelpAction):
    """Dummy class to list default configuration and exit, just like `--help`.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        from spykfunc.utils import Configuration
        kwargs = dict(overrides=namespace.overrides)
        if namespace.configuration:
            kwargs['configuration'] = namespace.configuration
        Configuration(namespace.output_dir, **kwargs).dump()
        parser.exit()


parser = argparse.ArgumentParser()
parser.add_argument("output", help="the output directory")
parser.add_argument("files", help="nrn files to merge", nargs="+")
parser.add_argument("-c", "--configuration",
                    help="A configuration file to use. See `--dump-defaults` for default settings")
parser.add_argument("-p", "--property", dest='overrides', action='append', default=[],
                    help="Override single properties of the configuration, i.e.,"
                         "`-p spark.master=spark://1.2.3.4:7077`. May be specified multiple times.")
parser.add_argument("--dump-configuration", action=_ConfDumpAction,
                    help="Show the configuration including modifications via options prior to this "
                         "flag and exit")
parser.add_argument("--big-endian", action='store_true',
                    help="Convert from big endian to little endian")
args = parser.parse_args()

properties = utils.Configuration(outdir=args.output,
                                 filename=args.configuration,
                                 overrides=[s.split('=', 1) for s in args.overrides])

sm.create("validation", properties("spark"))
sql = SQLContext.getOrCreate(sm.sc)
if args.big_endian:
    sql.registerJavaFunction("binary2float", "spykfunc.udfs.FloatArrayDeserializer")
else:
    sql.registerJavaFunction("binary2float", "spykfunc.udfs.FloatArrayDeserializerLittleEndian")


def _h5datasets(fn):
    with h5py.File(fn, 'r') as f:
        result = []

        def visitation(ds):
            if ds.startswith('a'):
                post_gid = int(ds[1:])
                result.extend((bytearray(row.tobytes()),) for row in numpy.insert(f[ds].value, 1, post_gid, axis=1))
        f.visit(visitation)
    return result


def h5datasets(fn):
    with h5py.File(fn) as f:
        return [[fn, ds.name] for ds in f['/'].values() if ds.name.startswith('/a')]


def h5conversion((fn, ds)):
    with h5py.File(fn) as f:
        post_gid = int(ds[2:])
        return [(bytearray(row.tobytes()),) for row in numpy.insert(f[ds].value, 1, post_gid, axis=1)]


h5fns = sm.parallelize(args.files, len(args.files))
h5rdd = h5fns.flatMap(h5datasets).flatMap(h5conversion)
binary_schema = StructType([StructField("binary", BinaryType(), False)])
h5binary = sql.createDataFrame(h5rdd, binary_schema)
h5floats = h5binary.selectExpr("binary2float(binary) as data")
h5touches = h5floats.select(
    (h5floats.data.getItem(0).cast("int") - 1).alias("pre_gid"),
    (h5floats.data.getItem(1).cast("int") - 1).alias("post_gid"),
    h5floats.data.getItem(2).alias("axional_delay"),
    h5floats.data.getItem(3).cast("int").alias("post_section"),
    h5floats.data.getItem(4).cast("int").alias("post_segment"),
    h5floats.data.getItem(5).alias("post_offset"),
    h5floats.data.getItem(6).cast("int").alias("pre_section"),
    h5floats.data.getItem(7).cast("int").alias("pre_segment"),
    h5floats.data.getItem(8).alias("pre_offset"),
    h5floats.data.getItem(9).alias("gsyn"),
    h5floats.data.getItem(10).alias("u"),
    h5floats.data.getItem(11).alias("d"),
    h5floats.data.getItem(12).alias("f"),
    h5floats.data.getItem(13).alias("dtc"),
    h5floats.data.getItem(14).alias("synapseType"),
    h5floats.data.getItem(15).alias("morphology"),
    h5floats.data.getItem(16).alias("branch_order_dend"),
    h5floats.data.getItem(17).alias("branch_order_axon"),
    h5floats.data.getItem(18).alias("ase"),
    h5floats.data.getItem(19).alias("branch_type"))
    # h5floats.data.getItem(14).cast("int").alias("synapseType"),
    # h5floats.data.getItem(15).cast("int").alias("morphology"),
    # h5floats.data.getItem(16).cast("int").alias("branch_order_dend"),
    # h5floats.data.getItem(17).cast("int").alias("branch_order_axon"),
    # h5floats.data.getItem(18).cast("int").alias("ase"),
    # h5floats.data.getItem(19).cast("int").alias("branch_type"))

h5touches.write.parquet(os.path.join(args.output, "circuit.parquet"))
print("converted {} touches".format(h5touches.count()))
