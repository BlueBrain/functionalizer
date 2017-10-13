import h5py
import os
from os import path
import numpy
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from . import utils

logger = utils.get_logger(__name__)
N_NEURONS_FILE = 1000

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

class NeuronExporter(object):
    def __init__(self, output_path):
        self.output_path = output_path
        # Get the concat_bin agg function form the java world
        _j_conc_udaf = sc._jvm.spykfunc.udfs.BinaryConcat().apply
        self.concat_bin = utils.wrap_java_udf(spark.sparkContext, _j_conc_udaf)

    def ensure_file_path(self, filename):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        return path.join(self.output_path, filename)

    # ---
    def save_temp(self, touches, filename="filtered_touches.tmp.parquet"):
        output_path = self.ensure_file_path(filename)
        touches.write.parquet(output_path, mode="overwrite")
        logger.info("Filtered touches temporarily saved to %s", output_path)
        return spark.read.parquet(output_path)  # break execution plan

    # ---
    def export_parquet(self, extended_touches_df, filename="nrn.parquet"):
        output_path = self.ensure_file_path(filename)
        return extended_touches_df.write.partitionBy("post_gid").parquet(output_path, mode="overwrite")

    # ---
    def export_hdf5(self, extended_touches_df, n_gids, filename="nrn.h5"):
        # In the export a lot of shuffling happens, we must carefully control partitioning
        n_partitions = ((n_gids - 1) // N_NEURONS_FILE) + 1
        n_partitions_work = (n_partitions*4) if (n_partitions*4 < 256) else n_partitions
        spark.conf.set("spark.sql.shuffle.partitions", n_partitions_work)

        nrn_filepath = self.ensure_file_path(filename)
        df = extended_touches_df

        # Massive conversion to binary using 'float2binary' java UDF and 'concat_bin' UDAF
        nrn_vals = df.select(df.pre_gid, df.post_gid, F.array(*self.nrn_fields_as_float(df)).alias("floatvec") )
        arrays_df = (nrn_vals
                     .selectExpr("pre_gid", "post_gid", "float2binary(floatvec) as bin_arr")
                     .sort("post_gid")
                     .sortWithinPartitions("post_gid", "pre_gid")
                     .groupBy("post_gid", "pre_gid")
                     .agg(self.concat_bin("bin_arr").alias("bin_matrix"), F.count("*").cast("int").alias("conn_count"))
                     .sort("post_gid")
                     .groupBy("post_gid")
                     .agg(self.concat_bin("bin_matrix").alias("bin_matrix"), F.collect_list("pre_gid").alias("pre_gids"), F.collect_list("conn_count").alias("conn_counts"))
                     .selectExpr("post_gid", "bin_matrix", "int2binary(pre_gids) as pre_gids_bin", "int2binary(conn_counts) as conn_counts_bin")
                     )

        # Number of partitions to number of files
        logger.debug("Ordering into {} partitions".format(n_partitions))
        arrays_df = arrays_df.orderBy("post_gid").coalesce(n_partitions)

        # Export via partition mapping
        write_hdf5 = get_export_hdf5_f(nrn_filepath)
        result_files = arrays_df.rdd.mapPartitions(write_hdf5).collect()
        logger.info("Files written: %s", ", ".join(result_files))

    @staticmethod
    def nrn_fields_as_float(df):
        # Select fields and cast to Float
        return (
            df.pre_gid.cast(T.FloatType()).alias("gid"),
            df.axional_delay,
            df.post_section.cast(T.FloatType()).alias("post_section"),
            df.post_segment.cast(T.FloatType()).alias("post_segment"),
            df.post_offset,
            df.pre_section.cast(T.FloatType()).alias("pre_section"),
            df.pre_segment.cast(T.FloatType()).alias("pre_segment"),
            df.pre_offset,
            "gsyn", "u", "d", "f", "dtc",
            df.synapseType.cast(T.FloatType()).alias("synapseType"),
            df.morphology.cast(T.FloatType()).alias("morphology"),
            df.branch_order_dend.cast(T.FloatType()).alias("branch_order_dend"),
            df.branch_order_axon.cast(T.FloatType()).alias("branch_order_axon"),
            df.ase.cast(T.FloatType()).alias("ase"),
            df.branch_type.cast(T.FloatType()).alias("branch_type")  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
        )



def get_export_hdf5_f(nrn_filepath):
    # The export routine - applied to each partition
    def write_hdf5(part_it):
        h5store = None
        output_filename = None
        for row in part_it:
            post_id = row[0]
            buff = row[1]
            if h5store is None:
                output_filename = "{}.{}".format(nrn_filepath, post_id)
                h5store = h5py.File(output_filename, "w")
            # We reconstruct the array in Numpy from the binary
            np_array = numpy.frombuffer(buff, dtype=">f4").reshape((-1, 19))
            h5store.create_dataset("a{}".format(post_id), data=np_array)
        h5store.close()
        return [output_filename]

    return write_hdf5
