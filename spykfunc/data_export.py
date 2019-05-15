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
from . import utils
from .definitions import SortBy
from .utils import spark_udef as spark_udef_utils
from .utils.filesystem import adjust_for_spark
from . import tools

DEFAULT_N_NEURONS_FILE = 200

logger = utils.get_logger(__name__)


MAPPING_DEFAULT = [
    ("post_gid", "connected_neurons_post", T.LongType()),
    ("pre_gid", "connected_neurons_pre", T.LongType()),
    ("axonal_delay", "delay", None),
    ("gsyn", "conductance", None),
    ("u", "u_syn", None),
    ("d", "depression_time", None),
    ("f", "facilitation_time", None),
    ("dtc", "decay_time", None),
    ("synapseType", "syn_type_id", None),
    ("morphology", "morpho_type_id_pre", None),
    # ("branch_order_dend", "morpho_branch_order_dend", None),  # N/A
    # ("branch_order_axon", "morpho_branch_order_axon", None),  # Irrelevant
    ("nrrp", "n_rrp_vesicles", T.ShortType()),
]

MAPPING_ADDONS_INDICES = [
    ("post_section", "morpho_section_id_post", None),
    ("post_segment", "morpho_segment_id_post", None),
    ("post_offset", "morpho_offset_segment_post", T.FloatType()),
    ("pre_section", "morpho_section_id_pre", None),
    ("pre_segment", "morpho_segment_id_pre", None),
    ("pre_offset", "morpho_offset_segment_pre", T.FloatType()),
]

MAPPING_ADDONS_V2 = [
    ("pre_section_fraction", "morpho_section_fraction_pre", T.FloatType()),
    ("post_section_fraction", "morpho_section_fraction_post", T.FloatType()),
    ("branch_type", "morpho_section_type_post", None),
    ("spine_length", "morpho_spine_length", None),
    ("pre_position_x", "position_contour_pre_x", None),
    ("pre_position_y", "position_contour_pre_y", None),
    ("pre_position_z", "position_contour_pre_z", None),
    ("post_position_x", "position_center_post_x", None),
    ("post_position_y", "position_center_post_y", None),
    ("post_position_z", "position_center_post_z", None),
]

MAPPING_GAP_JUNCTIONS = [
    ("src", "connected_neurons_pre", T.LongType()),
    ("dst", "connected_neurons_post", T.LongType()),
    ("post_junction", "junction_id_post", None),
    ("pre_junction", "junction_id_pre", None),
]

# Longest mapping first, to avoid matching subsets.
FIELD_MAPPINGS = [
    MAPPING_DEFAULT + MAPPING_ADDONS_INDICES + MAPPING_ADDONS_V2,
    MAPPING_DEFAULT + MAPPING_ADDONS_INDICES,
    MAPPING_GAP_JUNCTIONS + MAPPING_ADDONS_INDICES
]


class NeuronExporter(object):
    def __init__(self, output_path):
        self.output_path = path.realpath(output_path)
        # Get the concat_bin agg function form the java world
        _j_conc_udaf = sm._jvm.spykfunc.udfs.BinaryConcat().apply
        self.concat_bin = spark_udef_utils.wrap_java_udf(sm.sc, _j_conc_udaf)

    def ensure_file_path(self, filename):
        path.exists(self.output_path) or os.makedirs(self.output_path)
        return path.join(self.output_path, filename)

    # ---
    # [DEPRECATED]
    def save_temp(self, touches, filename="filtered_touches.tmp.parquet", partition_col=None):
        output_path = self.ensure_file_path(filename)
        if partition_col is not None:
            touches.write.partitionBy(partition_col).parquet(output_path, mode="overwrite")
        else:
            touches.write.parquet(output_path, mode="overwrite")
        logger.info("Filtered touches temporarily saved to %s", output_path)
        return sm.read.parquet(output_path)  # break execution plan

    def get_temp_result(self, filename="filtered_touches.tmp.parquet"):
        filepath = path.join(self.output_path, filename)
        if path.exists(filepath):
            return sm.read.parquet(filepath)
        return None

    # ---
    def export_syn2_parquet(self, df,
                                  filename="circuit.parquet",
                                  order: SortBy=SortBy.POST):
        """ Exports the results to parquet, following the transitional SYN2 spec
        with support for legacy NRN fields, e.g.: morpho_segment_id_post
        """
        output_path = os.path.join(self.output_path, filename)
        # Sorting will always incur a shuffle, so we sort by post-pre, as accessed in most cases
        df_output = df.select(*self.get_syn2_parquet_fields(df)) \
                      .sort(*(order.value))
        df_output.write.parquet(adjust_for_spark(output_path, local=True), mode="overwrite")

    # ---
    def export_hdf5(self, extended_touches_df, n_gids, create_efferent=False, n_partitions=None):
        # In the export a lot of shuffling happens, we must carefully control partitioning
        if n_partitions is None:
            n_partitions = ((n_gids - 1) // DEFAULT_N_NEURONS_FILE) + 1
        # We use shuffle.partitions to define the nr of partitions since coalesce is innefective
        # with larger number of partitions (and repartition is not an option!)
        sm.conf.set("spark.sql.shuffle.partitions", n_partitions)

        nrn_filepath = self.ensure_file_path("_nrn.h5")
        for fn in glob.glob1(self.output_path, "*nrn*.h5*"):
            os.remove(path.join(self.output_path, fn))

        df = extended_touches_df

        # Massive conversion to binary using 'float2binary' java UDF and 'concat_bin' UDAF
        arrays_df = (
            df
            .select(df.pre_gid, df.post_gid,
                    F.array(*self.nrn_fields_as_float(df)).alias("floatvec"))
            .sort("post_gid")
            .selectExpr("(pre_gid + 1) as pre_gid",  # nrn gids start at 1
                        "(post_gid + 1) as post_gid",
                        "float2binary(floatvec) as bin_arr")
            .groupBy("post_gid", "pre_gid")
            .agg(self.concat_bin("bin_arr").alias("bin_matrix"),
                 F.count("*").cast("int").alias("conn_count"))
            .sort("post_gid", "pre_gid")
            .groupBy("post_gid")
            .agg(self.concat_bin("bin_matrix").alias("bin_matrix"),
                 F.collect_list("pre_gid").alias("pre_gids"),
                 F.collect_list("conn_count").alias("conn_counts"))
            .selectExpr("post_gid",
                        "bin_matrix",
                        "int2binary(pre_gids) as pre_gids_bin",
                        "int2binary(conn_counts) as conn_counts_bin")
        )

        # arrays_df = self.save_temp(arrays_df, "aggregated_touches.parquet", partition_col="file_nr")

        # Init a list accumulator to gather output filenames
        nrn_filenames = sm.accumulator([], spark_udef_utils.ListAccum())

        # Export nrn.h5 via partition mapping
        logger.info("Saving to NRN.h5 in parallel... ({} files) [3 stages]".format(n_partitions))
        write_hdf5 = get_export_hdf5_f(nrn_filepath, nrn_filenames)
        summary_rdd = arrays_df.rdd.mapPartitions(write_hdf5)
        # Count to trigger saving, caching needed results.
        # We need it since there's a coalesce after, for little parallelism later
        summary_rdd = summary_rdd.persist(StorageLevel.MEMORY_AND_DISK)
        summary_rdd.count()

        # Mass rename
        new_names = []
        for i, fn in enumerate(nrn_filenames.value):
            new_name = path.join(self.output_path, "nrn.h5.{}".format(i))
            os.rename(fn, new_name)
            new_names.append(new_name)

        if create_efferent:
            logger.info("Creating nrn_efferent files in parallel")
            # Process conversion in parallel
            nrn_files_rdd = sm.parallelize(new_names, len(new_names))
            nrn_files_rdd.map(create_other_files).count()

        # Export the base for nrn_summary (only afferent counts)
        n_parts = (n_gids-1) // 100000 + 1  # Each 100K NRNs is roughly 500MB serialized data
        logger.info("Creating nrn_summary.h5 [{} parts]".format(n_parts))
        summary_rdd = summary_rdd.coalesce(n_parts)
        summary_path = path.join(self.output_path, ".nrn_summary0.h5")
        summary_h5_store = h5py.File(summary_path, "w")
        n = 0
        for post_gid, summary_npa in summary_rdd.toLocalIterator():
            n += 1
            summary_h5_store.create_dataset("a{}".format(post_gid), data=summary_npa)
            if stderr.isatty():
                # Show progress directy on stderr if attached to terminal
                print("\rProgress: {} / {}".format(n, n_gids), end="", file=stderr)
        summary_h5_store.close()
        if stderr.isatty():
            print("Complete.", file=stderr)

        # Build merged nrn_summary
        # TODO: Have this processing done in spark since now the summary is cached
        logger.debug("Transposing nrn_summary")
        final_nrn_summary = path.join(self.output_path, "nrn_summary.h5")
        nrn_completer = tools.NrnCompleter(summary_path, logger=logger)
        nrn_completer.create_transposed()
        logger.debug("Merging into final nrn_summary")
        nrn_completer.merge(merged_filename=final_nrn_summary)
        nrn_completer.add_meta(final_nrn_summary, dict(
            version=3,
            numberOfFiles=len(nrn_filenames.value)
        ))

    @staticmethod
    def get_syn2_parquet_fields(df):
        # Transitional SYN2 spec fields
        for mapping in FIELD_MAPPINGS:
            if all(hasattr(df, f) for f, _, _ in mapping):
                for field, alias, cast in mapping:
                    if cast:
                        yield getattr(df, field).cast(cast).alias(alias)
                    else:
                        yield getattr(df, field).alias(alias)
                break
        else:
            raise AttributeError("cannot determine field mapping from filter output")

    # ---
    @staticmethod
    def nrn_fields_as_float(df):
        # Select fields and cast to Float
        return (
            df.pre_gid.cast(T.FloatType()).alias("gid"),
            df.axonal_delay.cast(T.FloatType()),
            df.post_section.cast(T.FloatType()).alias("post_section"),
            df.post_segment.cast(T.FloatType()).alias("post_segment"),
            df.post_offset.cast(T.FloatType()),
            df.pre_section.cast(T.FloatType()).alias("pre_section"),
            df.pre_segment.cast(T.FloatType()).alias("pre_segment"),
            df.pre_offset.cast(T.FloatType()),
            df.gsyn.cast(T.FloatType()),
            df.u.cast(T.FloatType()),
            df.d.cast(T.FloatType()),
            df.f.cast(T.FloatType()),
            df.dtc.cast(T.FloatType()),
            df.synapseType.cast(T.FloatType()).alias("synapseType"),
            df.morphology.cast(T.FloatType()).alias("morphology"),
            df.branch_order_dend.cast(T.FloatType()).alias("branch_order_dend"),
            df.branch_order_axon.cast(T.FloatType()).alias("branch_order_axon"),
            df.nrrp.cast(T.FloatType()).alias("nrrp"),
            df.branch_type.cast(T.FloatType()).alias("branch_type")
            # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
        )


def get_export_hdf5_f(nrn_filepath, nrn_filenames_accu):
    """
    Returns the export_hdf5 routine, parametrized
    :param nrn_filepath: The base filename for the nrn files
    :param nrn_filenames_accu: The accumulator where to append the generated files' name
    """
    # The export routine - applied to each partition
    def write_hdf5(part_it):
        h5store = None
        output_filename = None
        for row in part_it:
            post_id = row[0]
            buff = row[1]
            pre_gids_buff = row[2]
            conn_counts_buff = row[3]
            if h5store is None:
                output_filename = "{}.{}".format(nrn_filepath, post_id)
                h5store = h5py.File(output_filename, "w")
            # We reconstruct the array in Numpy from the binary
            np_array = numpy.frombuffer(buff, dtype="f4").reshape((-1, 19))
            h5store.create_dataset("a{}".format(post_id), data=np_array)

            # Gather pre_gids and conn_counts as np to be passed to the master
            # Where they are centrally written to nrn_summary
            # This is RDDs here, so we are free to pass numpy arrays
            pre_gids = numpy.frombuffer(pre_gids_buff, dtype="i4")
            total_counts = numpy.frombuffer(conn_counts_buff, dtype="i4")
            count_array = numpy.column_stack((pre_gids, total_counts))
            yield (post_id, count_array)

        h5store.close()
        nrn_filenames_accu.add([output_filename])

    return write_hdf5


def create_other_files(nrn_file):
    converter = tools.NrnConverter()
    converter.create_efferent(nrn_file)
