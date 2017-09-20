import h5py
import pyspark
import os
from os import path
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from . import utils
from .dataio import morphotool
import numpy

logger = utils.get_logger(__name__)
N_NEURONS_FILE = 1000


class NeuronExporter(object):
    def __init__(self, morpho_dir, recipe, syn_properties, output_path="output"):
        # if not morphotool:
        #     raise RuntimeError("Can't export to .h5. Morphotool not available")
        self._neuronG = False
        self.touches = None

        self.output_path = output_path
        self.morpho_dir = morpho_dir
        self.recipe = recipe
        self.syn_properties_df = syn_properties
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # Broadcast an empty dict to hold morphologies
        # Each worker will fill it as required, no communication incurred
        self.spark = SparkSession.builder.getOrCreate()
        self.sc = self.spark.sparkContext  # type: pyspark.SparkContext
        self.morphologies = {}
        self.sc.broadcast(self.morphologies)


        _conc = self.sc._jvm.spykfunc.udfs.BinaryConcat().apply
        self.concat_bin = utils.make_agg_f(self.spark.sparkContext, _conc)

    @property
    def neuronG(self):
        return self._neuronG

    @neuronG.setter
    def neuronG(self, newval):
        self._neuronG = newval
        self.n_ids = self._neuronG.vertices.count()
        # Create required / select fields that belong to nrn.h5 spec
        self.touches = self.compute_additional_h5_fields()

    # ---
    def save_temp(self, neuronG=None, name="filtered_touches.tmp.parquet"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG
        self.touches.write.parquet(name, mode="overwrite")
        self.touches = self.spark.read.parquet(name)

    # ---
    def export_parquet(self, neuronG=None, filename="nrn.parquet"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG
        nrn_filepath = path.join(self.output_path, filename)
        return self.touches.write.mode("overwrite").partitionBy("post_gid").parquet(nrn_filepath, compression="gzip")

    # ---
    # def export_hdf5(self, filename="nrn.h5"):
    #     nrn_filepath = path.join(self.output_path, filename)
    #
    #     # In order to sequentially write and not overflood the master, we query and write touches GID per GID
    #     gids_df = self.neuronG.vertices.select("id").orderBy("id")
    #     gids = gids_df.rdd.keys().collect()  # In large cases we can use toLocalIterator()
    #     _many_files = len(gids) > 10000
    #
    #     h5store = None
    #
    #     for i, gid in enumerate(gids[::1000]):
    #         if i % 10000 == 0:
    #             cur_name = nrn_filepath
    #             if _many_files:
    #                 cur_name += ".{}".format(i//10000)
    #             if h5store:
    #                 h5store.close()
    #             h5store = h5py.File(cur_name, "w")
    #
    #         # The df of the neuron to export
    #         df = self.touches.where(F.col("post_gid") == gid).orderBy("pre_gid")
    #         df = self.prepare_df_to_nrn_format(df)
    #
    #         # logger.debug("Writing neuron {}".format(gid))
    #         # data_np = df.toPandas().as_matrix().astype("f4")
    #
    #         df.select(F.array("*").alias("floatvec")).registerTempTable("nrn_vals")
    #         array_df = df.sql_ctx.sql("select float2binary(floatvec) as binary from df")
    #         merged = array_df.agg(self.concat_bin(array_df.binary))
    #
    #         buffer = merged.rdd.keys().collect()[0]
    #         numpy.frombuffer(buffer, dtype=">f4").reshape((-1, 19))
    #
    #         h5store.create_dataset("a{}".format(gid), data=numpy)
    #
    #     if h5store:
    #         h5store.close()


    def export_hdf5(self, neuronG=None, filename="nrn.h5"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG

        nrn_filepath = path.join(self.output_path, filename)

        n_gids = self.neuronG.vertices.count()


        df = self.touches
        df.select(df.post_gid, F.array(*self.nrn_fields_as_float(df)).alias("floatvec")).registerTempTable("nrn_vals")

        # Massive conversion to binary
        arrays_df = df.sql_ctx.sql("select post_gid, float2binary(floatvec) as bin_arr from nrn_vals").groupBy("post_gid").agg(self.concat_bin("bin_arr").alias("bin_maxtrix"))

        # Number of partitions to number of files
        # NOTE: This is a workaround to control the number of partitions after the sort
        previous_def = int(self.spark.conf.get("spark.sql.shuffle.partitions"))
        self.spark.conf.set("spark.sql.shuffle.partitions", ((n_gids-1)//N_NEURONS_FILE) + 1)
        arrays_df = arrays_df.orderBy("post_gid")
        self.spark.conf.set("spark.sql.shuffle.partitions", previous_def)

        def write_hdf5(part_it):
            h5store = None
            output_filename = None
            for row in part_it:
                post_id = row[0]
                buff = row[1]

                if h5store is None:
                    output_filename = nrn_filepath + ".{}".format(post_id//N_NEURONS_FILE)
                    h5store = h5py.File(output_filename, "w")

                np_array = numpy.frombuffer(buff, dtype=">f4").reshape((-1, 19))
                h5store.create_dataset("a{}".format(post_id), data=np_array)

            h5store.close()
            return [output_filename]

        # # Deep debug
        # print([[n[0] for n in r] for r in arrays_df.select("post_gid").rdd.glom().collect()])

        # Export via partition mapping
        result_files = arrays_df.rdd.mapPartitions(write_hdf5).collect()
        logger.info("Files written: {}", ", ".join(result_files))


    # -----
    def compute_additional_h5_fields(self):
        touch_G = self.neuronG.find("(n1)-[t]->(n2)")

        # prepare DF - add required fields
        p_df = self.syn_properties_df.select(F.struct("*").alias("prop"))
        touches = touch_G.join(p_df, ((touch_G.n1.syn_class_index == p_df.prop.fromSClass_i) &
                                      (touch_G.n2.syn_class_index == p_df.prop.toSClass_i)))

        # 0: Connecting gid: presynaptic for nrn.h5, postsynaptic for nrn_efferent.h5
        # 1: Axonal delay: computed using the distance of the presynaptic axon to the post synaptic terminal (milliseconds) (float)
        # 2: postSection ID (int)
        # 3: postSegment ID (int)
        # 4: The post distance (in microns) of the synapse from the begining of the post segment 3D point, or \-1 for soma connections  (float)
        # 5: preSection ID (int)
        # 6: preSegment ID (int)
        # 7: The pre distance (in microns) of the synapse from the begining of the pre segment  3D point (float)
        # 8: g_synX is the conductance of the synapse (nanosiemens) (float)
        # 9: u_syn is the u parameter in the TM model (0-1) (float)
        # 10: d_syn is the time constant of depression (milliseconds) (int)
        # 11: f_syn is the time constant of facilitation (milliseconds) (int)
        # 12: DTC - Decay Time Constant (milliseconds) (float)
        # 13: synapseType, the synapse type Inhibitory < 100 or Excitatory >= 100 (specific value corresponds to generating recipe)
        # 14: The morphology type of the pre neuron.  Index corresponds with circuit.mvd2
        # 15-16: BranchOrder of the dendrite, BranchOrder of the axon (int,int)
        # 17: ASE Absolute Synaptic Efficacy (Millivolts) (int)
        # 18: Branch Type from the post neuron(0 for soma,

        # Compute #1: delaySomaDistance
        touches = touches.withColumn("axional_delay", (
                touches.prop.neuralTransmitterReleaseDelay +
                touches.t.distance_soma / touches.prop.axonalConductionVelocity
            ).cast(T.FloatType())
        )

        # Compute #8-12
        # We ruse a Java UDFs (gauss_rand) which requires using spark.sql
        touches.registerTempTable("cur_touches")
        touches = self.spark.sql(
            "select *,"
            " gauss_rand(0) * prop.gsynVar + prop.gsyn as gsyn, "  # g
            " gauss_rand(0) * prop.uVar + prop.u as u,"     # u
            " gauss_rand(0) * prop.dVar + prop.d as d,"     # d
            " gauss_rand(0) * prop.fVar + prop.f as f,"     # f
            " gauss_rand(0) * prop.dtcVar + prop.dtc as dtc"  # dtc
            " from cur_touches")

        # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
        t = touches.withColumn("synapseType",
                               (F.when(touches.prop.type.substr(0, 1) == F.lit('E'), 100)
                                .otherwise(0)
                                ) + touches.prop._class_i)

        # Select fields
        return t.select(
            t.t.src.alias("pre_gid"),
            t.t.dst.alias("post_gid"),
            t.axional_delay,
            t.t.post_section.alias("post_section"),
            t.t.post_segment.alias("post_segment"),
            t.t.post_offset.alias("post_offset"),
            t.t.pre_section.alias("pre_section"),
            t.t.pre_segment.alias("pre_segment"),
            t.t.pre_offset.alias("pre_offset"),
            "gsyn", "u", "d", "f", "dtc",
            t.synapseType.alias("synapseType"),
            t.n1.morphology_i.alias("morphology"),
            F.lit(0).alias("branch_order_dend"),  # TBD
            t.t.branch_order.alias("branch_order_axon"),
            t.prop.ase.alias("ase"),
            F.lit(0).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
        )


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
