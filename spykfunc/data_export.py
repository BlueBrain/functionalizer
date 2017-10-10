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
    def __init__(self, morpho_dir, recipe, syn_properties, output_path):
        # if not morphotool:
        #     raise RuntimeError("Can't export to .h5. Morphotool not available")
        self._neuronG = False
        self.touches = None
        self.output_path = output_path
        self.morpho_dir = morpho_dir
        self.recipe = recipe
        self.syn_properties_df = syn_properties

        # Broadcast an empty dict to hold morphologies
        # Each worker will fill it as required, no communication incurred
        # self.morphologies = {}
        # self.sc.broadcast(self.morphologies)

        self.spark = SparkSession.builder.getOrCreate()
        self.sc = self.spark.sparkContext  # type: pyspark.SparkContext

        # Get the concat_bin agg function form the java world
        _j_conc_udaf = self.sc._jvm.spykfunc.udfs.BinaryConcat().apply
        self.concat_bin = utils.wrap_java_udf(self.spark.sparkContext, _j_conc_udaf)

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
    def save_temp(self, neuronG=None, filename="filtered_touches.tmp.parquet"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        touches_parquet = path.join(self.output_path, filename)

        self.touches.write.parquet(touches_parquet, mode="overwrite")
        self.touches = self.spark.read.parquet(touches_parquet)  # break execution plan
        logger.info("Filtered touches temporarily saved to %s", touches_parquet)

    # ---
    def export_parquet(self, neuronG=None, filename="nrn.parquet"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        nrn_filepath = path.join(self.output_path, filename)
        return self.touches.write.mode("overwrite").partitionBy("post_gid").parquet(nrn_filepath)

    # ---
    def export_hdf5(self, neuronG=None, filename="nrn.h5"):
        if neuronG is not None and self._neuronG is not neuronG:
            self.neuronG = neuronG
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        nrn_filepath = path.join(self.output_path, filename)

        n_gids = self.neuronG.vertices.count()
        df = self.touches

        # Massive conversion to binary using 'float2binary' java UDF and 'concat_bin' UDAF
        nrn_vals = df.select(df.post_gid, F.array(*self.nrn_fields_as_float(df)).alias("floatvec") )
        arrays_df = (nrn_vals
                     .selectExpr("post_gid", "float2binary(floatvec) as bin_arr")
                     .groupBy("post_gid")
                     .agg(self.concat_bin("bin_arr").alias("bin_matrix"))
                     )

        # Number of partitions to number of files
        n_partitions = ((n_gids-1)//N_NEURONS_FILE) + 1
        logger.debug("Ordering into {} partitions".format(n_partitions))
        arrays_df = arrays_df.orderBy("post_gid").coalesce(n_partitions)

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

        # Export via partition mapping
        result_files = arrays_df.rdd.mapPartitions(write_hdf5).collect()
        logger.info("Files written: %s", ", ".join(result_files))

    # -----
    def compute_additional_h5_fields(self):
        touch_G = self.neuronG.find("(n1)-[t]->(n2)")

        # prepare DF - add required fields
        p_df = self.syn_properties_df.select(F.struct("*").alias("prop"))
        # # TODO: Synapse properties can also be matchning by MType and/or EType
        touches = touch_G.join(p_df, ((touch_G.n1.syn_class_index == p_df.prop.fromSClass_i) &
                                      (touch_G.n2.syn_class_index == p_df.prop.toSClass_i)))

        # touches = touch_G.join(p_df, ((p_df.prop.fromSClass_i.isNull() | (touch_G.n1.syn_class_index == p_df.prop.fromSClass_i)) &
        #                               (p_df.prop.toSClass_i.isNull() | (touch_G.n2.syn_class_index == p_df.prop.toSClass_i)) &
        #                               (p_df.prop.fromMType_i.isNull() | (touch_G.n1.morphology_i == p_df.prop.fromMType_i)) &
        #                               (p_df.prop.toMType_i.isNull() | (touch_G.n2.morphology_i == p_df.prop.toMType_i)) &
        #                               (p_df.prop.fromEType_i.isNull() | (touch_G.n1.electrophysiology == p_df.prop.fromEType_i)) &
        #                               (p_df.prop.toEType_i.isNull() | (touch_G.n2.electrophysiology == p_df.prop.toEType_i))))

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
        touches.createOrReplaceTempView("cur_touches")
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
