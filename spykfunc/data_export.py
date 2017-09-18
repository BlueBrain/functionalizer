import h5py
import pyspark
import os
from os import path
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from . import utils
from .dataio import morphotool

logger = utils.get_logger(__name__)


class NeuronExporter(object):
    def __init__(self, neuronG, morpho_dir, recipe, syn_properties, output_path=None):
        # if not morphotool:
        #     raise RuntimeError("Can't export to .h5. Morphotool not available")

        self.neuronG = neuronG
        self.n_ids = self.neuronG.vertices.count()
        self.output_path = "." if output_path is None else output_path
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

        # Create required / select fields that belong to nrn.h5 spec
        self.touches = self.compute_additional_h5_fields()

    # ---
    def export_parquet(self, filename="nrn.parquet"):
        nrn_filepath = path.join(self.output_path, filename)
        # min_part = int(self.spark.conf.get("spark.sql.shuffle.partitions"))
        # logger.debug("Coalescing to " + str(min_part))
        return self.touches.write.mode("overwrite").partitionBy("gid").parquet(nrn_filepath, compression="gzip")

    # ---
    def export_hdf5(self, filename="nrn.h5"):
        nrn_filepath = path.join(self.output_path, filename)

        # In order to sequentially write and not overflood the master, we query and write touches GID per GID
        gids_df = self.neuronG.vertices.select("id").orderBy("id")
        gids = gids_df.rdd.keys().collect()  # In large cases we can use toLocalIterator()
        _many_files = len(gids) > 10000

        h5store = None

        for i, gid in enumerate(gids):
            if i % 10000 == 0:
                cur_name = nrn_filepath
                if _many_files:
                    cur_name += ".{}".format(i//10000)
                if h5store:
                    h5store.close()
                h5store = h5py.File(cur_name, "w")

            # The df of the neuron to export
            df = self.touches.where(F.col("gid") == gid).orderBy("pre_gid")
            df = self.prepare_df_to_nrn_format(df)

            logger.debug("Writing neuron {}".format(gid))
            data_np = df.toPandas().as_matrix()
            h5store.create_dataset("a{}".format(gid), data=data_np.astype("f4"))

        if h5store:
            h5store.close()

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

        # Compute #0: gid
        touches = touches.withColumn("gid", touches.n1.id + 1)

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
            t.gid.alias("gid"),
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
    def prepare_df_to_nrn_format(df):
        # Select fields and cast to Float
        return df.select(
            df.gid.cast(T.FloatType()).alias("gid"),
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
            df.branch_order.cast(T.FloatType()).alias("branch_order_axon"),
            df.ase.cast(T.FloatType()).alias("ase"),
            df.branch_type.cast(T.FloatType()).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
        )