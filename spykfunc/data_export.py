import h5py
import pyspark
from os import path
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from .definitions import CellClass
from . import data_loader
from . import utils
from .dataio import morphotool

logger = utils.get_logger(__name__)

_DEBUG = True


def save_parquet(neuronG, output_path=None):
    touches = neuronG.vertices
    output_path = "." if output_path is None else output_path
    logger.info("Dumping touch Dataframe as parquet")
    touches.write.parquet(path.join(output_path, "touches.parquet"))


class Hdf5Exporter(object):
    def __init__(self, neuronG, morpho_dir, recipe, syn_properties, output_path=None):
        if not morphotool:
            raise RuntimeError("Can't export to .h5. Morphotool not available")

        self.neuronG = neuronG
        self.output_path = "." if output_path is None else output_path
        self.morpho_dir = morpho_dir
        self.recipe = recipe
        self.syn_properties_df = syn_properties

        # Broadcast an empty dict to hold morphologies
        # Each worker will fill it as required, no communication incurred
        self.spark = SparkSession.builder.getOrCreate()
        self.sc = self.spark.sparkContext  # type: pyspark.SparkContext
        self.morphologies = {}
        self.sc.broadcast(self.morphologies)


    def do_export(self, filename="nrn.h5"):
        nrn_filepath = path.join(self.output_path, filename)
        touch_G = self.neuronG.find("(n1)-[t]->(n2)")

        # In order to sequentially write and not overflood the master, we query and write touches GID per GID
        gids_df = self.neuronG.vertices.select("id").orderBy("id")
        gids = gids_df.rdd.keys().collect()  # In large cases we can use toLocalIterator()
        _many_files = len(gids) > 10000

        # prepare DF - add required fields
        p_df = self.syn_properties_df.select(F.struct("*").alias("prop"))
        touches = touch_G.join(p_df, ((touch_G.n1.syn_class_index == p_df.prop.fromSClass_i) &
                                      (touch_G.n2.syn_class_index == p_df.prop.toSClass_i)))

        touches = self.compute_additional_h5_fields(touches)


        # DBG
        touches.show()
        return


        if _DEBUG:
            gids = [1]

        for i, gid in enumerate(gids):
            if i % 10000 == 0:
                cur_name = nrn_filepath
                if _many_files:
                    cur_name += ".{}".format(i//10000)
                #f = h5py.File(cur_name, 'w')

            # The df of the neuron to export
            df = touches.where(F.col("n2.id") == gid).orderBy("n1.id")
            df.show()




            convert_h5 = make_conversion_udf(self.morphologies, self.morpho_dir)
            prepared_df = df.select(convert_h5(df.n1.id, df.axional_delay, df.t, df.prop, df.n2.syn_class_index))
            # some magic now to extract the array from the DF, one per one or preferably the whole matrix...
            #h5ds = f.create_dataset("a{}".format(gid), (df.count(), 19), numpy.float32)


    def compute_additional_h5_fields(self, touches):

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
        touches = touches.withColumn("axional_delay",
                                     touches.prop.neuralTransmitterReleaseDelay +
                                     touches.t.distance_soma / touches.prop.axonalConductionVelocity)

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
        touches = touches.withColumn("synapseType",
                                     (F.when(touches.prop.type.substr(0, 1) == F.lit('E'), 100)
                                      .otherwise(0)
                                      ) + touches.prop._class_i)

        return touches.select(
            "gid",
            "axional_delay",
            "t.post_section",
            "t.post_segment",
            "t.post_offset",
            "t.pre_section",
            "t.pre_segment",
            "t.pre_offset",
            "gsyn", "u", "d", "f", "dtc",
            "synapseType",
            "n1.morphology_i",
            #"t.branch_order",  # Branch order of dend section (morpho?)
            "t.branch_order",  # Branch of axon
            "prop.ase",
            #F.lit(0),          # the type of the branch, 0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite (morpho?)
        )

        # morpho_dict = _morpho_dict.value
        # if morpho_name in morpho_dict:
        #     morpho = morpho_dict[morpho_name]
        # else:
        #     morpho = morpho_dict[morpho_name] = Morphology(morpho_dir, morpho_name)
