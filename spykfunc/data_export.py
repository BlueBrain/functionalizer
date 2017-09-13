import h5py
import pyspark
from os import path
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .definitions import CellClass
from . import data_loader
from . import utils
from .dataio import morphotool

logger = utils.get_logger(__name__)


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
        self.sc = pyspark.SparkContext.getOrCreate()  # type: pyspark.SparkContext
        self.morphologies = {}
        self.sc.broadcast(self.morphologies)


    def do_export(self, filename="nrn.h5"):
        nrn_filepath = path.join(self.output_path, filename)
        touch_G = self.neuronG

        # In order to sequentially write and not overflood the master, we query and write touches GID per GID
        gids_df = touch_G.vertices.select("id").orderBy("id")
        gids = gids_df.rdd.keys().collect()  # In large cases we can use toLocalIterator()
        _many_files = len(gids) > 10000

        # prepare DF add required fields
        touch_G.join(self.syn_properties_df)


        for i, gid in enumerate(gids):
            if i % 10000 == 0:
                cur_name = nrn_filepath
                if _many_files:
                    cur_name += ".{}".format(i//10000)
                f = h5py.File(cur_name, 'w')

            # The df of the neuron to export
            df = touch_G.find("(n1)-[t]->(n2)").where(F.col("n2.id") == gid).orderBy("n1.id")


            # convert_h5 = make_conversion_udf(self.morphologies, self.morpho_dir)
            # prepared_df = df.select(convert_h5(df.t, df.n1.id, df.n2.syn_class_index))
            # some magic now to extract the array from the DF, one per one or preferably the whole matrix...
            #h5ds = f.create_dataset("a{}".format(gid), (df.count(), 19), numpy.float32)



# Export format
_export_params_schema = T.StructType([
    T.StructField("h5_entry", T.FloatType(), 19),
])

# An UDF to convert every single touch to a line in the H5 file
def make_conversion_udf(_morpho_dict, morpho_dir):
    def to_tuple(touch, gid, post_syn_class_index):
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

        # NOTE: Assuming this order
        synapse_classes_by_index = [CellClass.CLASS_INH, CellClass.CLASS_EXC]
        is_excitatory = synapse_classes_by_index[post_syn_class_index] == CellClass.CLASS_EXC


        morpho_dict = _morpho_dict.value
        if morpho_name in morpho_dict:
            morpho = morpho_dict[morpho_name]
        else:
            morpho = morpho_dict[morpho_name] = Morphology(morpho_dir, morpho_name)


        return (
            gid + 1,
            --axional_delay,       # Neuron.findDelay()
            touch.post_section,
            touch.post_segment,
            touch.post_offset,
            touch.pre_section,
            touch.pre_segment,
            row.pre_offset,
            --conductance,         # g  synapseProperties->classes
            --u_parameter,         # udf params
            --d_syn(depression),
            --f_syn(facilitation),
            --dtc(Decay_Time_Constant),  # also udf[0] dtc_engine
            is_excitatory*100 + post_syn_class_index,
            row.n1.morphology,
            # findBranchOrder(neuron.branch[syn->synapses[0]]), dendrite
            touch.branch_order,
            # ASE
            --branch_type(0-soma)  #  neuron.branchType[syn->synapses[0]]
        )

    return F.udf(to_tuple, _export_params_schema)