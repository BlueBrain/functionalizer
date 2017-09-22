from __future__ import print_function, absolute_import
import os
import copy

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .definitions import MType
from .dataio.cppneuron import NeuronData
from . import schema
from .dataio import touches
from .dataio.common import Part
from .utils.spark_udef import DictAccum
from .utils import get_logger, make_slices

import logging
logger = get_logger(__name__)


###################################################################
# Main loader class
###################################################################
class NeuronDataSpark(NeuronData):
    """
    Neuron data loader.
    It inherits from NeuronData (extension type), which has the following C++ fields (accessible from Python):
     - vector[string] mtypeVec
     - vector[string] etypeVec
     - vector[string] synaClassVec

    Neuron info - Depending on the range
     - NeuronBuffer neurons
     - vector[string] neuronNames
     - object nameMap
    """

    def __init__(self, loader, spark_session):
        self._spark = spark_session
        self._sc = self._spark.sparkContext
        self.neuronDF = None
        self.set_loader(loader)

        if not os.path.isdir(".cache"):
            os.makedirs(".cache")

    # ---
    def load_mvd_neurons_morphologies(self, neuron_filter=None, **kwargs):
        self._load_mvd_neurons(neuron_filter, **kwargs)
        # Dont load morphologies in the begginging
        # if morphotool:
        #     self._load_h5_morphologies(self.nameMap.keys(), neuron_filter, **kwargs)
        self.morphologyRDD = None

    # ---
    def _load_mvd_neurons(self, neuron_filter=None, total_parts=None):
        # Neuron data which stays in client
        logger.info("Loading global Neuron data...")
        self.load_globals()
        n_neurons = int(self.nNeurons)

        if total_parts is None:
            total_parts = n_neurons // 1000
        if total_parts < 4:
            total_parts = 4
        elif total_parts > 256:
            total_parts = 256

        logger.info("Total neurons: %d", n_neurons)
        logger.debug("Partitions: %d", total_parts)

        mvd_parquet = ".cache/neuronDF{}k.parquet".format(n_neurons//1000)

        if os.path.exists(mvd_parquet):
            logger.info("Loading from MVD parquet")
            mvd = self._spark.read.parquet(mvd_parquet)
            self.neuronDF = F.broadcast(mvd).cache()

        else:
            logger.info("Building MVD from raw mvd files")
            # Initial RDD has only the range objects
            neuronRDD = self._sc.parallelize(range(total_parts), total_parts)

            # LOAD neurons in parallel
            name_accu = self._sc.accumulator({}, DictAccum())
            neuronRDD = neuronRDD.flatMap(
                neuron_loader_gen(NeuronData, self._loader.__class__,
                                  self._loader.get_params(),
                                  n_neurons, total_parts, name_accu, self.mtypeVec))

            # Create DF
            logger.info("Creating data frame...")
            # Mark as "broadcastable" and cache
            self.neuronDF = F.broadcast(
                self._spark.createDataFrame(neuronRDD, schema.NEURON_SCHEMA)
            ).cache()

            # Evaluate to build partial NameMaps
            self.neuronDF.write.mode('overwrite').parquet(mvd_parquet)

            # Then we set the global name map
            self.set_name_map(name_accu.value)

    # ---
    def _load_h5_morphologies(self, names, filter=None, total_parts=128):
        """ Load morphologies into a spark RDD
        """
        # Initial with the morpho names
        neuronRDD = self._sc.parallelize((names[s] for s in make_slices(len(names), total_parts)), total_parts)

        # LOAD morphologies in parallel
        self.morphologyRDD = neuronRDD.flatMap(
            morphology_loader_gen(NeuronData, self._loader.__class__, self._loader.get_params())
        )

    # ---
    def load_touch_parquet(self, files):
        logger.info("Loading parquets...")
        if isinstance(files, str):
            # file string accepts wildcards
            self.touchDF = self._load_touch_parquet(files)
        else:
            if not files:
                raise Exception("Please provide a non-empty file list")

            self.touchDF = self._load_touch_parquet(files[0])
            for f in files[1:]:
                self.touchDF.union(self.load_touch_parquet(f))

        return self.touchDF

    # ---
    def _load_touch_parquet(self, f):
        return self._spark.read.schema(schema.TOUCH_SCHEMA).parquet(f)

    # ---
    def load_touch_bin(self, touch_file):
        """ Reads touches directly from Binary file into a Dataframe
        """
        touch_info = touches.TouchInfo(touch_file)
        # Requires the conversion of the np array to dataframe
        logger.error("Binary touches converted to dataframe not implemented yet")
        return touch_info

    # ----
    def load_synapse_properties_and_classification(self, recipe):
        """Loader for SynapsesProperties
        """
        # Prop requires mapping from str to int ids
        vec_reverse = {name: i for i, name in enumerate(self.synaClassVec)}
        for syn_prop in recipe.synapse_properties:
            syn_prop.fromSClass_i = vec_reverse[syn_prop.fromSClass]
            syn_prop.toSClass_i = vec_reverse[syn_prop.toSClass]

        prop_df = _load_from_recipe(recipe.synapse_properties, schema.SYNAPSE_PROPERTY_SCHEMA, self._sc)\
            .withColumnRenamed("_i", "_prop_i")
        class_df = _load_from_recipe(recipe.synapse_classification, schema.SYNAPSE_CLASS_SCHEMA, self._sc)\
            .withColumnRenamed("_i", "_class_i")

        return F.broadcast(prop_df.join(F.broadcast(class_df), prop_df.type == class_df.id).cache())


#######################
# Generic loader
#######################

def _load_from_recipe(recipe_group, group_schema, spark_context=None):
    sc = spark_context or SparkContext.getOrCreate()   # type: SparkContext

    f_names = list(group_schema.names)
    rdd = sc.parallelize([tuple(getattr(entry, name)
                                for name in f_names)
                          for entry in recipe_group])
    return rdd.toDF(group_schema)


####################################################################
# Functions to load/convert Neurons, to be executed by the workers.
# These functions are defined at module level so that they can be
#  serialized by Spark without dependencies
###################################################################

# Load a given neuron set
def neuron_loader_gen(data_class, loader_class, loader_params, n_neurons,
                      total_parts, name_accumulator, mtypeVec):
    """
    Generates a loading "map" function, to operate on RDDs of range objects
    The loading function shall return a list (or a  generator) of details compatible with the Neuron schema.

    :param data_class: The class which will hold the loaded data
    :param loader_class: The loader
    :param loader_params: The params of the loader
    :param n_neurons: Total number of neurons, if already know
    :param total_parts: The total number of parts to split the data reading into
    :param name_accumulator: The name accumulator
    :param mtypeVec: The vector of mTypes
    :return: The loading function
    """
    def _convert_entry(nrn, name, mtype_name, layer):
        return (int(nrn[0]),                    # id  (0==schema.NeuronFields["id"], but lets avoid all those lookups
                int(nrn[1]),                    # morphology_index
                mtype_name,                     # morphology
                int(nrn[2]),                    # electrophysiology
                int(nrn[3]),                    # syn_class_index
                [float(x) for x in nrn[4]],     # position
                [float(x) for x in nrn[5]],     # rotation
                name,
                layer)

    def load_neurons_par(part_nr):
        logging.debug("Gonna read part %d/%d", part_nr, total_parts)

        # Every loader builds the list of MTypes - avoid serialize/deserialize of the more complex struct
        mtypes = [MType(mtype) for mtype in mtypeVec]

        # Recreates objects to store subset data, avoiding Spark to serialize them
        da = data_class(nr_neurons=n_neurons)
        da.set_loader(loader_class(**loader_params))

        # Convert part into object to the actual range of rows
        da.load_neurons(Part(part_nr, total_parts))
        logging.debug("Name map: %s", da.nameMap)
        name_accumulator.add(da.nameMap)
        if isinstance(da.neurons[0], tuple):
            return da.neurons
        name_it = iter(da.neuronNames)

        # Dont alloc a full list, give back generator
        return (_convert_entry(nrn, next(name_it), mtypes[nrn[1]].name, mtypes[nrn[1]].layer) for nrn in da.neurons)

    return load_neurons_par


# Load given Morphology
# TODO: Can we avoid loading morphologies (at all) or load them lazily?
def morphology_loader_gen(data_class, loader_class, loader_params):
    """
    Generates a loading function for morphologies, returning the MorphoTree objects
    :return: A loader of morphologies
    """
    # Dont break whole module if morphotool is not available
    from morphotool import MorphoReader

    def load_morphology_par(morpho_names):
        logging.debug("Gonna read %d morphologies, starting at %s",
                      len(morpho_names), morpho_names[0] if morpho_names else '<empty>')
        return [
            MorphoReader(os.path.join(loader_params["morphology_dir"], name + ".h5")).create_morpho_tree()
            for name in morpho_names
        ]

    return load_morphology_par


# Touches loader
def touches_loader_gen(data_class, loader_class, loader_params):
    """
    Generates a function loading touches, possibly depending on a range/other constrains (e.g. for single neuron).
    :return: Touch loader function for RDDs
    """
    # I guess this generator is gonna be called every time the RDD is to be constructed (unless cached)
    def load_touches_par(neuron_id):
        logging.debug("Gonna read touches belonging to neuron %d", neuron_id)
        return ()

    return load_touches_par
