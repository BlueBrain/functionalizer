from __future__ import absolute_import

import hashlib
import os
import sparkmanager as sm
import logging
from lazy_property import LazyProperty
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .dataio.cppneuron import NeuronData
from . import schema
from .dataio import touches
from .dataio.morphologies import MorphologyDB
from .dataio.common import Part
from .definitions import MType
from .utils import get_logger, make_slices, to_native_str
from .utils.spark import BroadcastValue, cache_broadcast_single_part
from .utils.filesystem import adjust_for_spark


# Globals
logger = get_logger(__name__)


###################################################################
# Main loader class
###################################################################
class NeuronDataSpark(NeuronData):
    """
    Neuron data loader.
    It inherits from NeuronData (extension type), which has the following C++ fields (accessible from Python):
     - bool globals_loaded
     - int nNeurons
     - vector[string] mtypeVec
     - vector[string] etypeVec
     - vector[string] morphologyVec
     - vector[string] synaClassVec

    Neuron info - Depending on the range
     - NeuronBuffer neurons
    """

    NEURON_COLUMNS_TO_DROP = ['layer', 'position', 'rotation']

    def __init__(self, loader, cache):
        self.neuronDF = None
        self.morphologyDB = None
        self.set_loader(loader)
        self.cache = cache

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache)

    def require_mvd_globals(self):
        if not self.globals_loaded:
            logger.info("Loading global Neuron data...")
            self.load_globals()

    @LazyProperty
    def mTypes(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.mtypeVec)

    @LazyProperty
    def eTypes(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.etypeVec)

    @property
    def morphologies(self):
        self.require_mvd_globals()
        for m in self.morphologyVec:
            yield to_native_str(m)

    @LazyProperty
    def cellClasses(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.synaClassVec)

    # ---
    def load_mvd_neurons_morphologies(self, neuron_filter=None, **kwargs):
        self._load_mvd_neurons(neuron_filter, **kwargs)
        # Morphologies are loaded lazily by the MorphologyDB object
        self.morphologyDB = BroadcastValue(
            MorphologyDB(self._loader.morphology_dir.decode('utf-8'),
                         self.mvdvec_to_dict(self.morphologies))
        )

    # ---
    def _load_mvd_neurons(self, neuron_filter=None):
        self.require_mvd_globals()
        n_neurons = int(self.nNeurons)

        fn = self._loader.mvd_filename
        sha = hashlib.sha256()
        sha.update(os.path.realpath(fn))
        sha.update(str(os.stat(fn).st_size).encode())
        sha.update(str(os.stat(fn).st_mtime).encode())
        digest = sha.hexdigest()[:8]

        logger.info("Total neurons: %d", n_neurons)
        mvd_parquet = os.path.join(self.cache,
                                   "neurons_{:.1f}k_{}.parquet".format(n_neurons / 1000.0, digest))

        if os.path.exists(mvd_parquet):
            logger.info("Loading MVD from parquet")
            mvd = sm.read.parquet(adjust_for_spark(mvd_parquet, local=True)).cache()
            self.layers = tuple(sorted(r.layer for r in mvd.select('layer').distinct().collect()))
            self.neuronDF = F.broadcast(mvd.drop(*self.NEURON_COLUMNS_TO_DROP))
            n_neurons = self.neuronDF.count()  # force materialize
        else:
            logger.info("Building MVD from raw mvd files")
            total_parts = max(sm.defaultParallelism * 4, n_neurons // 10000 + 1)
            logger.debug("Partitions: %d", total_parts)

            # Initial RDD has only the range objects
            neuronRDD = sm.parallelize(range(total_parts), total_parts)

            # LOAD neurons in parallel
            neuronRDD = neuronRDD.flatMap(
                neuron_loader_gen(NeuronData, self._loader.__class__,
                                  self._loader.get_params(),
                                  n_neurons, total_parts, self.mTypes))
            # Create DF
            logger.info("Creating data frame...")
            raw_mvd = sm.createDataFrame(neuronRDD, schema.NEURON_SCHEMA).cache()
            self.layers = tuple(sorted(r.layer for r in raw_mvd.select('layer').distinct().collect()))
            layers = sm.createDataFrame(enumerate(self.layers), schema.LAYER_SCHEMA)

            # Evaluate (build partial NameMaps) and store
            mvd = raw_mvd.join(layers, "layer") \
                         .write.mode('overwrite') \
                         .parquet(adjust_for_spark(mvd_parquet, local=True))
            raw_mvd.unpersist()

            # Mark as "broadcastable" and cache
            self.neuronDF = F.broadcast(
                sm.read.parquet(adjust_for_spark(mvd_parquet)).drop(*self.NEURON_COLUMNS_TO_DROP)
            ).cache()

    # ---
    def _load_h5_morphologies_RDD(self, names, filter=None, total_parts=128):
        """ Load morphologies into a spark RDD

        :deprecated: This function is currently superseded by _init_h5_morphologies
                     which will lazily load morphologies in the workers they are needed
        """
        # Initial with the morpho names
        neuronRDD = sm.parallelize((names[s] for s in make_slices(len(names), total_parts)), total_parts)

        # LOAD morphologies in parallel
        self.morphologyRDD = neuronRDD.flatMap(
            morphology_loader_gen(NeuronData, self._loader.__class__, self._loader.get_params())
        )

    # ---
    def load_touch_parquet(self, *files):
        def compatible(s1: T.StructType, s2: T.StructType) -> bool:
            """Test schema compatibility
            """
            if len(s1.fields) != len(s2.fields):
                return False
            for f1, f2 in zip(s1.fields, s2.fields):
                if f1.name != f2.name:
                    return False
            return True
        files = [adjust_for_spark(f) for f in files]
        have = sm.read.load(files[0]).schema
        try:
            want, = [s for s in schema.TOUCH_SCHEMAS if compatible(s, have)]
        except ValueError:
            logger.error("Incompatible schema of input files")
            raise RuntimeError("Incompatible schema of input files")
        return sm.read.schema(want).parquet(*files)

    # ---
    def load_touch_bin(self, touch_file):
        """ Reads touches directly from Binary file into a Dataframe
        """
        touch_info = touches.TouchInfo(touch_file)
        # Requires the conversion of the np array to dataframe
        logger.error("Binary touches converted to dataframe not implemented yet")
        return touch_info

    def mvdvec_to_df(self, vec, field_names):
        """ Transforms a small string vector into an indexed dataframe.
            The dataframe is immediately cached and broadcasted as single partition
        """
        self.require_mvd_globals()
        return cache_broadcast_single_part(
            sm.createDataFrame(enumerate(vec), schema.indexed_strings(field_names)))

    def mvdvec_to_dict(self, vec):
        """Transform a string vector into a lookup dictionary

        :param vec: the vector to turn into a lookup dictionary
        """
        self.require_mvd_globals()
        return dict(enumerate(vec))

    @LazyProperty
    def sclass_df(self):
        return self.mvdvec_to_df(self.cellClasses, ["sclass_i", "sclass_name"])

    @LazyProperty
    def mtype_df(self):
        return self.mvdvec_to_df(self.mTypes, ["mtype_i", "mtype_name"])

    @LazyProperty
    def etype_df(self):
        return self.mvdvec_to_df(self.eTypes, ["etype_i", "etype_name"])


####################################################################
# Functions to load/convert Neurons, to be executed by the workers.
# These functions are defined at module level so that they can be
#  serialized by Spark without dependencies
###################################################################

# Load a given neuron set
def neuron_loader_gen(data_class, loader_class, loader_params, n_neurons,
                      total_parts, mtypes):
    """
    Generates a loading "map" function, to operate on RDDs of range objects
    The loading function shall return a list (or a  generator) of details compatible with the Neuron schema.

    :param data_class: The class which will hold the loaded data
    :param loader_class: The loader
    :param loader_params: The params of the loader
    :param n_neurons: Total number of neurons, if already know
    :param total_parts: The total number of parts to split the data reading into
    :param mtypeVec: The vector of mTypes
    :return: The loading function
    """

    # Every loader builds the list of MTypes - avoid serialize/deserialize of the more complex struct
    mtype_bc = sm.broadcast([MType(mt) for mt in mtypes])

    if str is not bytes:
        loader_params['mvd_filename'] = loader_params['mvd_filename'].decode('utf-8')
        loader_params['morphology_dir'] = loader_params['morphology_dir'].decode('utf-8')

    def _convert_entry(nrn):
        return (int(nrn[0]),                    # id  (0==schema.NeuronFields["id"], but lets avoid all those lookups
                int(nrn[1]),                    # mtype_index
                # mtype_name,                   # mtype (str no longer in df)
                int(nrn[2]),                    # etype_index
                int(nrn[3]),                    # morphology_index
                int(nrn[4]),                    # syn_class_index
                int(nrn[5]),                    # layer
                [float(x) for x in nrn[6]],     # position
                [float(x) for x in nrn[7]])     # rotation

    def load_neurons_par(part_nr):
        logging.debug("Going to read part %d/%d", part_nr, total_parts)

        # Recreates objects to store subset data, avoiding Spark to serialize them
        da = data_class(nr_neurons=n_neurons)
        da.set_loader(loader_class(**loader_params))

        # Convert part into object to the actual range of rows
        da.load_neurons(Part(part_nr, total_parts))
        # name_accumulator.add(da.nameMap)
        if isinstance(da.neurons[0], tuple):
            return da.neurons

        # Dont alloc a full list, give back generator
        return (_convert_entry(nrn) for nrn in da.neurons)
    return load_neurons_par


# Load given Morphology
# @deprecated - This method is currently superseded by the morphology DB object
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
