from __future__ import absolute_import

from lazy_property import LazyProperty
import hashlib
import itertools
import os
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .dataio.cppneuron import NeuronData
from . import schema
from .dataio import touches
from .dataio.morphologies import MorphologyDB
from .dataio.common import Part
from .definitions import MType
from .utils.spark_udef import DictAccum
from .utils import get_logger, make_slices, to_native_str
from .utils.spark import BroadcastValue, cache_broadcast_single_part
from .utils.filesystem import adjust_for_spark
from collections import defaultdict, OrderedDict
import fnmatch
import sparkmanager as sm
import logging
import pickle

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
     - vector[string] synaClassVec

    Neuron info - Depending on the range
     - NeuronBuffer neurons
     - vector[string] neuronNames
     - dict nameMap
    """

    def __init__(self, loader, cache):
        self.neuronDF = None
        self.layers = None
        self.set_loader(loader)
        self.morphologies = None
        self.cache = cache

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache)

    def require_mvd_globals(self):
        if not self.globals_loaded:
            logger.info("Loading global Neuron data...")
            self.load_globals()

    @property
    def morphology_names(self):
        return self.nameMap.keys()

    @LazyProperty
    def mTypes(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.mtypeVec)

    @LazyProperty
    def eTypes(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.etypeVec)

    @LazyProperty
    def cellClasses(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.synaClassVec)

    # ---
    def load_mvd_neurons_morphologies(self, neuron_filter=None, **kwargs):
        self._load_mvd_neurons(neuron_filter, **kwargs)
        # Morphologies are loaded lazily by the MorphologyDB object
        self.morphologies = BroadcastValue(MorphologyDB(self._loader.morphology_dir))

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
        namemap_file = os.path.join(self.cache,
                                   "morphos_{:.1f}k_{}.pkl".format(n_neurons / 1000.0, digest))

        if os.path.exists(mvd_parquet):
            logger.info("Loading MVD from parquet")
            mvd = sm.read.parquet(adjust_for_spark(mvd_parquet, local=True)).cache()
            self.layers = mvd.select('layer').distinct().rdd.keys().collect()
            self.neuronDF = F.broadcast(mvd)
            n_neurons = self.neuronDF.count()  # force materialize
            name_accu = pickle.load(open(namemap_file, 'rb'))
            self.set_name_map(name_accu)
        else:
            logger.info("Building MVD from raw mvd files")
            total_parts = max(sm.defaultParallelism * 4, n_neurons // 10000 + 1)
            logger.debug("Partitions: %d", total_parts)

            # Initial RDD has only the range objects
            neuronRDD = sm.parallelize(range(total_parts), total_parts)

            # LOAD neurons in parallel
            name_accu = sm.accumulator({}, DictAccum())
            neuronRDD = neuronRDD.flatMap(
                neuron_loader_gen(NeuronData, self._loader.__class__,
                                  self._loader.get_params(),
                                  n_neurons, total_parts, name_accu, self.mTypes))
            # Create DF
            logger.info("Creating data frame...")

            raw_mvd = sm.createDataFrame(neuronRDD, schema.NEURON_SCHEMA).cache()
            self.layers = raw_mvd.select('layer').distinct().rdd.keys().collect()
            layer_rdd = sm.parallelize(enumerate(self.layers), 1)
            layer_df = F.broadcast(layer_rdd.toDF(schema.LAYER_SCHEMA))

            # Evaluate (build partial NameMaps) and store
            mvd = raw_mvd.join(layer_df, "layer") \
                         .write.mode('overwrite') \
                         .parquet(adjust_for_spark(mvd_parquet, local=True))
            raw_mvd.unpersist()

            # Mark as "broadcastable" and cache
            self.neuronDF = F.broadcast(sm.read.parquet(adjust_for_spark(mvd_parquet))).cache()

            # Then we set the global name map
            pickle.dump(name_accu.value, open(namemap_file, 'wb'))
            self.set_name_map(name_accu.value)

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
        files = [adjust_for_spark(f) for f in files]
        return sm.read.schema(schema.TOUCH_SCHEMA).parquet(*files)

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

    @LazyProperty
    def sclass_df(self):
        return self.mvdvec_to_df(self.cellClasses, ["sclass_i", "sclass_name"])

    @LazyProperty
    def mtype_df(self):
        return self.mvdvec_to_df(self.mTypes, ["mtype_i", "mtype_name"])

    @LazyProperty
    def etype_df(self):
        return self.mvdvec_to_df(self.eTypes, ["etype_i", "etype_name"])

    # ----
    def load_synapse_properties_and_classification(self, recipe, map_ids=False):
        """Loader for SynapsesProperties
        """
        prop_df = _load_from_recipe_ds(recipe.synapse_properties, schema.SYNAPSE_PROPERTY_SCHEMA) \
            .withColumnRenamed("_i", "_prop_i")
        class_df = _load_from_recipe_ds(recipe.synapse_classification, schema.SYNAPSE_CLASS_SCHEMA) \
            .withColumnRenamed("_i", "_class_i")

        if map_ids:
            # Mapping entities from str to int ids
            # Case 1: SClass
            sclass_df = self.sclass_df
            prop_df = prop_df\
                .join(sclass_df.toDF("fromSClass_i", "fromSClass"), "fromSClass", "left_outer")\
                .join(sclass_df.toDF("toSClass_i", "toSClass"), "toSClass", "left_outer")

            # Case 2: Mtype
            mtype_df = self.mtype_df
            prop_df = prop_df\
                .join(mtype_df.toDF("fromMType_i", "fromMType"), "fromMType", "left_outer")\
                .join(mtype_df.toDF("toMType_i", "toMType"), "toMType", "left_outer")

            # Case 3: Etype
            etype_df = self.etype_df
            prop_df = prop_df\
                .join(etype_df.toDF("fromEType_i", "fromEType"), "fromEType", "left_outer")\
                .join(etype_df.toDF("toEType_i", "toEType"), "toEType", "left_outer")

        # These are small DF, we coalesce to 1 so the sort doesnt require shuffle
        prop_df = prop_df.coalesce(1).sort("type")
        class_df = class_df.coalesce(1).sort("id")
        merged_props = prop_df.join(class_df, prop_df.type == class_df.id, "left").cache()
        n_syn_prop = merged_props.count()
        logger.info("Found {} synpse property entries".format(n_syn_prop))

        merged_props = F.broadcast(merged_props.checkpoint())
        return merged_props

    def load_synapse_reposition_pathways(self, recipe):
        """Loader for pathways that need synapses to be repositioned
        """
        mtype = self.mTypes
        mtype_rev = {name: i for i, name in enumerate(mtype)}

        paths = []
        for shift in recipe.synapse_reposition:
            src = mtype_rev.values()
            dst = mtype_rev.values()
            if shift.type != 'AIS':
                continue
            if shift.fromMType:
                src = [mtype_rev[m] for m in fnmatch.filter(mtype, shift.fromMType)]
            if shift.toMType:
                dst = [mtype_rev[m] for m in fnmatch.filter(mtype, shift.toMType)]
            paths.extend(itertools.product(src, dst))
        pathways = sm.createDataFrame([((s << 16) | d, True) for s, d in paths], schema.SYNAPSE_REPOSITION_SCHEMA)
        return F.broadcast(pathways)

    def load_touch_rules_matrix(self, recipe):
        """Loader for TouchRules
        """
        self.require_mvd_globals()

        layers = self.layers
        layers_rev = {layer: i for i, layer in enumerate(layers)}

        mtype = self.mTypes
        mtype_rev = {name: i for i, name in enumerate(mtype)}

        touch_rule_matrix = np.zeros(
            shape=(len(layers_rev), len(layers_rev),
                   len(mtype_rev), len(mtype_rev), 2
                   ),
            dtype="uint8"
        )

        for rule in recipe.touch_rules:
            l1 = rule.fromLayer if rule.fromLayer else slice(None)
            l2 = rule.toLayer if rule.toLayer else slice(None)
            t1s = [slice(None)]
            t2s = [slice(None)]
            if rule.fromMType:
                t1s = [mtype_rev[m] for m in fnmatch.filter(mtype, rule.fromMType)]
            if rule.toMType:
                t2s = [mtype_rev[m] for m in fnmatch.filter(mtype, rule.toMType)]
            r = (0 if rule.type == "soma" else 1) if rule.type else slice(None)

            for t1 in t1s:
                for t2 in t2s:
                    touch_rule_matrix[l1, l2, t1, t2, r] = 1
        return touch_rule_matrix

    # ---
    def load_synapse_prop_matrix(self, recipe):
        """Loader for SynapsesProperties
        """
        self.require_mvd_globals()
        syn_class_rules = recipe.synapse_properties
        syn_mtype_rev = {name: i for i, name in enumerate(self.mTypes)}
        syn_etype_rev = {name: i for i, name in enumerate(self.eTypes)}
        syn_sclass_rev = {name: i for i, name in enumerate(self.cellClasses)}

        prop_rule_matrix = np.empty(
            # Our 6-dim matrix
            shape=(len(syn_mtype_rev), len(syn_etype_rev), len(syn_sclass_rev),
                   len(syn_mtype_rev), len(syn_etype_rev), len(syn_sclass_rev)),
            dtype="uint16"
        )

        expanded_names = defaultdict(dict)
        field_to_values = OrderedDict((("MType", self.mTypes),
                                       ("EType", self.eTypes),
                                       ("SClass", self.cellClasses)))
        field_to_reverses = {"MType": syn_mtype_rev,
                             "EType": syn_etype_rev,
                             "SClass": syn_sclass_rev}

        # Iterate for all rules, expanding * as necessary
        # We keep rule definition order as required
        for rule in syn_class_rules:
            selectors = [None] * 6
            for i, direction in enumerate(("from", "to")):
                for j, (field_t, vals) in enumerate(field_to_values.items()):
                    field_name = direction + field_t
                    field_val = rule[field_name]
                    if field_val in (None, "*"):
                        # Slice(None) is numpy way for "all" in that dimension (same as colon)
                        selectors[i*3+j] = [slice(None)]
                    elif "*" in field_val:
                        # Check if expansion was cached
                        val_matches = expanded_names[field_t].get(field_val)
                        if not val_matches:
                            # Expand it
                            val_matches = expanded_names[field_t][field_val] = \
                                fnmatch.filter(field_to_values[field_t], field_val)
                        # Convert to int
                        selectors[i*3+j] = [field_to_reverses[field_t][v] for v in val_matches]
                    else:
                        # Convert to int. If the val is not found (e.g. morpho not in mvd) we must skip (use empty list)
                        selector_val = field_to_reverses[field_t].get(field_val)
                        selectors[i*3+j] = [selector_val] if selector_val is not None else []

            # The rule might have been expanded, so now we apply all of them
            # Assign to the matrix.
            for m1 in selectors[0]:
                for e1 in selectors[1]:
                    for s1 in selectors[2]:
                        for m2 in selectors[3]:
                            for e2 in selectors[4]:
                                for s2 in selectors[5]:
                                    prop_rule_matrix[m1, e1, s1, m2, e2, s2] = rule._i

        return prop_rule_matrix


#######################
# Generic loader
#######################

def _load_from_recipe_ds(recipe_group, group_schema):
    rdd = sm.parallelize(_load_from_recipe(recipe_group, group_schema))
    return rdd.toDF(group_schema)


_spark_t_to_py = {
    T.ShortType: int,
    T.IntegerType: int,
    T.LongType: int,
    T.FloatType: float,
    T.DoubleType: float,
    T.StringType: str,
    T.BooleanType: bool
}


def cast_in_eq_py_t(val, spark_t):
    return _spark_t_to_py[spark_t.__class__](val)


def _load_from_recipe(recipe_group, group_schema):
    return [tuple(cast_in_eq_py_t(getattr(entry, field.name), field.dataType)
                  for field in group_schema)
            for entry in recipe_group]


####################################################################
# Functions to load/convert Neurons, to be executed by the workers.
# These functions are defined at module level so that they can be
#  serialized by Spark without dependencies
###################################################################

# Load a given neuron set
def neuron_loader_gen(data_class, loader_class, loader_params, n_neurons,
                      total_parts, name_accumulator, mtypes):
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

    # Every loader builds the list of MTypes - avoid serialize/deserialize of the more complex struct
    mtype_bc = sm.broadcast([MType(mt) for mt in mtypes])

    if str is not bytes:
        loader_params['mvd_filename'] = loader_params['mvd_filename'].decode('utf-8')
        loader_params['morphology_dir'] = loader_params['morphology_dir'].decode('utf-8')

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
        mtypes = mtype_bc.value

        # Recreates objects to store subset data, avoiding Spark to serialize them
        da = data_class(nr_neurons=n_neurons)
        da.set_loader(loader_class(**loader_params))

        # Convert part into object to the actual range of rows
        da.load_neurons(Part(part_nr, total_parts))
        name_accumulator.add(da.nameMap)
        if isinstance(da.neurons[0], tuple):
            return da.neurons
        name_it = iter(da.neuronNames)

        # Dont alloc a full list, give back generator
        return (_convert_entry(nrn, next(name_it), mtypes[nrn[1]].name, mtypes[nrn[1]].layer) for nrn in da.neurons)

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
