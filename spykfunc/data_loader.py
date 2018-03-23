from __future__ import absolute_import

from lazy_property import LazyProperty
import os
import numpy as np
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .dataio.cppneuron import NeuronData
from . import schema
from .dataio import touches
from .dataio.common import Part
from .definitions import MType
from .utils.spark_udef import DictAccum
from .utils import get_logger, make_slices, to_native_str
from collections import defaultdict, OrderedDict
import fnmatch

import sparkmanager as sm

import logging

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
     - object nameMap
    """

    def __init__(self, loader):
        self.neuronDF = None
        self.set_loader(loader)

        if not os.path.isdir(".cache"):
            os.makedirs(".cache")

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

    @LazyProperty
    def cellClasses(self):
        self.require_mvd_globals()
        return tuple(to_native_str(a) for a in self.synaClassVec)

    # ---
    def load_mvd_neurons_morphologies(self, neuron_filter=None, **kwargs):
        self._load_mvd_neurons(neuron_filter, **kwargs)
        # Dont load morphologies in the begginging
        # if morphotool:
        #     self._load_h5_morphologies(self.nameMap.keys(), neuron_filter, **kwargs)
        self.morphologyRDD = None

    # ---
    def _load_mvd_neurons(self, neuron_filter=None):
        self.require_mvd_globals()
        n_neurons = int(self.nNeurons)

        logger.info("Total neurons: %d", n_neurons)
        mvd_parquet = ".cache/neuronDF{}k.parquet".format(n_neurons / 1000.0)

        if os.path.exists(mvd_parquet):
            logger.info("Loading from MVD parquet")
            mvd = sm.read.parquet(mvd_parquet)
            if 'layer_i' not in mvd.schema.names:
                self.layers = mvd.select('layer').distinct().collect()
                lrdd = sm.parallelize(enumerate(i.layer for i in self.layers))
                layers = lrdd.toDF(schema.LAYER_SCHEMA)
                mvd = mvd.join(layers, "layer")
            self.neuronDF = F.broadcast(mvd).cache()
            n_neurons = self.neuronDF.count()  # Force broadcast to meterialize
            logger.info("Loaded {} neurons from MVD".format(n_neurons))

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
            # Mark as "broadcastable" and cache
            self.neuronDF = F.broadcast(
                sm.createDataFrame(neuronRDD, schema.NEURON_SCHEMA)
                    .where(F.col("id").isNotNull())
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
        neuronRDD = sm.parallelize((names[s] for s in make_slices(len(names), total_parts)), total_parts)

        # LOAD morphologies in parallel
        self.morphologyRDD = neuronRDD.flatMap(
            morphology_loader_gen(NeuronData, self._loader.__class__, self._loader.get_params())
        )

    # ---
    def load_touch_parquet(self, *files):
        return sm.read.schema(schema.TOUCH_SCHEMA).parquet(*files)

    # ---
    def load_touch_bin(self, touch_file):
        """ Reads touches directly from Binary file into a Dataframe
        """
        touch_info = touches.TouchInfo(touch_file)
        # Requires the conversion of the np array to dataframe
        logger.error("Binary touches converted to dataframe not implemented yet")
        return touch_info

    @LazyProperty
    def sclass_df(self):
        self.require_mvd_globals()
        return F.broadcast(sm.parallelize(enumerate(self.cellClasses))
                           .toDF(["sclass_i", "sclass_name"], schema.INT_STR_SCHEMA).cache())

    @LazyProperty
    def mtype_df(self):
        self.require_mvd_globals()
        if not self.globals_loaded:
            raise RuntimeError("Global MVD info not loaded")
        return F.broadcast(sm.parallelize(enumerate(self.mTypes))
                           .toDF(["mtype_i", "mtype_name"], schema.INT_STR_SCHEMA).cache())

    @LazyProperty
    def etype_df(self):
        self.require_mvd_globals()
        if not self.globals_loaded:
            raise RuntimeError("Global MVD info not loaded")
        return F.broadcast(sm.parallelize(enumerate(self.eTypes))
                           .toDF(["etype_i", "etype_name"], schema.INT_STR_SCHEMA).cache())

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
