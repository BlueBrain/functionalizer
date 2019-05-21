"""A default filter plugin
"""
from collections import defaultdict, OrderedDict
import fnmatch
import itertools
import numpy as np

import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc import schema
from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty, _REQUIRED_
from spykfunc.utils import get_logger

from . import Seeds

logger = get_logger(__name__)


class SynapsesReposition(GenericProperty):
    """Class representing rules to shift synapse positions"""

    _supported_attrs = {'fromMType', 'toMType', 'type'}
    fromMType = None
    toMType = None
    type = ""


class SynapsesProperty(GenericProperty):
    """Class representing a Synapse property"""

    fromSClass = None  # None -> no filter (equiv to *wildcard)
    toSClass = None
    fromMType = None
    toMType = None
    fromEType = None
    toEType = None
    type = ""
    neuralTransmitterReleaseDelay = 0.1
    axonalConductionVelocity = 0.00333  # TODO: or 300?
    _supported_attrs = [k for k in locals().keys()
                        if not k.startswith("_")]


class SynapsesClassification(GenericProperty):
    """Class representing a Synapse Classification"""

    id = ""
    gsyn = _REQUIRED_
    gsynSD = _REQUIRED_
    nsyn = 0.
    nsynSD = 0.
    dtc = _REQUIRED_
    dtcSD = _REQUIRED_
    u = _REQUIRED_
    uSD = _REQUIRED_
    d = _REQUIRED_
    dSD = _REQUIRED_
    f = _REQUIRED_
    fSD = _REQUIRED_
    nrrp = 0
    _supported_attrs = [k for k in locals().keys()
                        if not k.startswith("_")]

    # v5 fields were sufixed by Var instead of SD
    _map_attrs = {name + "Var": name + "SD" for name in
                  ["gsyn", "nsyn", "dtc", "u", "d", "f"]}
    _warn_missing_attrs = ["nrrp"]


class SynapseProperties(DatasetOperation):
    """Assign synapse properties

    This "filter" augments touches with properties of synapses by

    * shifting the post-section of synapses for ChC and SpAA cells to the
      soma according to the `SynapsesReposition` rules of the recipe.
    * adding the fields

      - `gsyn` following a Gamma-distribution,
      - `d` following a Gamma-distribution,
      - `f` following a Gamma-distribution,
      - `u` following a truncated Normal-distribution,
      - `dtc` following a truncated Normal-distribution,
      - `nrrp` following a Poisson-distribution

      as specified by the `SynapsesClassification` part of the recipe.

    To draw from the distributions, a seed derived from the `synapseSeed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = True
    _morphologies = True

    def __init__(self, recipe, morphos, stats):
        self.seed = Seeds.load(recipe.xml).synapseSeed
        logger.info("Using seed %d for synapse properties", self.seed)

        self.properties = list(
            recipe.load_group(
                recipe.xml.find("SynapsesProperties"),
                SynapsesProperty
            )
        )
        self.reposition = list(
            recipe.load_group(
                recipe.xml.find("SynapsesReposition"),
                SynapsesReposition,
                required=False
            )
        )
        self.classification = list(
            recipe.load_group(
                recipe.xml.find("SynapsesClassification"),
                SynapsesClassification
            )
        )

    def apply(self, circuit):
        """Add properties to the circuit
        """
        from spykfunc.synapse_properties import patch_ChC_SPAA_cells
        from spykfunc.synapse_properties import compute_additional_h5_fields

        reposition = self.convert_reposition(circuit)
        classification = self.convert_classification(circuit)
        properties = self.convert_properties(circuit)

        if self._morphologies:
            circuit.df = patch_ChC_SPAA_cells(circuit.df,
                                              circuit.morphologies,
                                              reposition)

        extended_touches = compute_additional_h5_fields(
            circuit.df,
            circuit.reduced,
            classification,
            properties,
            self.seed
        )
        return extended_touches


    def convert_properties(self, circuit, map_ids=False):
        """Loader for SynapsesProperties
        """
        prop_df = _load_from_recipe_ds(self.properties, schema.SYNAPSE_PROPERTY_SCHEMA) \
            .withColumnRenamed("_i", "_prop_i")
        class_df = _load_from_recipe_ds(self.classification, schema.SYNAPSE_CLASS_SCHEMA) \
            .withColumnRenamed("_i", "_class_i")

        if map_ids:
            # Mapping entities from str to int ids
            # Case 1: SClass
            sclass_df = circuit.data.sclass_df
            prop_df = prop_df\
                .join(sclass_df.toDF("fromSClass_i", "fromSClass"), "fromSClass", "left_outer")\
                .join(sclass_df.toDF("toSClass_i", "toSClass"), "toSClass", "left_outer")

            # Case 2: Mtype
            mtype_df = circuit.data.mtype_df
            prop_df = prop_df\
                .join(mtype_df.toDF("fromMType_i", "fromMType"), "fromMType", "left_outer")\
                .join(mtype_df.toDF("toMType_i", "toMType"), "toMType", "left_outer")

            # Case 3: Etype
            etype_df = circuit.data.etype_df
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

    def convert_reposition(self, circuit):
        """Loader for pathways that need synapses to be repositioned
        """
        mtype = circuit.morphology_types
        mtype_rev = {name: i for i, name in enumerate(mtype)}

        paths = []
        for shift in self.reposition:
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

    def convert_classification(self, circuit):
        """Loader for SynapsesProperties
        """
        # shorthand
        mtypes = circuit.morphology_types
        etypes = circuit.electrophysiology_types
        cclasses = circuit.cell_classes
        syn_class_rules = self.properties
        syn_mtype_rev = {name: i for i, name in enumerate(mtypes)}
        syn_etype_rev = {name: i for i, name in enumerate(etypes)}
        syn_sclass_rev = {name: i for i, name in enumerate(cclasses)}

        prop_rule_matrix = np.empty(
            # Our 6-dim matrix
            shape=(len(syn_mtype_rev), len(syn_etype_rev), len(syn_sclass_rev),
                   len(syn_mtype_rev), len(syn_etype_rev), len(syn_sclass_rev)),
            dtype="uint16"
        )

        expanded_names = defaultdict(dict)
        field_to_values = OrderedDict((("MType", mtypes),
                                       ("EType", etypes),
                                       ("SClass", cclasses)))
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


def _load_from_recipe_ds(recipe_group, group_schema):
    rdd = sm.parallelize(_load_from_recipe(recipe_group, group_schema))
    return rdd.toDF(group_schema)
