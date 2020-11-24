"""Filters to add properties to synapses
"""
from collections import defaultdict, OrderedDict
from typing import List
import fnmatch
import numpy as np

import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc import schema
from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

logger = get_logger(__name__)


class SynapseProperties(DatasetOperation):
    """Assign synapse properties

    This "filter" augments touches with properties of synapses by adding
    the fields

    - `gsyn` following a Gamma-distribution
    - `d` following a Gamma-distribution
    - `f` following a Gamma-distribution
    - `u` following a truncated Normal-distribution
    - `dtc` following a truncated Normal-distribution
    - `nrrp` following a Poisson-distribution

    - `gsynSRSF`, taken verbatim from the recipe
    - `uHillCoefficient`, also taken verbatim  from the recipe

    as specified by the `SynapsesClassification` part of the recipe.

    To draw from the distributions, a seed derived from the `synapseSeed`
    in the recipe is used.

    The internal implementation uses Pandas UDFs calling into
    Cython/Highfive for the random number generation.
    """

    _checkpoint = True

    _columns = [
        (None, "gsyn"),
        (None, "u"),
        (None, "d"),
        (None, "f"),
        (None, "dtc"),
        (None, "nrrp"),

        ("distance_soma", "axonal_delay"),

        (None, "synapseType"),
        (None, "synapse_type_id"),
    ]

    def __init__(self, recipe, source, target, morphos):
        self.seed = recipe.seeds.synapseSeed
        logger.info("Using seed %d for synapse properties", self.seed)

        self.rules = self.convert_rules(
            source, target, recipe.synapse_properties.rules
        )
        self.properties = self.convert_properties(
            recipe.synapse_properties.rules,
            recipe.synapse_properties.classes
        )

        if "gsynSRSF" in self.properties.columns:
            self._columns.append((None, "gsynSRSF"))
        if "uHillCoefficient" in self.properties.columns:
            self._columns.append((None, "uHillCoefficient"))

    def apply(self, circuit):
        """Add properties to the circuit
        """
        from spykfunc.synapse_properties import compute_additional_h5_fields

        extended_touches = compute_additional_h5_fields(
            circuit.df,
            circuit.reduced,
            self.rules,
            self.properties,
            self.seed
        )
        return extended_touches

    @staticmethod
    def convert_properties(rules, classes):
        """Merges synapse class assignment and properties
        """
        prop_df = _load_from_recipe(classes, schema.SYNAPSE_PROPERTY_SCHEMA, trim=True) \
            .withColumnRenamed("_i", "_prop_i")
        class_df = _load_from_recipe(rules, schema.SYNAPSE_CLASSIFICATION_SCHEMA) \
            .withColumnRenamed("_i", "_class_i")

        # These are small DF, we coalesce to 1 so the sort doesnt require shuffle
        class_df = class_df.coalesce(1).sort("type")
        prop_df = prop_df.coalesce(1).sort("id")
        merged_props = class_df.join(prop_df, prop_df.id == class_df.type, "left").cache()
        n_syn_prop = merged_props.count()
        logger.info("Found {} synapse property entries".format(n_syn_prop))

        merged_props = F.broadcast(merged_props.checkpoint())
        return merged_props

    @staticmethod
    def convert_rules(source, target, rules):
        """Expands the synapse classification rules
        """
        # shorthand
        values = dict()
        reverses = dict()
        shape = []

        for direction, population in (("from", source), ("to", target)):
            mtypes = population.mtypes
            etypes = population.etypes
            cclasses = population.cell_classes

            syn_mtype_rev = {name: i for i, name in enumerate(mtypes)}
            syn_etype_rev = {name: i for i, name in enumerate(etypes)}
            syn_sclass_rev = {name: i for i, name in enumerate(cclasses)}

            shape += [len(syn_mtype_rev), len(syn_etype_rev), len(syn_sclass_rev)]

            values[direction] = OrderedDict((("MType", mtypes),
                                             ("EType", etypes),
                                             ("SClass", cclasses)))
            reverses[direction] = {"MType": syn_mtype_rev,
                                   "EType": syn_etype_rev,
                                   "SClass": syn_sclass_rev}

        not_covered = max(r._i for r in rules) + 1

        prop_rule_matrix = np.full(
            # Our 6-dim matrix
            fill_value=not_covered,
            shape=shape,
            dtype="uint16"
        )

        # Iterate for all rules, expanding * as necessary
        # We keep rule definition order as required
        for rule in rules:
            selectors = [None] * 6
            for i, direction in enumerate(("from", "to")):
                expanded_names = defaultdict(dict)
                field_to_values = values[direction]
                field_to_reverses = reverses[direction]
                for j, field_t in enumerate(field_to_values):
                    field_name = direction + field_t
                    field_val = getattr(rule, field_name)
                    if field_val in (None, "*"):
                        # Slice(None) is numpy way for "all" in that dimension (same as colon)
                        selectors[i*3+j] = [slice(None)]
                    else:
                        # Check if expansion was cached
                        val_matches = expanded_names[field_t].get(field_val)
                        if not val_matches:
                            # Expand it
                            val_matches = expanded_names[field_t][field_val] = \
                                fnmatch.filter(field_to_values[field_t], field_val)
                            if len(val_matches) == 0:
                                logger.warn(
                                    f"Synapse classification can't match {field_t}='{field_val}'"
                                )

                        # Convert to int
                        selectors[i*3+j] = [field_to_reverses[field_t][v] for v in val_matches]

            # The rule might have been expanded, so now we apply all of them
            # Assign to the matrix.
            for m1 in selectors[0]:
                for e1 in selectors[1]:
                    for s1 in selectors[2]:
                        for m2 in selectors[3]:
                            for e2 in selectors[4]:
                                for s2 in selectors[5]:
                                    prop_rule_matrix[m1, e1, s1, m2, e2, s2] = rule._i

        if not_covered in prop_rule_matrix:
            logger.warning("Synapse classification does not cover all values!")

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


def _load_from_recipe(recipe_group, group_schema, *, trim: bool = False):
    if trim:
        fields = []
        for field in reversed(list(group_schema.fields)):
            haves = [hasattr(entry, field.name) for entry in recipe_group]
            if all(haves):
                fields.append(field)
            else:
                logger.warning(
                    "Field %s not present in all rules, skipping conversion",
                    field.name
                )
        group_schema = T.StructType(fields)
    data = [tuple(cast_in_eq_py_t(getattr(entry, field.name), field.dataType)
                  for field in group_schema)
            for entry in recipe_group]
    rdd = sm.parallelize(data)
    return rdd.toDF(group_schema)
