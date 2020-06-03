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
from spykfunc.recipe import Attribute, GenericProperty
from spykfunc.utils import get_logger

from . import Seeds

logger = get_logger(__name__)


class SynapsesProperty(GenericProperty):
    """Class representing a Synapse property"""

    attributes = [
        Attribute("fromSClass", default="*"),
        Attribute("toSClass", default="*"),
        Attribute("fromMType", default="*"),
        Attribute("toMType", default="*"),
        Attribute("fromEType", default="*"),
        Attribute("toEType", default="*"),
        Attribute("type", default=""),
        Attribute(
            "neuralTransmitterReleaseDelay",
            default=0.1,
            group_default=True
        ),
        Attribute(
            "axonalConductionVelocity",
            default=300.0,
            group_default=True
        ),
    ]

    group_name = "SynapsesProperties"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.type[0] not in "EI":
            raise ValueError(f"Synapse type needs to start with either 'E' or 'I'")


class SynapsesClassification(GenericProperty):
    """Class representing a Synapse Classification"""

    attributes = [
        Attribute(name="id", kind=str),
        Attribute(name="gsyn", kind=float),
        Attribute(name="gsynSD", kind=float, alias="gsynVar"),
        Attribute(name="nsyn", kind=float, default=0.),   # legacy attribute, not actually supported
        Attribute(name="nsynSD", kind=float, alias="nsynVar", default=0.),
        Attribute(name="dtc", kind=float),
        Attribute(name="dtcSD", kind=float, alias="dtcVar"),
        Attribute(name="u", kind=float),
        Attribute(name="uSD", kind=float, alias="uVar"),
        Attribute(name="d", kind=float),
        Attribute(name="dSD", kind=float, alias="dVar"),
        Attribute(name="f", kind=float),
        Attribute(name="fSD", kind=float, alias="fVar"),
        Attribute(name="nrrp", default=0.0, warn=True),
        Attribute(name="gsynSRSF", kind=float, required=False),
        Attribute(name="uHillCoefficient", kind=float, required=False),
    ]

    group_name = "SynapsesClassification"

    @classmethod
    def load(cls, xml):
        data = GenericProperty.load.__func__(cls, xml)
        for attr in [a for a in cls._attributes if not a.required]:
            values = sum(getattr(d, attr.name, None) is not None for d in data)
            if values == 0:  # no values, remove attribute
                pass
                # for d in data:
                #     delattr(d, attr)
            elif values != len(data):
                raise ValueError(f"Attribute {attr} needs to be set/unset"
                                 f" for all {cls.__name__} simultaneously")
        return data


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
        self.seed = Seeds.load(recipe.xml).synapseSeed
        logger.info("Using seed %d for synapse properties", self.seed)

        # Someone assigned the classification of synapses to an element
        # called "Properties" and property management to "Classification"
        # [sic]
        classification = SynapsesProperty.load(recipe.xml)
        properties = SynapsesClassification.load(recipe.xml)

        self.classification = self.convert_classification(
            source, target, classification
        )
        self.properties = self.convert_properties(classification, properties)

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
            self.classification,
            self.properties,
            self.seed
        )
        return extended_touches

    @staticmethod
    def convert_properties(classification, properties):
        """Loader for SynapsesClassification [sic]

        This element of the recipe describes the properties after classification.
        """
        prop_df = _load_from_recipe(properties, schema.SYNAPSE_PROPERTY_SCHEMA, trim=True) \
            .withColumnRenamed("_i", "_prop_i")
        class_df = _load_from_recipe(classification, schema.SYNAPSE_CLASSIFICATION_SCHEMA) \
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
    def convert_classification(source, target, classification):
        """Loader for SynapsesProperties [sic]

        This element of the recipe classifies synapses, to later on assign
        matching properties. Classification may be based on source and
        target `mtype`, `etype`, or synapse class.
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

        syn_class_rules = classification
        not_covered = max(r._i for r in syn_class_rules) + 1

        prop_rule_matrix = np.full(
            # Our 6-dim matrix
            fill_value=not_covered,
            shape=shape,
            dtype="uint16"
        )

        # Iterate for all rules, expanding * as necessary
        # We keep rule definition order as required
        for rule in syn_class_rules:
            selectors = [None] * 6
            for i, direction in enumerate(("from", "to")):
                expanded_names = defaultdict(dict)
                field_to_values = values[direction]
                field_to_reverses = reverses[direction]
                for j, field_t in enumerate(field_to_values):
                    field_name = direction + field_t
                    field_val = rule[field_name]
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
        for i, field in reversed(list(enumerate(group_schema.fields))):
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
