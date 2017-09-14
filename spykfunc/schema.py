from collections import defaultdict
from . import utils
from pyspark.sql import types
from enum import Enum


class DataSets:
    NEURONS = "Dataset.Neurons"
    TOUCHES = "Dataset.Touches"
    MORPHOLOGIES = "Dataset.Morphologies"


NEURON_SCHEMA = types.StructType([
    types.StructField("id", types.IntegerType(), False),
    types.StructField("morphology_i", types.IntegerType(), False),  # mtype
    types.StructField("morphology", types.StringType(), False),  # mtype
    types.StructField("electrophysiology", types.IntegerType(), False),  # etype
    types.StructField("syn_class_index", types.IntegerType(), False),
    types.StructField("position", types.ArrayType(types.DoubleType(), False), False),
    types.StructField("rotation", types.ArrayType(types.DoubleType(), False), False),
    types.StructField("name", types.StringType(), False),
    types.StructField("layer", types.ByteType(), False),
])

TOUCH_SCHEMA = types.StructType([
    types.StructField("pre_neuron_id", types.IntegerType(), False),
    types.StructField("post_neuron_id", types.IntegerType(), False),
    types.StructField("pre_section", types.ShortType(), False),
    types.StructField("pre_segment", types.ShortType(), False),
    types.StructField("post_section", types.ShortType(), False),
    types.StructField("post_segment", types.ShortType(), False),
    types.StructField("pre_offset", types.FloatType(), False),
    types.StructField("post_offset", types.FloatType(), False),
    types.StructField("distance_soma", types.FloatType(), False),
    types.StructField("branch_order", types.ByteType(), False)
])

MTYPE_SCHEMA = types.StructType([
    types.StructField("_i", types.ShortType(), False),
    types.StructField("index", types.ShortType(), False),
    types.StructField("name", types.StringType(), False),
    types.StructField("layer", types.ShortType(), False),
    types.StructField("morpho_type", types.StringType(), False),
    types.StructField("end_layer", types.ShortType(), False),
])

SYNAPSE_PROPERTY_SCHEMA = types.StructType([
    types.StructField("_i", types.ShortType(), False),
    types.StructField("fromSClass", types.StringType(), False),
    types.StructField("toSClass", types.StringType(), False),
    types.StructField("fromSClass_i", types.ShortType(), False),
    types.StructField("toSClass_i", types.ShortType(), False),
    types.StructField("type", types.StringType(), False),
    types.StructField("neuralTransmitterReleaseDelay", types.FloatType(), False),
    types.StructField("axonalConductionVelocity", types.FloatType(), False),
])

SYNAPSE_CLASS_SCHEMA = types.StructType([
    types.StructField("_i", types.ShortType(), False),
    types.StructField("id", types.StringType(), False),
    types.StructField("gsyn", types.FloatType(), False),
    types.StructField("gsynVar", types.FloatType(), False),
    types.StructField("nsyn", types.FloatType(), False),
    types.StructField("nsynVar", types.FloatType(), False),
    types.StructField("dtc", types.FloatType(), False),
    types.StructField("dtcVar", types.FloatType(), False),
    types.StructField("u", types.FloatType(), False),
    types.StructField("uVar", types.FloatType(), False),
    types.StructField("d", types.IntegerType(), False),
    types.StructField("dVar", types.IntegerType(), False),
    types.StructField("f", types.IntegerType(), False),
    types.StructField("fVar", types.IntegerType(), False),
    types.StructField("ase", types.IntegerType(), False),
])

# Fields as Enumerations
_neuronFields = tuple(field.name for field in NEURON_SCHEMA)
NeuronFields = Enum("NeuronFields", _neuronFields)
_touchFields = tuple(field.name for field in TOUCH_SCHEMA)
TouchFields = Enum("TouchFields", _touchFields)
_mtype_fields = tuple(field.name for field in TOUCH_SCHEMA)
MTypeFields = Enum("MTypeFields", _mtype_fields)

# moprhologies won't probabl make it to dataframes
# _morphologyFields = ()


# ---
# Role in graph, Foreign-key semantic
# Graphframes queries shall not go over 2 levels (3 vertexes)
#
class Neuron_Graph_Entity:
    PRE_NEURON = "Entity.Pre_Neuron"
    POST_NEURON = "Entity.Post_Neuron"
    TOUCH = "Entity.Touch"
    # Second level
    POST_POST_NEURON = "Entity.Post_Post_Neuron"
    SECOND_TOUCH = "Entity.SecondTouch"
    # Morphologies
    PRE_NEURON_MORPHO = "Entity.Pre_Neuron_Morpho"
    POST_NEURON_MORPHO = "Entity.Post_Neuron_Morpho"


# -----------------------------------------------------------
# Class defining a base field,
# so that queries can refer to fields and not only literals
# -----------------------------------------------------------
class Field(object):
    """
    Basic field of a data structure, column of a table
    """
    # Field should be agnostic from its function in the graph
    # However we provide a default here, indicating that subclasses should implement it
    _graph_entity = None
    _alias = None
    _instance_name = None

    def __init__(self, dataset, fieldname):
        self.dataset = dataset
        self.field_name = fieldname

    def alias(self, new_name):
        self._alias = new_name

    def __eq__(self, other):
        if other is self:
            return True

        myname = self.alias or self.field_name
        othername = other.alias or other.field_name
        return self._graph_entity == other.graph_entity and myname == othername

    def __str__(self):
        return (self._instance_name or "") + '.' + self.field_name

    def __call__(self, instance):
        # "instantiates" the field to a particular struct name
        self._instance_name = instance
        return self


# -------------------------------------------
# Specification of fields
# -------------------------------------------
class NeuronField(Field):
    """
    A field of the Neuron data structure
    """

    def __init__(self, fieldname):
        super(NeuronField, self).__init__(DataSets.NEURONS, fieldname)

    # cache for dynamically constructed fields
    _cache = defaultdict(dict)

    # Pre-computed fields can be specialized
    def __get__(self, instance, owner):
        if issubclass(owner, PreNeuronFields) and not isinstance(self, PreNeuronField):
            return self._get_cached_or_create(PreNeuronField, self.field_name)
        elif issubclass(owner, PostNeuronFields) and not isinstance(self, PostNeuronField):
            return self._get_cached_or_create(PostNeuronField, self.field_name)
        elif issubclass(owner, PostPostNeuronFields) and not isinstance(self, PostPostNeuronField):
            return self._get_cached_or_create(PostPostNeuronField, self.field_name)
        return self

    @classmethod
    def _get_cached_or_create(cls, class_type, field_name):
        return utils.get_or_create(cls._cache[class_type], field_name, class_type, {"fieldname": field_name})


class TouchField(Field):
    """
        A field of the Touch data structure
    """

    def __init__(self, fieldname):
        super(TouchField, self).__init__(DataSets.TOUCHES, fieldname)


# --------------------------------
# Fields Identification
# --------------------------------
class NeuronFields(object):
    """
    Existing fields in Neuron
    """
    id = NeuronField("id")
    morphology = NeuronField("morphology")
    electrophysiology = NeuronField("electrophysiology")
    syn_class_index = NeuronField("syn_class_index")
    position = NeuronField("position")
    rotation = NeuronField("rotation")
    name = NeuronField("name")


class PreNeuronFields(NeuronFields):
    pass


class PostNeuronFields(NeuronFields):
    pass


class PostPostNeuronFields(NeuronFields):
    pass


class TouchFields(object):
    """
    Existing fields in Touch
    """
    src = TouchField("src")
    dst = TouchField("dst")
    pre_section = TouchField("pre_section")
    pre_segment = TouchField("pre_segment")
    post_setion = TouchField("post_setion")
    post_segment = TouchField("post_segment")
    pre_offset = TouchField("pre_offset")
    post_offset = TouchField("post_offset")
    distance_soma = TouchField("distance_soma")
    branch_order = TouchField("branch_order")


class SecondTouchFields(TouchFields):
    pass


# ----------------------------------------------
# Specification of the fields role in the graph
# -----------------------------------------------
class PreNeuronField(NeuronField):
    """ PreNeuron Fields
    """
    graph_entity = Neuron_Graph_Entity.PRE_NEURON
    ALL = PreNeuronFields


class PostNeuronField(NeuronField):
    """ PostNeuron Fields
    """
    graph_entity = Neuron_Graph_Entity.POST_NEURON
    ALL = PostNeuronFields


class PostPostNeuronField(NeuronField):
    """ PostPostNeuron Fields
    """
    graph_entity = Neuron_Graph_Entity.POST_NEURON
    ALL = PostPostNeuronFields


# ---------------------------------
# Default instance names
# ---------------------------------

_graph_entitiy_to_fields = {
    Neuron_Graph_Entity.PRE_NEURON: tuple(
        getattr(PreNeuronFields, fname) for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.POST_NEURON: tuple(
        getattr(PostNeuronFields, fname) for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.TOUCH: tuple(field for field in TouchFields.__dict__.values() if isinstance(field, Field)),
    Neuron_Graph_Entity.POST_POST_NEURON: tuple(
        getattr(PostPostNeuronFields, fname) for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.SECOND_TOUCH: tuple(
        getattr(SecondTouchFields, fname) for fname, field in TouchFields.__dict__.items() if isinstance(field, Field)),
}

_default_graph_entities = {
    Neuron_Graph_Entity.PRE_NEURON: 'n1',
    Neuron_Graph_Entity.POST_NEURON: 'n2',
    Neuron_Graph_Entity.TOUCH: 't',
    Neuron_Graph_Entity.POST_POST_NEURON: 'n3',
    Neuron_Graph_Entity.SECOND_TOUCH: 't2'
}

_default_graph_fields = tuple(
    f(instance)
    for entity_t, instance in _default_graph_entities.items()
    for f in _graph_entitiy_to_fields[entity_t]
)

# test: Getting a specific field, which is constructed from a generic one
if __name__ == "__main__":
    a = PostNeuronFields.morphology
    print(a.dataset, a.field_name, a.graph_entity)
