from collections import defaultdict
from . import utils
from pyspark.sql import types as T
from pyspark.sql import functions as F
from enum import Enum


class DataSets:
    NEURONS = "Dataset.Neurons"
    TOUCHES = "Dataset.Touches"
    MORPHOLOGIES = "Dataset.Morphologies"


LAYER_SCHEMA = T.StructType([
    T.StructField("layer_i", T.ByteType(), False),
    T.StructField("layer", T.ByteType(), False),
])

NEURON_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("morphology_i", T.IntegerType(), False),  # mtype
    T.StructField("morphology", T.StringType(), False),     # mtype
    T.StructField("electrophysiology", T.IntegerType(), False),  # etype
    T.StructField("syn_class_index", T.IntegerType(), False),
    T.StructField("position", T.ArrayType(T.DoubleType(), False), False),
    T.StructField("rotation", T.ArrayType(T.DoubleType(), False), False),
    T.StructField("name", T.StringType(), False),
    T.StructField("layer", T.ByteType(), False),
])

TOUCH_SCHEMA = T.StructType([
    T.StructField("pre_neuron_id", T.IntegerType(), False),
    T.StructField("post_neuron_id", T.IntegerType(), False),
    T.StructField("pre_section", T.ShortType(), False),
    T.StructField("pre_segment", T.ShortType(), False),
    T.StructField("post_section", T.ShortType(), False),
    T.StructField("post_segment", T.ShortType(), False),
    T.StructField("pre_offset", T.FloatType(), False),
    T.StructField("post_offset", T.FloatType(), False),
    T.StructField("distance_soma", T.FloatType(), False),
    T.StructField("branch_order", T.ByteType(), False)
])

MTYPE_SCHEMA = T.StructType([
    T.StructField("_i", T.ShortType(), False),
    T.StructField("index", T.ShortType(), False),
    T.StructField("name", T.StringType(), False),
    T.StructField("layer", T.ShortType(), False),
    T.StructField("morpho_type", T.StringType(), False),
    T.StructField("end_layer", T.ShortType(), False),
])

SYNAPSE_PROPERTY_SCHEMA = T.StructType([
    T.StructField("_i", T.ShortType(), False),
    T.StructField("fromSClass", T.StringType(), True),
    T.StructField("toSClass", T.StringType(), True),
    T.StructField("fromMType", T.StringType(), True),
    T.StructField("toMType", T.StringType(), True),
    T.StructField("fromEType", T.StringType(), True),
    T.StructField("toEType", T.StringType(), True),
    T.StructField("type", T.StringType(), False),
    T.StructField("neuralTransmitterReleaseDelay", T.FloatType(), False),
    T.StructField("axonalConductionVelocity", T.FloatType(), False),
])

SYNAPSE_REPOSITION_SCHEMA = T.StructType([
    T.StructField("pathway_i", T.IntegerType(), False),
    T.StructField("reposition", T.BooleanType(), False),
])

SYNAPSE_CLASS_MAP_SCHEMA = T.StructType([
    T.StructField("syn_prop_index", T.IntegerType(), False),
    T.StructField("syn_prop_i", T.ShortType(), False),
])

SYNAPSE_CLASS_SCHEMA = T.StructType([
    T.StructField("_i", T.ShortType(), False),
    T.StructField("id", T.StringType(), False),
    T.StructField("gsyn", T.FloatType(), False),
    T.StructField("gsynSD", T.FloatType(), False),
    T.StructField("nsyn", T.FloatType(), False),
    T.StructField("nsynSD", T.FloatType(), False),
    T.StructField("dtc", T.FloatType(), False),
    T.StructField("dtcSD", T.FloatType(), False),
    T.StructField("u", T.FloatType(), False),
    T.StructField("uSD", T.FloatType(), False),
    T.StructField("d", T.FloatType(), False),
    T.StructField("dSD", T.FloatType(), False),
    T.StructField("f", T.FloatType(), False),
    T.StructField("fSD", T.FloatType(), False),
    T.StructField("nrrp", T.FloatType(), False),
])


def indexed_strings(names):
    """Create a schema mapping int to str
    """
    assert len(names) == 2
    return T.StructType([T.StructField(names[0], T.IntegerType(), False),
                         T.StructField(names[1], T.StringType(), False)])


def to_pathway_i(col1, col2):
    """
    Expression to calculate the pathway index based on the morphologies' index
    :param col1: The dataframe src morphology index field
    :param col2: The dataframe dst morphology index field
    """
    return (F.shiftLeft(col1, 16) + F.col(col2)).cast(T.IntegerType()).alias("pathway_i")


def to_pathway_str(col1, col2):
    """Expression to calculate the pathway name from the morphologies name
    """
    return F.concat(F.col(col1), F.lit("->"), F.col(col2))


def pathway_i_to_str(df_pathway_i, mtypes):
    """
    Expression to calculate the pathway name from the pathway index.
    :param df_pathway_i: Pathway index column
    :param mtypes: mtypes dataframe, with two fields: (index: int, name: str)
    """
    col = "pathway_i"
    return (
        df_pathway_i
        .withColumn("src_morpho_i", F.shiftRight(col, 16))
        .withColumn("dst_morpho_i", F.col(col).bitwiseAND((1 << 16)-1))
        .join(mtypes.toDF("src_morpho_i", "src_morpho"), "src_morpho_i")
        .join(mtypes.toDF("dst_morpho_i", "dst_morpho"), "dst_morpho_i")
        .withColumn("pathway_str", F.concat("src_morpho", F.lit('->'), "dst_morpho"))
        .drop("src_morpho_i", "src_morpho", "dst_morpho_i", "dst_morpho")
    )


def touches_with_pathway(circuit):
    """Add path identifying field.
    """
    return circuit.withColumn(
        "pathway_i",
        to_pathway_i("src_morphology_i", "dst_morphology_i")
    )


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
        getattr(PreNeuronFields, fname)
        for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.POST_NEURON: tuple(
        getattr(PostNeuronFields, fname)
        for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.TOUCH: tuple(
        field
        for field in TouchFields.__dict__.values() if isinstance(field, Field)),
    Neuron_Graph_Entity.POST_POST_NEURON: tuple(
        getattr(PostPostNeuronFields, fname)
        for fname, field in NeuronFields.__dict__.items() if isinstance(field, Field)),
    Neuron_Graph_Entity.SECOND_TOUCH: tuple(
        getattr(SecondTouchFields, fname)
        for fname, field in TouchFields.__dict__.items() if isinstance(field, Field)),
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
