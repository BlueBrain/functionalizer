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

MTYPE_NAMES_SCHEMA = T.StructType([
    T.StructField("mtype_i", T.IntegerType(), False),
    T.StructField("mtype", T.StringType(), False),
])

NEURON_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("mtype_i", T.ShortType(), False),
    T.StructField("etype_i", T.ShortType(), False),
    T.StructField("morphology", T.StringType(), False),
    T.StructField("syn_class_i", T.ShortType(), False),
    # T.StructField("layer", T.ShortType(), False),
    # T.StructField("position", T.ArrayType(T.DoubleType(), False), False),
    # T.StructField("rotation", T.ArrayType(T.DoubleType(), False), False),
])

TOUCH_SCHEMA_V1 = T.StructType([
    T.StructField("synapse_id", T.LongType(), False),
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

TOUCH_SCHEMA_V2 = T.StructType(TOUCH_SCHEMA_V1.fields + [
    T.StructField("pre_section_fraction", T.FloatType(), False),
    T.StructField("post_section_fraction", T.FloatType(), False),
    T.StructField("pre_position_x", T.FloatType(), False),
    T.StructField("pre_position_y", T.FloatType(), False),
    T.StructField("pre_position_z", T.FloatType(), False),
    T.StructField("post_position_x", T.FloatType(), False),
    T.StructField("post_position_y", T.FloatType(), False),
    T.StructField("post_position_z", T.FloatType(), False),
    T.StructField("spine_length", T.FloatType(), False),
    T.StructField("branch_type", T.ShortType(), False),
])

TOUCH_SCHEMAS = [
    TOUCH_SCHEMA_V1,
    TOUCH_SCHEMA_V2
]

GAP_JUNCTION_SCHEMA = T.StructType([
    T.StructField("pre_neuron_id", T.IntegerType(), False),
    T.StructField("post_neuron_id", T.IntegerType(), False),
    T.StructField("pre_section", T.ShortType(), False),
    T.StructField("pre_segment", T.ShortType(), False),
    T.StructField("post_section", T.ShortType(), False),
    T.StructField("post_segment", T.ShortType(), False),
    T.StructField("pre_offset", T.FloatType(), False),
    T.StructField("post_offset", T.FloatType(), False),
    T.StructField("pre_junction", T.LongType(), False),
    T.StructField("post_junction", T.LongType(), False),
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
    T.StructField("nrrp", T.ShortType(), False),
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
        to_pathway_i("src_mtype_i", "dst_mtype_i")
    )
