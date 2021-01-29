from collections import defaultdict
from . import utils
from pyspark.sql import types as T
from pyspark.sql import functions as F
from enum import Enum


class DataSets:
    NEURONS = "Dataset.Neurons"
    TOUCHES = "Dataset.Touches"
    MORPHOLOGIES = "Dataset.Morphologies"


# Maps from the internal naming scheme to the SYN2 one. The third component
# of the tuple specifies the datatype to convert to. If None, no conversion
# is performed.
OUTPUT_COLUMN_MAPPING = [
    ("post_gid", "connected_neurons_post", T.LongType()),
    ("pre_gid", "connected_neurons_pre", T.LongType()),
    ("axonal_delay", "delay", None),
    ("gsyn", "conductance", None),
    ("u", "u_syn", None),
    ("d", "depression_time", None),
    ("f", "facilitation_time", None),
    ("dtc", "decay_time", None),
    ("gsynSRSF", "conductance_scale_factor", None),
    ("uHillCoefficient", "u_hill_coefficient", None),
    ("synapseType", "syn_type_id", None),
    # Renamed to edge_type_id to conform to SONATA
    ("synapse_type_id", "synapse_type_id", None),
    ("morphology", "morpho_type_id_pre", None),
    # ("branch_order_dend", "morpho_branch_order_dend", None),  # N/A
    # ("branch_order_axon", "morpho_branch_order_axon", None),  # Irrelevant
    ("nrrp", "n_rrp_vesicles", T.ShortType()),
    ("post_section", "morpho_section_id_post", None),
    ("post_segment", "morpho_segment_id_post", None),
    ("post_offset", "morpho_offset_segment_post", T.FloatType()),
    ("pre_section", "morpho_section_id_pre", None),
    ("pre_segment", "morpho_segment_id_pre", None),
    ("pre_offset", "morpho_offset_segment_pre", T.FloatType()),
    ("pre_section_fraction", "morpho_section_fraction_pre", T.FloatType()),
    ("post_section_fraction", "morpho_section_fraction_post", T.FloatType()),
    ("spine_length", "morpho_spine_length", None),
    ("pre_position_x", "position_contour_pre_x", None),
    ("pre_position_y", "position_contour_pre_y", None),
    ("pre_position_z", "position_contour_pre_z", None),
    ("post_position_x", "position_center_post_x", None),
    ("post_position_y", "position_center_post_y", None),
    ("post_position_z", "position_center_post_z", None),
    ("pre_position_center_x", "position_center_pre_x", None),
    ("pre_position_center_y", "position_center_pre_y", None),
    ("pre_position_center_z", "position_center_pre_z", None),
    ("post_position_surface_x", "position_contour_post_x", None),
    ("post_position_surface_y", "position_contour_post_y", None),
    ("post_position_surface_z", "position_contour_post_z", None),
    ("pre_branch_type", "morpho_section_type_pre", None),
    ("post_branch_type", "morpho_section_type_post", None),
    ("src", "connected_neurons_pre", T.LongType()),
    ("dst", "connected_neurons_post", T.LongType()),
    ("post_junction", "junction_id_post", None),
    ("pre_junction", "junction_id_pre", None),
]


# When reading SONATA, this will map any attributes Spykfunc may produce
# back to the internal names. SONATA specific names are hardcoded, other
# fields are added by using the reverse of the output map.
INPUT_COLUMN_MAPPING = [
    ("distance_soma", "distance_soma"),
    ("afferent_section_type", "post_branch_type"),
    ("afferent_segment_offset", "post_offset"),
    ("afferent_center_x", "post_position_x"),
    ("afferent_center_y", "post_position_y"),
    ("afferent_center_z", "post_position_z"),
    ("afferent_section_pos", "post_section_fraction"),
    ("afferent_section_id", "post_section"),
    ("afferent_segment_id", "post_segment"),
    ("efferent_section_type", "pre_branch_type"),
    ("efferent_segment_offset", "pre_offset"),
    ("efferent_surface_x", "pre_position_x"),
    ("efferent_surface_y", "pre_position_y"),
    ("efferent_surface_z", "pre_position_z"),
    ("efferent_section_pos", "pre_section_fraction"),
    ("efferent_section_id", "pre_section"),
    ("efferent_segment_id", "pre_segment"),
    ("spine_length", "spine_length"),
    ("synapse_id", "synapse_id"),
    ("edge_type_id", "synapse_type_id"),
    ("efferent_morphology_id", "morphology"),
] + [(external, internal) for (internal, external, _) in OUTPUT_COLUMN_MAPPING]


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

TOUCH_SCHEMA_V3 = T.StructType(TOUCH_SCHEMA_V2.fields[:-1] + [
    T.StructField("pre_branch_type", T.ShortType(), False),
    T.StructField("post_branch_type", T.ShortType(), False),
])

TOUCH_SCHEMA_V4 = T.StructType(TOUCH_SCHEMA_V3.fields + [
    T.StructField("pre_position_center_x", T.FloatType(), False),
    T.StructField("pre_position_center_y", T.FloatType(), False),
    T.StructField("pre_position_center_z", T.FloatType(), False),
    T.StructField("post_position_surface_x", T.FloatType(), False),
    T.StructField("post_position_surface_y", T.FloatType(), False),
    T.StructField("post_position_surface_z", T.FloatType(), False),
])

TOUCH_SCHEMAS = [
    TOUCH_SCHEMA_V4,
    TOUCH_SCHEMA_V3,
    TOUCH_SCHEMA_V2,
    TOUCH_SCHEMA_V1,
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

SYNAPSE_CLASSIFICATION_SCHEMA = T.StructType([
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
    T.StructField("classification_index", T.IntegerType(), False),
    T.StructField("classification_i", T.ShortType(), False),
])

SYNAPSE_PROPERTY_SCHEMA = T.StructType([
    T.StructField("_i", T.ShortType(), False),
    T.StructField("id", T.StringType(), False),
    T.StructField("gsyn", T.FloatType(), False),
    T.StructField("gsynSD", T.FloatType(), False),
    T.StructField("dtc", T.FloatType(), False),
    T.StructField("dtcSD", T.FloatType(), False),
    T.StructField("u", T.FloatType(), False),
    T.StructField("uSD", T.FloatType(), False),
    T.StructField("d", T.FloatType(), False),
    T.StructField("dSD", T.FloatType(), False),
    T.StructField("f", T.FloatType(), False),
    T.StructField("fSD", T.FloatType(), False),
    T.StructField("nrrp", T.FloatType(), False),
    T.StructField("gsynSRSF", T.FloatType(), False),
    T.StructField("uHillCoefficient", T.FloatType(), False),
])


def indexed_strings(names):
    """Create a schema mapping int to str
    """
    assert len(names) == 2
    return T.StructType([T.StructField(names[0], T.IntegerType(), False),
                         T.StructField(names[1], T.StringType(), False)])
