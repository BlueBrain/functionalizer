import re

from collections import defaultdict
from . import utils
from pyspark.sql import types as T
from pyspark.sql import functions as F
from enum import Enum


METADATA_FIXED_KEYS = (
    "source_population_name",
    "source_population_size",
    "target_population_name",
    "target_population_size",
)
METADATA_PATTERN = "spykfunc_run{}_{}"
METADATA_PATTERN_RE = re.compile(METADATA_PATTERN.format(r"(\d+(?:\.\d+)*)", ""))


# Maps from both old touch files and old spykfunc output (syn2-style) to
# the conventions used with SONATA
LEGACY_MAPPING = {
    "branch_type": "section_type",
    "connected_neurons_post": "target_node_id",
    "connected_neurons_pre": "source_node_id",
    "junction_id_post": "afferent_junction_id",
    "junction_id_pre": "efferent_junction_id",
    "morpho_offset_segment_post": "afferent_segment_offset",
    "morpho_offset_segment_pre": "efferent_segment_offset",
    "morpho_section_fraction_post": "afferent_section_pos",
    "morpho_section_fraction_pre": "efferent_section_pos",
    "morpho_section_id_post": "afferent_section_id",
    "morpho_section_id_pre": "efferent_section_id",
    "morpho_section_type_post": "afferent_section_type",
    "morpho_section_type_pre": "efferent_section_type",
    "morpho_segment_id_post": "afferent_segment_id",
    "morpho_segment_id_pre": "efferent_segment_id",
    "morpho_spine_length": "spine_length",
    "morpho_type_id_pre": "efferent_morphology_id",
    "position_center_post_x": "afferent_center_x",
    "position_center_post_y": "afferent_center_y",
    "position_center_post_z": "afferent_center_z",
    "position_center_pre_x": "efferent_center_x",
    "position_center_pre_y": "efferent_center_y",
    "position_center_pre_z": "efferent_center_z",
    "position_contour_post_x": "afferent_surface_x",
    "position_contour_post_y": "afferent_surface_y",
    "position_contour_post_z": "afferent_surface_z",
    "position_contour_pre_x": "efferent_surface_x",
    "position_contour_pre_y": "efferent_surface_y",
    "position_contour_pre_z": "efferent_surface_z",
    "post_branch_type": "afferent_section_type",
    "post_gid": "target_node_id",
    "post_neuron_id": "target_node_id",
    "post_offset": "afferent_segment_offset",
    "post_position_surface_x": "afferent_surface_x",
    "post_position_surface_y": "afferent_surface_y",
    "post_position_surface_z": "afferent_surface_z",
    "post_position_x": "afferent_center_x",
    "post_position_y": "afferent_center_y",
    "post_position_z": "afferent_center_z",
    "post_section": "afferent_section_id",
    "post_section_fraction": "afferent_section_pos",
    "post_segment": "afferent_segment_id",
    "pre_branch_type": "efferent_section_type",
    "pre_gid": "source_node_id",
    "pre_neuron_id": "source_node_id",
    "pre_offset": "efferent_segment_offset",
    "pre_position_center_x": "efferent_center_x",
    "pre_position_center_y": "efferent_center_y",
    "pre_position_center_z": "efferent_center_z",
    "pre_position_x": "efferent_surface_x",
    "pre_position_y": "efferent_surface_y",
    "pre_position_z": "efferent_surface_z",
    "pre_section": "efferent_section_id",
    "pre_section_fraction": "efferent_section_pos",
    "pre_segment": "efferent_segment_id",
    "synapse_type_id": "edge_type_id",
}


# Maps from the internal naming scheme to SONATA one. The third component
# of the tuple specifies the datatype to convert to. If None, no conversion
# is performed.
OUTPUT_MAPPING = {
    "axonal_delay": ("delay", None),
    "gsyn": ("conductance", None),
    "u": ("u_syn", None),
    "d": ("depression_time", None),
    "f": ("facilitation_time", None),
    "dtc": ("decay_time", None),
    "gsynSRSF": ("conductance_scale_factor", None),
    "uHillCoefficient": ("u_hill_coefficient", None),
    "morphology": ("morpho_type_id_pre", None),
    "nrrp": ("n_rrp_vesicles", T.ShortType()),
}

SYNAPSE_CLASSIFICATION_SCHEMA = T.StructType([
    T.StructField("_i", T.ShortType(), False),
    T.StructField("type", T.StringType(), False),
    T.StructField("neuralTransmitterReleaseDelay", T.FloatType(), False),
    T.StructField("axonalConductionVelocity", T.FloatType(), False),
])

SYNAPSE_REPOSITION_SCHEMA = T.StructType([
    T.StructField("pathway_i", T.IntegerType(), False),
    T.StructField("reposition", T.BooleanType(), False),
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
