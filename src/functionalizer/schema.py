"""Schema definitions and mappings."""

import re

from pyspark.pandas import typedef
from pyspark.sql import types as T

METADATA_FIXED_KEYS = (
    "source_population_name",
    "source_population_size",
    "target_population_name",
    "target_population_size",
)
METADATA_PATTERN = "functionalizer_run{}_{}"
METADATA_PATTERN_RE = re.compile(METADATA_PATTERN.format(r"(\d+(?:\.\d+)*)", ""))


# Maps from both old touch files and old functionalizer output (syn2-style) to
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


# Maps from the internal naming scheme to SONATA one. The second component
# of the tuple specifies the datatype to convert to. If None, no conversion
# is performed.
OUTPUT_MAPPING = {
    "morphology": ("morpho_type_id_pre", None),
}


def schema_for_dataframe(df):
    """Create a Spark schema from the Pandas DataFrame."""

    def _type(col):
        if df.dtypes[col] == object:
            return T.StringType()
        return typedef.as_spark_type(df.dtypes[col])

    return T.StructType([T.StructField(col, _type(col), False) for col in df.columns])
