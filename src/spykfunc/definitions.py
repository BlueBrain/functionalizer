"""Basic definitions used when running Spykfunc."""
from enum import Enum


class SortBy(Enum):
    """Columns to sort SONATA edge data by."""

    POST = ("target_node_id", "source_node_id", "synapse_id")
    PRE = ("source_node_id", "target_node_id", "synapse_id")


class RunningMode(Enum):
    """Filters to use for various running modes."""

    STRUCTURAL = ("BoutonDistance", "TouchRules")
    FUNCTIONAL = (
        "BoutonDistance",
        "TouchRules",
        "SpineLength",
        "ReduceAndCut",
        "SynapseReposition",
        "SynapseProperties",
    )
    GAP_JUNCTIONS = ("SomaDistance", "DenseID", "GapJunction", "GapJunctionProperties")


class CheckpointPhases(Enum):
    """Predefined running phases for edge processing."""

    FILTER_RULES = 0
    FILTER_REDUCED_TOUCHES = 1
    REDUCE_AND_CUT = 2
    SYNAPSE_PROPS = 3
