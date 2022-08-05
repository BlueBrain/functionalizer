from enum import Enum


class SortBy(Enum):
    POST = ("target_node_id", "source_node_id", "synapse_id")
    PRE = ("source_node_id", "target_node_id", "synapse_id")


class RunningMode(Enum):
    """Definintions for running modes"""

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
    FILTER_RULES = 0
    FILTER_REDUCED_TOUCHES = 1
    REDUCE_AND_CUT = 2
    SYNAPSE_PROPS = 3
