from enum import Enum


# Fields to sort by, besides the neuron IDs. Used to get a deterministic
# order in the output.
_SORTING_FIELDS = (
    "pre_section",
    "pre_segment",
    "post_section",
    "post_segment",
    "pre_offset",
    "post_offset"
)


class SortBy(Enum):
    POST = (
        "connected_neurons_post",
        "connected_neurons_pre",
        *_SORTING_FIELDS
    )
    PRE = (
        "connected_neurons_pre",
        "connected_neurons_post",
        *_SORTING_FIELDS
    )


class RunningMode(Enum):
    """Definintions for running modes
    """
    STRUCTURAL = ('BoutonDistance', 'SynapseProperties')
    FUNCTIONAL = ('BoutonDistance', 'TouchRules', 'SpineLength', 'ReduceAndCut', 'SynapseReposition', 'SynapseProperties')
    GAP_JUNCTIONS = ('SomaDistance', 'DenseID', 'GapJunction', 'GapJunctionProperties')


class CellClass(Enum):
    """
    Enumeration of Cell Classes, typically Inhibitory or Excitatory.
    At any point we can register functionalizer indexes and get the corresponding index from the items directly
    """
    NONE = 0
    EXC = 1
    INH = 2

    def __int__(self):
        return self.value

    @classmethod
    def initialize_indices(cls, vec):
        for i, name in enumerate(vec):
            cls[name].index = i

    @classmethod
    def from_index(cls, i):
        for cc in cls:
            if cc is cls.NONE:
                continue
            if not hasattr(cc, "index"):
                raise RuntimeError("CellClass indices no available. Please initialize_indices()")
            if cc.index == i:
                return cc
        return cls.NONE


class MType(object):
    """
    Morphology types.
    Morphology types are described by the initial brain layer, then the cell type and eventually the end layer.
    We don't specify all of them statically but create/cache instances
    """
    __slots__ = ("name", "layer", "morpho_type", "end_layer")
    _cache = {}

    def __new__(cls, mtype=None):
        if not mtype:
            return object.__new__(MType)
        mt = cls._cache.get(mtype)
        if not mt:
            mt = cls._cache[mtype] = cls.create_mtype(mtype)
        return mt

    @staticmethod
    def create_mtype(mtype_str):
        mt = object.__new__(MType)
        mt.name = mtype_str
        _parts = mtype_str.strip().split("_")
        assert len(_parts) >= 2, "Invalid mtype"
        mt.layer = _parts[0]
        mt.morpho_type = _parts[1]
        if len(_parts) == 3:
            mt.end_layer = _parts[2]
        else:
            mt.end_layer = None
        return mt

    @classmethod
    def get(cls, name):
        return cls._cache.get(name)

    def __repr__(self):
        return "<MType: %s>" % (self.name,)


class CheckpointPhases(Enum):
    FILTER_RULES = 0
    FILTER_REDUCED_TOUCHES = 1
    REDUCE_AND_CUT = 2
    SYNAPSE_PROPS = 3
