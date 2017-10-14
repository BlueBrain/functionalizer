from enum import Enum

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
    def init_fzer_indexes(cls, vec):
        for i, name in enumerate(vec):
            cls[name].fzer_index = i

    @classmethod
    def from_fzer_index(cls, i):
        for cc in cls:
            if cc is cls.NONE:
                continue
            if not hasattr(cc, "fzer_index"):
                raise RuntimeError("CellClass indexes no available. Please init_fzer_indexes()")
            if cc.fzer_index == i:
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
        assert len(_parts) >= 2 and _parts[0][0] == "L", "Invalid mtype"
        mt.layer = int(_parts[0][1:])
        mt.morpho_type = _parts[1]
        if len(_parts) == 3:
            assert _parts[2][0] == "L", "Invalid mtype"
            mt.end_layer = int(_parts[2][1:])
        else:
            mt.end_layer = None
        return mt

    @classmethod
    def get(cls, name):
        return cls._cache.get(name)

    def __repr__(self):
        return "<MType: %s>" % (self.name,)
