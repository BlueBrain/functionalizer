class CellClass:
    CLASS_NONE = 0
    CLASS_EXC = 1
    CLASS_INH = 2

    def __init__(cls, *args, **kwargs):
        raise TypeError("No instantiation of Enums")

    @classmethod
    def from_string(cls, cellClassName):
        if cellClassName == "EXC":
            return cls.CLASS_EXC
        elif cellClassName == "INH":
            return cls.CLASS_INH
        return cls.CLASS_NONE


class MType(object):
    """
    Morphology types.
    Morphology types are described by the initial brain layer, then the cell type and eventually the end layer.
    We don't specify all of them statically but create/cache instances
    """
    __slots__ = ("name", "layer", "morpho_type", "end_layer")
    _cache = {}

    def __new__(cls, mtype):
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

    def __repr__(self):
        return "<MType: %s>" % (self.name,)
