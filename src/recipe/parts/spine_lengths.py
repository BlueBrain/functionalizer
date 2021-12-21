"""Spine length specification
"""
from ..property import Property, PropertyGroup


class Quantile(Property):
    _name = "quantile"

    _attributes = {
        "length": float,
        "fraction": float,
    }


class SpineLengths(PropertyGroup):
    _kind = Quantile
