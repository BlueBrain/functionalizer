"""Spine length specification."""
from ..property import Property, PropertyGroup


class Quantile(Property):
    """Quantile definition for spine length distributions."""

    _name = "quantile"

    _attributes = {
        "length": float,
        "fraction": float,
    }


class SpineLengths(PropertyGroup):
    """A group of spine length `Quantile`."""

    _kind = Quantile
