"""Simple touch reduction."""
from ..property import Property, singleton


@singleton
class TouchReduction(Property):
    """Class representing a Touch filter."""

    _attributes = {
        "survival_rate": float,
    }
