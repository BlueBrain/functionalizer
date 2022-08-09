"""Seed specification."""
from ..property import Property, singleton


@singleton(implicit=True)
class Seeds(Property):
    """Property to store seeds."""

    _attributes = {
        "recipeSeed": 0,
        "columnSeed": 0,
        "synapseSeed": 0,
    }
