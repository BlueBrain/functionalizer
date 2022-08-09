"""Properties for gap-junctions."""
from ..property import Property, singleton


@singleton(implicit=True)
class GapJunctionProperties(Property):
    """Synaptic properties for gap-junctions."""

    _name = "GapJunctionProperty"

    _attributes = {"gsyn": 0.2}
