"""Specifications for synapse distance from the soma."""
from ..property import Property, singleton


@singleton(implicit=True)
class InitialBoutonDistance(Property):
    """Minimum distances of synapses from the soma."""

    _attribute_alias = {
        "defaultInhSynapsesDistance": "inhibitorySynapsesDistance",
        "defaultExcSynapsesDistance": "excitatorySynapsesDistance",
    }

    _attributes = {
        "inhibitorySynapsesDistance": 5.0,
        "excitatorySynapsesDistance": 25.0,
    }
