"""Test recipe class and functionality
"""
import pytest
from spykfunc.recipe import Recipe
try:
    from pathlib2 import Path
except ImportError:
    from pathlib import Path


def test_load_xml():
    """Test recipe reading
    """
    r = Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways.xml"))
    assert r.seeds.synapseSeed == 4236279


def test_syn_distances():
    """Simple test for property conversion.
    """
    from xml.etree.ElementTree import Element
    el = Element("InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 0.25})
    rep = Recipe()
    rep.load_bouton_distance(el)
    assert rep.synapses_distance.inhibitorySynapsesDistance == 0.25
    rep.load_bouton_distance({"blaaaa": 1, "defaultInhSynapsesDistance": 0.26})
    assert rep.synapses_distance.inhibitorySynapsesDistance == 0.26

def test_syn_distances_repr():
    import sys, spykfunc.recipe
    rep = Recipe()
    rep.load_bouton_distance({"blaaaa": 1, "defaultInhSynapsesDistance": 6})
    rpr = '<InitialBoutonDistance defaultExcSynapsesDistance="25.0" defaultInhSynapsesDistance="6">'
    assert str(rep.synapses_distance) == rpr
