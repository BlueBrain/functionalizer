"""Test recipe class and functionality
"""
from pathlib import Path

from spykfunc.recipe import Recipe
from spykfunc.filters.implementations.bouton_distance import BoutonDistanceFilter as BF


def test_load_xml():
    """Test recipe reading
    """
    r = Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways.xml"))
    assert r.xml.find("Seeds").attrib["synapseSeed"] == "4236279"


def test_syn_distances():
    """Simple test for property conversion.
    """
    from xml.etree.ElementTree import Element
    el = Element("InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 0.25})
    info = BF.convert_info(el)
    assert info.inhibitorySynapsesDistance == 0.25
    info = BF.convert_info({"blaaaa": 1, "defaultInhSynapsesDistance": 0.26})
    assert info.inhibitorySynapsesDistance == 0.26


def test_syn_distances_repr():
    """Test that xml->python->xml is identical.
    """
    info = BF.convert_info({"blaaaa": 1, "defaultInhSynapsesDistance": 6})
    rpr1 = '<InitialBoutonDistance defaultExcSynapsesDistance="25.0" defaultInhSynapsesDistance="6">'
    rpr2 = '<InitialBoutonDistance defaultInhSynapsesDistance="6" defaultExcSynapsesDistance="25.0">'
    assert str(info) in (rpr1, rpr2)
