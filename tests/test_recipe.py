"""Test recipe class and functionality
"""
from pathlib import Path
from xml.etree.ElementTree import Element

import pytest

from spykfunc.recipe import Recipe
from spykfunc.filters.implementations.bouton_distance import InitialBoutonDistance as IBD
from spykfunc.filters.implementations.touch_rules import TouchRule as TR
from spykfunc.filters.implementations.touch_rules import logger


MTYPES = ["L1_SLAC", "L23_PC", "L23_MC", "L4_PC", "L4_MC", "L5_TTPC1", "L5_MC", "L6_TPC_L1", "L6_MC"]


def test_load_xml():
    """Test recipe reading
    """
    r = Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways.xml"))
    assert r.xml.find("Seeds").attrib["synapseSeed"] == "4236279"


def test_syn_distances():
    """Simple test for property conversion.
    """
    el = Element("InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 0.25})
    root = Element('data')
    root.append(el)
    info = IBD.load(root)
    assert info.inhibitorySynapsesDistance == 0.25


def test_syn_distances_repr():
    """Test that xml->python->xml is identical.
    """
    el = Element("InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 6})
    root = Element('data')
    root.append(el)
    info = IBD.load(root)
    rpr1 = '<InitialBoutonDistance defaultExcSynapsesDistance="25.0" defaultInhSynapsesDistance="6">'
    rpr2 = '<InitialBoutonDistance defaultInhSynapsesDistance="6" defaultExcSynapsesDistance="25.0">'
    assert str(info) in (rpr1, rpr2)


def test_touch_rules():
    """Test touch rules: make sure that all morphology types are covered.
    """
    r = Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways.xml"))
    TR.load(r.xml, MTYPES, [])
    # caplog.clear()
    with pytest.raises(ValueError):
        TR.load(r.xml, MTYPES + ["FOOBAR"], [], strict=True)
