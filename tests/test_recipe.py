"""Test recipe class and functionality
"""
from pathlib import Path
from xml.etree.ElementTree import Element

import pytest

from spykfunc.recipe import Recipe
from spykfunc.filters.implementations.bouton_distance import InitialBoutonDistance as BD
from spykfunc.filters.implementations.touch_rules import TouchRule as TR
from spykfunc.filters.implementations.synapse_properties import SynapsesProperty as SP


MTYPES = [
    "L1_SLAC",
    "L23_PC",
    "L23_MC",
    "L4_PC",
    "L4_MC",
    "L5_TTPC1",
    "L5_MC",
    "L6_TPC_L1",
    "L6_MC",
]


@pytest.fixture
def bad_recipe():
    return Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways_faulty.xml"))


@pytest.fixture
def good_recipe():
    return Recipe(str(Path(__file__).parent / "v5builderRecipeAllPathways.xml"))


def test_load_xml(good_recipe):
    """Test recipe reading
    """
    assert good_recipe.xml.find("Seeds").attrib["synapseSeed"] == "4236279"


def test_syn_distances():
    """Simple test for property conversion.
    """
    el = Element(
        "InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 0.25}
    )
    root = Element("data")
    root.append(el)
    info = BD.load(root)
    assert info.inhibitorySynapsesDistance == 0.25


def test_syn_properties(good_recipe, bad_recipe):
    """Test that the `type` referred to by the recipe starts with either E or I
    """
    list(good_recipe.load_group(good_recipe.xml.find("SynapsesProperties"), SP))
    # caplog.clear()
    with pytest.raises(ValueError):
        list(bad_recipe.load_group(bad_recipe.xml.find("SynapsesProperties"), SP))


def test_syn_distances_repr():
    """Test that xml->python->xml is identical.
    """
    el = Element(
        "InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 6}
    )
    root = Element("data")
    root.append(el)
    info = BD.load(root)
    rpr1 = '<InitialBoutonDistance defaultExcSynapsesDistance="25.0" defaultInhSynapsesDistance="6">'
    rpr2 = '<InitialBoutonDistance defaultInhSynapsesDistance="6" defaultExcSynapsesDistance="25.0">'
    assert str(info) in (rpr1, rpr2)


def test_touch_rules(good_recipe):
    """Test touch rules: make sure that all morphology types are covered.
    """
    TR.load(good_recipe.xml, MTYPES, [])
    # caplog.clear()
    with pytest.raises(ValueError):
        TR.load(good_recipe.xml, MTYPES + ["FOOBAR"], [], strict=True)
