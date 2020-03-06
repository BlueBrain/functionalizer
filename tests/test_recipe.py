"""Test recipe class and functionality
"""
from pathlib import Path
from xml.etree.ElementTree import Element

import pytest

from spykfunc.recipe import Recipe
from spykfunc.filters.implementations.bouton_distance import InitialBoutonDistance as BD
from spykfunc.filters.implementations.gap_junction import GapJunctionProperty as GJ
from spykfunc.filters.implementations.synapse_properties import SynapsesProperty as SP
from spykfunc.filters.implementations.synapse_properties import SynapsesClassification as SC
from spykfunc.filters.implementations.touch import TouchRule as TR


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


def recipe(stub: str) -> Recipe:
    """Centralized recipe location handling
    """
    return Recipe(str(Path(__file__).parent / "recipes" / (stub + ".xml")))


@pytest.fixture
def bad_recipe():
    return recipe("faulty")


@pytest.fixture
def good_recipe():
    return recipe("v5")


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


def test_syn_properties_basic(good_recipe, bad_recipe):
    """Test that the `type` referred to by the recipe starts with either E or I
    """
    props = SP.load(good_recipe.xml)
    assert len(props) == 75
    assert all(p.axonalConductionVelocity == 300 for p in props)
    assert all(p.neuralTransmitterReleaseDelay == 0.1 for p in props)

    with pytest.raises(ValueError):
        SP.load(bad_recipe.xml)


def test_syn_properties_basic_defaults():
    """Test basic synapse_property default settings during classification
    """
    props = SP.load(recipe("synapse_properties").xml)
    assert props[0].axonalConductionVelocity == 666
    assert props[0].neuralTransmitterReleaseDelay == -13

    assert props[-1].axonalConductionVelocity == 123
    assert props[-2].neuralTransmitterReleaseDelay == 0


def test_syn_properties_optional_none():
    """Test optional synapse properties. Need to be defined for all.
    """
    data = SC.load(recipe("synapse_properties").xml)
    assert not hasattr(data[0], "gsynSRSF")
    assert not hasattr(data[0], "uHillCoefficient")


def test_syn_properties_optional_some():
    """Test optional synapse properties. Need to be defined for all.
    """
    with pytest.raises(ValueError):
        SC.load(recipe("synapse_properties_uhill_some").xml)


def test_syn_properties_optional_all():
    """Test optional synapse properties, defined for all properties.
    """
    data = SC.load(recipe("synapse_properties_uhill_all").xml)
    assert hasattr(data[0], "gsynSRSF")
    assert hasattr(data[0], "uHillCoefficient")


def test_syn_distances_repr():
    """Test that xml->python->xml is identical.
    """
    el = Element(
        "InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 6}
    )
    root = Element("data")
    root.append(el)
    info = BD.load(root)
    rpr1 = '<InitialBoutonDistance excitatorySynapsesDistance="25.0" inhibitorySynapsesDistance="6.0">'
    rpr2 = '<InitialBoutonDistance inhibitorySynapsesDistance="6.0" excitatorySynapsesDistance="25.0">'
    assert str(info) in (rpr1, rpr2)


def test_touch_rules(good_recipe):
    """Test touch rules: make sure that all morphology types are covered.
    """
    TR.load(good_recipe.xml, MTYPES, MTYPES)
    # caplog.clear()
    with pytest.raises(ValueError):
        TR.load(good_recipe.xml, MTYPES + ["FOOBAR"], MTYPES, strict=True)


def test_gap_junction():
    """Test that the gap junction conductance is set right
    """
    recipe = Recipe(str(Path(__file__).parent / "recipes" / "gap_junctions.xml"))
    assert GJ.load(recipe.xml)[0].gsyn == 0.75
