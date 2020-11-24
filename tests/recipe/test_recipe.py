"""Test recipe class and functionality
"""
from pathlib import Path

import pytest

from recipe import Recipe
from recipe.property import Property


def test_values():
    class Faker(Property):
        _attributes = {
            "a": 0
        }

    f = Faker()
    assert f.a == 0
    assert str(f) == '<Faker />'

    f = Faker(a=5)
    assert str(f) == '<Faker a="5" />'
    del f.a
    assert str(f) == '<Faker />'

    g = Faker(defaults={"a": 5})
    assert str(g) == '<Faker />'
    assert g.a == 5

    # ensure actual defaults did not change
    h = Faker()
    assert h.a == 0
    assert str(h) == '<Faker />'


def test_alias():
    class Faker(Property):
        _attributes = {
            "a": 0,
            "b": 2
        }
        _alias = {"c": "b"}

    f = Faker()
    assert f.a == 0
    assert str(f) == '<Faker />'

    g = Faker(c=5)
    assert str(g) == '<Faker b="5" />'
    del g.b
    assert g.b == 2
    assert str(g) == '<Faker />'

    g = Faker(c=5)
    assert str(g) == '<Faker b="5" />'
    del g.b
    assert g.b == 2
    assert str(g) == '<Faker />'


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


def load_recipe(stub: str) -> Recipe:
    """Centralized recipe location handling
    """
    return Recipe(str(Path(__file__).parent / "data" / (stub + ".xml")))


@pytest.fixture
def good_recipe():
    return load_recipe("v5")


def test_load_xml(good_recipe):
    """Test recipe reading
    """
    assert good_recipe.seeds.synapseSeed == 4236279
