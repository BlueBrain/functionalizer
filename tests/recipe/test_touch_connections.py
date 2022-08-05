"""Tests the touch connections
"""

from io import StringIO
from recipe import Recipe


def test_rule_expansion():
    r = Recipe(StringIO(SOME_PROPERTIES))
    expansion = list(r.connection_rules[0]({"toMType": ["BAR"], "toRegion": ["NONE"]}))
    assert len(expansion) == 1
    assert set(expansion[0][0]) == set(["toMType", "toRegion"])


def test_rule_requirements():
    r = Recipe(StringIO(SOME_PROPERTIES))
    assert r.connection_rules.required == ["toMType", "toRegion"]


def test_rule_validity():
    r = Recipe(StringIO(SOME_PROPERTIES))
    assert r.connection_rules.validate({"toMType": ["BAR"], "toRegion": ["NONE"]})


def test_rule_invalidity():
    r = Recipe(StringIO(SOME_PROPERTIES))
    assert not r.connection_rules.validate({"toMType": ["FOO", "BAR"], "toRegion": ["NONE"]})


def test_rule_matrix():
    r = Recipe(StringIO(SOME_PROPERTIES))
    cr = r.connection_rules
    matrix = cr.to_matrix(
        {
            "fromMType": ["bar", "foo"],
            "toMType": ["BAR", "BAZ"],
            "toRegion": ["NONE", "SOME", "MANY"],
        }
    )
    assert len(matrix) == 6  # len(toMType) * len(toRegion)
    # Element order: ["NONE" x ["BAR", "BAZ"]…
    assert cr[matrix[0]].bouton_reduction_factor == 0.9
    assert cr[matrix[1]].bouton_reduction_factor == 0.3
    assert cr[matrix[2]].bouton_reduction_factor == 0.6
    assert cr[matrix[3]].bouton_reduction_factor == 0.3
    assert cr[matrix[4]].bouton_reduction_factor == 0.6
    assert cr[matrix[5]].bouton_reduction_factor == 0.3


PARAMETERS_1 = 'bouton_reduction_factor="0.9" pMu_A="0.9" p_A="0.9"'
PARAMETERS_2 = 'bouton_reduction_factor="0.6" pMu_A="0.9" p_A="0.9"'
PARAMETERS_3 = 'bouton_reduction_factor="0.3" pMu_A="0.9" p_A="0.9"'

SOME_PROPERTIES = f"""\
<?xml version="1.0"?>
<blueColumn>
  <ConnectionRules>
    <mTypeRule fromMType="*" toMType="B*" {PARAMETERS_3} />
    <mTypeRule fromMType="*" toMType="BAR" {PARAMETERS_2} />
    <mTypeRule fromMType="*" toMType="BAR" toRegion="NONE" {PARAMETERS_1} />
  </ConnectionRules>
</blueColumn>
"""
