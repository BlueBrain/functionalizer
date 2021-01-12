"""Test the seed part
"""
import pytest

from io import StringIO
from recipe import Recipe


def test_default_distances():
    r = Recipe(StringIO(DISTANCES_ABSENT))
    assert r.bouton_distances.inhibitorySynapsesDistance == 5.0
    assert r.bouton_distances.excitatorySynapsesDistance == 25.0
    assert str(r) == DISTANCES_ABSENT


def test_full_distances():
    r = Recipe(StringIO(DISTANCES_FULL_OLD))
    assert r.bouton_distances.inhibitorySynapsesDistance == 4.0
    assert r.bouton_distances.excitatorySynapsesDistance == 24.0
    assert str(r) == DISTANCES_FULL_NEW

    assert r.bouton_distances.validate()


DISTANCES_ABSENT = """\
<?xml version="1.0"?>
<blueColumn>
</blueColumn>
"""

DISTANCES_FULL_OLD = """\
<?xml version="1.0"?>
<blueColumn>
  <InitialBoutonDistance defaultInhSynapsesDistance="4.0" defaultExcSynapsesDistance="24.0" />
</blueColumn>
"""

DISTANCES_FULL_NEW = """\
<?xml version="1.0"?>
<blueColumn>
  <InitialBoutonDistance inhibitorySynapsesDistance="4.0" excitatorySynapsesDistance="24.0" />
</blueColumn>
"""
