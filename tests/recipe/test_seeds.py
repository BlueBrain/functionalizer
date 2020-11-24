"""Test the seed part
"""
import pytest

from io import StringIO
from recipe import Recipe


def test_default_seeds():
    r = Recipe(StringIO(SEEDS_ABSENT))
    assert r.seeds.synapseSeed == 0
    assert str(r) == SEEDS_ABSENT

    r.seeds.synapseSeed = "9"
    assert str(r) == SEEDS_PARTIAL.format("synapseSeed", 9)
    del r.seeds.synapseSeed
    assert str(r) == SEEDS_ABSENT

    with pytest.raises(ValueError):
        r.seeds.synapseSeed = "abc"


def test_partial_seeds():
    r = Recipe(StringIO(SEEDS_PARTIAL.format("synapseSeed", 123)))
    assert r.seeds.synapseSeed == 123

    r.seeds.synapseSeed = 5
    assert r.seeds.synapseSeed == 5
    assert r.seeds.columnSeed == 0

    del r.seeds.synapseSeed
    assert r.seeds.synapseSeed == 0
    r.seeds.columnSeed = 456

    assert str(r) == SEEDS_PARTIAL.format("columnSeed", 456)

    assert r.seeds.validate()


SEEDS_ABSENT = """\
<?xml version="1.0"?>
<blueColumn>
</blueColumn>
"""

SEEDS_PARTIAL = """\
<?xml version="1.0"?>
<blueColumn>
  <Seeds {}="{}" />
</blueColumn>
"""
