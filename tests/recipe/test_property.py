"""Test the abstract property functionality
"""
from lxml import etree

import pytest

from recipe.property import Property, PropertyGroup


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
        _attribute_alias = {"c": "b"}

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


def test_renaming():
    class Foo(Property):
        _name = "foo"
        _alias = "bar"
    class Snafu(PropertyGroup):
        _name = "snafu"
        _kind = Foo
    parser = etree.XMLParser(recover=True, remove_comments=True)
    tree = etree.fromstring(PROPERTY_ALIASED, parser)
    data = Snafu.load(tree)
    assert len(data) == 2
    for e in data:
        assert str(e) == "<foo />"


PROPERTY_ALIASED = """\
<?xml version="1.0"?>
<blubb>
  <snafu>
    <foo />
    <bar />
  </snafu>
  <blarg>
    <foo />
    <bar />
    <bar />
  </blarg>
</blubb>
"""
