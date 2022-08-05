"""Test recipe class
"""
from io import StringIO
from lxml import etree
from pathlib import Path

import pytest

from recipe import Recipe


def load_recipe(stub: str, strict: bool = True) -> Recipe:
    """Centralized recipe location handling"""
    return Recipe(str(Path(__file__).parent / "data" / (stub + ".xml")), strict)


@pytest.fixture
def legacy_recipe():
    return load_recipe("v5", strict=False)


def test_load_legacy_xml(legacy_recipe):
    """Test recipe reading"""
    assert legacy_recipe.seeds.synapseSeed == 4236279


@pytest.fixture
def modern_recipe():
    return load_recipe("v7")


def test_load_modern_xml(modern_recipe):
    """Test recipe reading"""
    assert modern_recipe.structural_spine_lengths[0].spineLength == 2.5


def test_validate_modern_xml(modern_recipe):
    """Test recipe validation"""
    assert modern_recipe.validate(dict(fromMType=MTYPES_V7, toMType=MTYPES_V7))


def test_validate_modern_xml_incomplete_mtypes(modern_recipe):
    """Test recipe validation"""
    assert not modern_recipe.validate(dict(fromMType=MTYPES_V7[:-5], toMType=MTYPES_V7[:-5]))


def test_nonexisting_recipe():
    with pytest.raises(IOError):
        load_recipe("void")


def test_invalid_recipe(tmp_path):
    fd = tmp_path / "foo.xml"
    fd.write_text("<?xml?>\n<foo<bar>>")
    with pytest.raises(etree.XMLSyntaxError):
        Recipe(str(fd))


def test_recipe_with_additional_attributes(tmp_path):
    fd = tmp_path / "foo.xml"
    fd.write_text(RECIPE_SPURIOUS_ATTRIBUTES)
    with pytest.raises(AttributeError):
        Recipe(str(fd))
    Recipe(str(fd), strict=False)


def test_recipe_with_additional_default_attributes(tmp_path):
    fd = tmp_path / "foo.xml"
    fd.write_text(RECIPE_SPURIOUS_DEFAULT_ATTRIBUTES)
    with pytest.raises(AttributeError):
        Recipe(str(fd))
    Recipe(str(fd), strict=False)


def test_split_recipe(tmp_path):
    fd = tmp_path / "foo.xml"
    fd.write_text(RECIPE_CONNECTIVITY_ONLY)
    rcp = Recipe(str(fd))

    conn = StringIO()
    rest = StringIO()

    rcp.dump(rest, conn)

    conn.seek(0)
    rest.seek(0)

    recipe = rest.read()

    assert conn.read() == RECIPE_CONNECTIVITY_EXPECTED
    assert "ConnectionRules" not in recipe
    assert "ENTITY connectivity" in recipe
    assert "&connectivity;" in recipe
    assert "UNKNOWN" in recipe


def test_split_recipe_read_again(tmp_path):
    fn = tmp_path / "foo.xml"
    fn.write_text(RECIPE_CONNECTIVITY_ONLY)
    rcp = Recipe(str(fn))

    outn = str(tmp_path / "write.xml")
    outo = str(tmp_path / "write_more.xml")

    with open(outn, "w") as f1, open(outo, "w") as f2:
        rcp.dump(f1, f2)

    rcp = Recipe(outn)
    assert rcp.connection_rules

    with open(outo) as fd:
        assert fd.read() == RECIPE_CONNECTIVITY_EXPECTED


RECIPE_SPURIOUS_ATTRIBUTES = """\
<?xml version="1.0"?>
<blueColumn>
  <NeuronTypes>
    <StructuralType id="L1_DAC" spineLength="2.5" foobar="spam"/>
  </NeuronTypes>
</blueColumn>
"""

RECIPE_SPURIOUS_DEFAULT_ATTRIBUTES = """\
<?xml version="1.0"?>
<blueColumn>
  <NeuronTypes foobar="spam">
    <StructuralType id="L1_DAC" spineLength="2.5"/>
  </NeuronTypes>
</blueColumn>
"""

RECIPE_CONNECTIVITY_ONLY = """\
<?xml version='1.0' encoding='UTF-8'?>
<blueColumn>
  <ConnectionRules>
    <mTypeRule from="L1" to="HLT" p_A="1.0" bouton_reduction_factor="1.0" pMu_A="0.0"/>
  </ConnectionRules>
</blueColumn>
"""

RECIPE_CONNECTIVITY_EXPECTED = """\
<ConnectionRules>
  <rule fromMType="L1" toMType="HLT" p_A="1.0" bouton_reduction_factor="1.0" pMu_A="0.0" />
</ConnectionRules>\
"""

MTYPES_V7 = [
    "L1_DAC",
    "L1_HAC",
    "L1_LAC",
    "L1_NGC-DA",
    "L1_NGC-SA",
    "L1_SAC",
    "L23_BP",
    "L23_BTC",
    "L23_CHC",
    "L23_DBC",
    "L23_LBC",
    "L23_MC",
    "L23_NBC",
    "L23_NGC",
    "L23_SBC",
    "L2_IPC",
    "L2_TPC:A",
    "L2_TPC:B",
    "L3_TPC:A",
    "L3_TPC:C",
    "L4_BP",
    "L4_BTC",
    "L4_CHC",
    "L4_DBC",
    "L4_LBC",
    "L4_MC",
    "L4_NBC",
    "L4_NGC",
    "L4_SBC",
    "L4_SSC",
    "L4_TPC",
    "L4_UPC",
    "L5_BP",
    "L5_BTC",
    "L5_CHC",
    "L5_DBC",
    "L5_LBC",
    "L5_MC",
    "L5_NBC",
    "L5_NGC",
    "L5_SBC",
    "L5_TPC:A",
    "L5_TPC:B",
    "L5_TPC:C",
    "L5_UPC",
    "L6_BP",
    "L6_BPC",
    "L6_BTC",
    "L6_CHC",
    "L6_DBC",
    "L6_HPC",
    "L6_IPC",
    "L6_LBC",
    "L6_MC",
    "L6_NBC",
    "L6_NGC",
    "L6_SBC",
    "L6_TPC:A",
    "L6_TPC:C",
    "L6_UPC",
]
