"""Test the various filters
"""
import os
import pytest
import sparkmanager as sm
from conftest import DATADIR
from pathlib import Path
from fz_td_recipe import Recipe
from spykfunc.filters import DatasetOperation


@pytest.mark.slow
def test_property_assignment(fz):
    fz.circuit.df = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    fz.recipe.seeds.synapseSeed = "123"
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target, None
    )[0]
    data = fltr.apply(fz.circuit)
    have = data.select("src", "dst", "syn_type_id", "syn_property_rule")
    want = (
        sm.read.parquet(os.path.join(DATADIR, "syn_prop_out.parquet"))
        .groupBy("pre_gid", "post_gid", "synapseType")
        .count()
    )
    comp = have.alias("h").join(
        want.alias("w"), [have.src == want.pre_gid, have.dst == want.post_gid]
    )
    assert comp.where("(h.syn_type_id + h.syn_property_rule) != w.synapseType").count() == 0


@pytest.mark.slow
def test_property_positive_u(fz):
    fz.circuit.df = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    fz.recipe.seeds.synapseSeed = "123"
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target, None
    )[0]
    data = fltr.apply(fz.circuit)
    assert data.where("u < 0").count() == 0


@pytest.mark.slow
def test_property_u_hill(fz):
    fz.circuit.df = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    fz.recipe = Recipe(str(Path(__file__).parent / "recipe" / "synapse_properties_uhill_all.xml"))
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target, None
    )[0]
    data = fltr.apply(fz.circuit)
    inhibitory = data.where("type = 'I2'").count()
    inhibitory_test = data.where("type = 'I2' and abs(uHillCoefficient - 1.46) < 0.01").count()
    assert inhibitory == inhibitory_test
