"""Test the various filters"""

from pathlib import Path

import pytest
from conftest import CIRCUIT_CONFIG, DATADIR
from fz_td_recipe import Recipe

import sparkmanager as sm
from functionalizer.filters import DatasetOperation


@pytest.mark.slow
def test_property_assignment(fz):
    fz.circuit.df = sm.read.parquet(str(DATADIR / "syn_prop_in.parquet"))
    fz.recipe.set("seed", 123)
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target
    )[0]
    data = fltr.apply(fz.circuit)
    have = data.select("src", "dst", "syn_type_id", "syn_property_rule")
    want = (
        sm.read.parquet(str(DATADIR / "syn_prop_out.parquet"))
        .groupBy("pre_gid", "post_gid", "synapseType")
        .count()
    )
    comp = have.alias("h").join(
        want.alias("w"), [have.src == want.pre_gid, have.dst == want.post_gid]
    )
    assert comp.where("(h.syn_type_id + h.syn_property_rule) != w.synapseType").count() == 0


@pytest.mark.slow
def test_property_positive_u(fz):
    fz.circuit.df = sm.read.parquet(str(DATADIR / "syn_prop_in.parquet"))
    fz.recipe.set("seed", 123)
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target
    )[0]
    data = fltr.apply(fz.circuit)
    assert data.where("u_syn < 0").count() == 0


@pytest.mark.slow
def test_property_u_hill(fz):
    fz.circuit.df = sm.read.parquet(str(DATADIR / "syn_prop_in.parquet"))
    fz.recipe = Recipe(
        Path(__file__).parent / "recipe" / "recipe_uhill.json",
        CIRCUIT_CONFIG,
        (None, None),
    )
    fltr = DatasetOperation.initialize(
        ["SynapseProperties"], fz.recipe, fz.circuit.source, fz.circuit.target
    )[0]
    data = fltr.apply(fz.circuit)
    inhibitory = data.where("class = 'I2'").count()
    inhibitory_test = data.where("class = 'I2' and abs(u_hill_coefficient - 1.46) < 0.01").count()
    assert inhibitory == inhibitory_test
