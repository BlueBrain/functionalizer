"""Test the various filters
"""
import os
import pytest
import sparkmanager as sm
from conftest import DATADIR
from spykfunc.filters import DatasetOperation


@pytest.mark.slow
def test_property_assignment(fz):
    fz.circuit.df = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    fz.recipe.xml.find("Seeds").attrib['synapseSeed'] = "123"
    fltr = DatasetOperation.initialize(["SynapseProperties"],
                                       fz.recipe,
                                       fz.circuit.source,
                                       fz.circuit.target,
                                       None)[0]
    data = fltr.apply(fz.circuit)
    have = data.select("pre_gid", "post_gid", "synapseType")
    want = sm.read.parquet(os.path.join(DATADIR, "syn_prop_out.parquet")) \
        .groupBy("pre_gid", "post_gid", "synapseType").count()
    comp = have.alias("h").join(want.alias("w"),
        [have.pre_gid == want.pre_gid, have.post_gid == want.post_gid])
    assert comp.where("h.synapseType != w.synapseType").count() == 0


@pytest.mark.slow
def test_property_positive_u(fz):
    fz.circuit.df = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    fz.recipe.xml.find("Seeds").attrib['synapseSeed'] = "123"
    fltr = DatasetOperation.initialize(["SynapseProperties"],
                                       fz.recipe,
                                       fz.circuit.source,
                                       fz.circuit.target,
                                       None)[0]
    data = fltr.apply(fz.circuit)
    assert data.where('u < 0').count() == 0
