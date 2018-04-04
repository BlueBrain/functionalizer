"""Test the various filters
"""

import os
import pytest
import sparkmanager as sm
from fixtures import DATADIR
from fixtures import fz_fixture  # NoQA
from spykfunc.synapse_properties import compute_additional_h5_fields


@pytest.mark.slow
def test_property_assignment(fz):
    fz.circuit = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    data = compute_additional_h5_fields(fz.circuit,
                                        fz._circuit.reduced,
                                        fz._circuit.synapse_class_matrix,
                                        fz._circuit.synapse_class_properties)
    have = data.select("pre_gid", "post_gid", "synapseType")
    want = sm.read.parquet(os.path.join(DATADIR, "syn_prop_out.parquet")) \
        .groupBy("pre_gid", "post_gid", "synapseType").count()
    comp = have.alias("h").join(want.alias("w"),
        [have.pre_gid == want.pre_gid, have.post_gid == want.post_gid])
    assert comp.where("h.synapseType != w.synapseType").count() == 0
