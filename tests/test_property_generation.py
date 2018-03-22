"""Test the various filters
"""

import os
import pytest
import sparkmanager as sm
import spykfunc

from fixtures import DATADIR, ARGS, fz
from spykfunc.synapse_properties import compute_additional_h5_fields


@pytest.mark.slow
def test_property_assignment(fz):
    fz.circuit = sm.read.parquet(os.path.join(DATADIR, "syn_prop_in.parquet"))
    data = compute_additional_h5_fields(fz.circuit, fz._circuit.synapse_class_matrix, fz._circuit.synapse_class_properties)
    have = data.select("pre_gid", "post_gid", "synapseType").toPandas()
    want = sm.read.parquet(os.path.join(DATADIR, "syn_prop_out.parquet")).toPandas()
    assert have.equals(want)
