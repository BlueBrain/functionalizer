"""Tests relating to SONATA used for edge input
"""
import os
import pytest
from conftest import ARGS, DATADIR, create_functionalizer


@pytest.mark.slow
def test_sonata_properties(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("sonata_properties")
    cdir = tmpdir / "check"
    odir = tmpdir / "out"
    fz = create_functionalizer(tmpdir, ["SynapseProperties"]).init_data(
        *ARGS[:-1], edges=(os.path.join(DATADIR, "edges.h5"), "default")
    )
    fz.process_filters()

    assert "delay" in fz.circuit.df.columns
    assert "gsyn" in fz.circuit.df.columns
    assert "u" in fz.circuit.df.columns
    assert "d" in fz.circuit.df.columns
    assert "f" in fz.circuit.df.columns
    assert "dtc" in fz.circuit.df.columns
    assert "nrrp" in fz.circuit.df.columns
