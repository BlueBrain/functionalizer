"""Tests relating to SONATA used for edge input
"""
import os
import pytest
from conftest import ARGS, DATADIR
from spykfunc.functionalizer import Functionalizer


@pytest.mark.slow
def test_sonata_properties(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("sonata_properties")
    cdir = tmpdir.join("check")
    odir = tmpdir.join("out")
    fz = Functionalizer(
        filters=["SynapseProperties"], checkpoint_dir=str(cdir), output_dir=str(odir)
    ).init_data(*ARGS[:-1], edges=(os.path.join(DATADIR, "edges.h5"), "default"))
    fz.process_filters()

    assert "delay" in fz.circuit.df.columns
    assert "gsyn" in fz.circuit.df.columns
    assert "u" in fz.circuit.df.columns
    assert "d" in fz.circuit.df.columns
    assert "f" in fz.circuit.df.columns
    assert "dtc" in fz.circuit.df.columns
    assert "nrrp" in fz.circuit.df.columns
