"""Tests relating to SONATA used for edge input
"""
import h5py
import os
import numpy
import pytest
import sparkmanager as sm
from conftest import ARGS, DATADIR, create_functionalizer
from spykfunc.io.circuit import BRANCH_COLUMNS, BRANCH_MAX_VALUE_SONATA, EdgeData
from spykfunc.utils.conf import Configuration


@pytest.fixture(name="edges_w_branch_type")
def edges_w_branch_type(tmp_path_factory):
    def f(afferent_values=None, efferent_values=None, size=100):
        filename = tmp_path_factory.mktemp("sonadah") / "edges.h5"
        with h5py.File(filename, "w") as fd:
            fd.create_dataset("/edges/default/edge_type_id", data=numpy.zeros(size))
            fd.create_dataset("/edges/default/source_node_id", data=numpy.zeros(size))
            fd.create_dataset("/edges/default/target_node_id", data=numpy.zeros(size))
            fd.create_group("/edges/default/0")
            rng = numpy.random.default_rng(123)
            if afferent_values:
                fd.create_dataset(
                    "/edges/default/0/afferent_section_type",
                    data=rng.choice(afferent_values, size=size),
                )
            if efferent_values:
                fd.create_dataset(
                    "/edges/default/0/efferent_section_type",
                    data=rng.choice(efferent_values, size=size),
                )
        return filename, "default"

    return f


def test_branch_shift(edges_w_branch_type):
    conf = Configuration(".")
    sm.create("test_shift", conf("spark"))

    loader = EdgeData._load_sonata(*edges_w_branch_type([2], [2, 3, 4]))
    values = set()
    df = loader()
    for colname in BRANCH_COLUMNS:
        values.update(df.select(colname).toPandas()[colname].unique())
    assert values == {1, 2, 3}

    loader = EdgeData._load_sonata(*edges_w_branch_type())
    loader()


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
