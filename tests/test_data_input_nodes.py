"""
"""
from pathlib import Path
import pytest
from conftest import DATADIR, create_functionalizer

from spykfunc.io import NodeData


@pytest.mark.slow
def test_full_node_file_loading(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("full_nodes")
    # create a functionalizer instance to set up spark
    fz = create_functionalizer(tmpdir, [])

    node_file = str(Path(DATADIR).parent / "circuit_proj66_tiny" / "nodes.h5")
    node_population = "neocortex_neurons"

    nodes = NodeData((node_file, node_population), ("", ""), tmpdir)
    df = nodes.df.toPandas()

    assert "mtype_i" in df
    assert "morph_class" in df
    assert "morphology" in df
    assert "sclass_i" in df
    assert set(df["morph_class"].unique()) == set(["INT", "PYR"])
    assert nodes.sclass_values == ["EXC", "INH"]


@pytest.mark.slow
def test_partial_node_file_loading(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("partial_nodes")
    # create a functionalizer instance to set up spark
    fz = create_functionalizer(tmpdir, [])

    node_file = str(Path(DATADIR).parent / "circuit_proj66_tiny" / "nodes_small.h5")
    node_population = "neocortex_neurons"

    nodes = NodeData((node_file, node_population), ("", ""), tmpdir)
    df = nodes.df.toPandas()

    assert set(df.columns) == set(["id", "etype_i", "mtype_i"])
