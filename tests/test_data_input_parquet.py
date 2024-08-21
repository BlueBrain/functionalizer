"""Tests relating to SONATA used for edge input"""

import numpy
import pandas as pd
import pytest

import sparkmanager as sm
from functionalizer.io.circuit import BRANCH_COLUMNS, EdgeData
from functionalizer.utils.conf import Configuration


@pytest.fixture(name="edges_w_branch_type")
def edges_w_branch_type(tmp_path_factory):
    def f(afferent_values=None, efferent_values=None, size=100):
        filename = tmp_path_factory.mktemp("pahqued") / "edges.parquet"
        data = {
            "edge_type_id": numpy.zeros(size),
            "source_node_id": numpy.zeros(size),
            "target_node_id": numpy.zeros(size),
        }
        rng = numpy.random.default_rng(123)
        if afferent_values:
            data["afferent_section_type"] = rng.choice(afferent_values, size=size)
        if efferent_values:
            data["efferent_section_type"] = rng.choice(efferent_values, size=size)
        for k, d in data.items():
            print(k, len(d), size, d)
        pd.DataFrame(data).to_parquet(filename)
        return {}, filename

    return f


def test_branch_shift(edges_w_branch_type):
    conf = Configuration(".")
    sm.create("test_shift", conf("spark"))

    loader = EdgeData._load_parquet(*edges_w_branch_type([2], [2, 3, 4]))
    values = set()
    df = loader()
    for colname in BRANCH_COLUMNS:
        values.update(df.select(colname).toPandas()[colname].unique())
    assert values == {1, 2, 3}

    loader = EdgeData._load_parquet(*edges_w_branch_type([2], [0, 1, 2]))
    values = set()
    df = loader()
    for colname in BRANCH_COLUMNS:
        values.update(df.select(colname).toPandas()[colname].unique())
    assert values == {0, 1, 2}

    loader = EdgeData._load_parquet(*edges_w_branch_type([2], [0, 1, 2, 3, 4]))
    with pytest.raises(RuntimeError):
        loader()

    loader = EdgeData._load_parquet(*edges_w_branch_type([2], [1, 2]))
    with pytest.raises(RuntimeError):
        loader()

    loader = EdgeData._load_parquet(*edges_w_branch_type([0], [4]))
    with pytest.raises(RuntimeError):
        loader()

    loader = EdgeData._load_parquet(*edges_w_branch_type())
    loader()
