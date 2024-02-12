"""Test gap-junction mode
"""

import copy
import numpy as np
import pandas
import pytest

from pathlib import Path

from pyspark.sql import functions as F
from fz_td_recipe import Recipe
from spykfunc.filters import DatasetOperation


# (src, dst), num_connections
DENDRO_DATA = [
    ((987, 990), 10),  # 6 with exact or abs() == 1 match
    ((975, 951), 8),  # 2 with exact or abs() == 1 match
]

# src, dst, [(pre_section, pre_segment)]
SOMA_DATA = [
    (872, 998, [(107, 69)]),
    (858, 998, [(129, 4), (132, 7)]),
    (812, 968, [(132, 18)]),
    (810, 983, [(43, 67), (147, 42), (152, 49)]),
]

SOMA_DATA_BIDIRECTIONAL = [
    (872, 998, []),
    (858, 998, [(129, 4)]),
    (812, 968, [(132, 18)]),
    (810, 983, []),
]


@pytest.mark.slow
def test_soma_distance(gj):
    """Verify that soma_distances are larger than soma radii.

    Also check that temporary columns are dropped.
    """
    circuit = copy.copy(gj.circuit)
    circuit.df = circuit.df.where("src == 873 and dst == 999")
    fltr = DatasetOperation.initialize(["SomaDistance"], None, None, None, gj.circuit.morphologies)[
        0
    ]
    res = fltr.apply(circuit)
    assert "valid_touch" not in res.schema
    assert res.count() == 36


@pytest.mark.slow
def test_soma_filter(gj):
    """Verify filter results based on the 1000 neuron test circuit.

    Matches the selection of dendro-soma touches.
    """
    query = "src == {} and dst == {} and afferent_section_id == 0"
    fltr = DatasetOperation.initialize(["GapJunction"], None, None, None, gj.circuit.morphologies)[
        0
    ]
    circuit = gj.circuit.df.withColumnRenamed("synapse_id", "efferent_junction_id").withColumn(
        "afferent_junction_id", F.col("efferent_junction_id")
    )

    for src, dst, expected in SOMA_DATA:
        df = circuit.where(query.format(src, dst)).toPandas()
        df = fltr._soma_filter(df)
        assert set(expected) == set(zip(df.efferent_section_id, df.efferent_segment_id))


@pytest.mark.slow
def test_soma_filter_bidirectional(gj):
    """Verify filter results based on the 1000 neuron test circuit.

    Ensures that dendro-soma touches are bi-directional.
    """
    query = (
        "src in ({0}, {1}) and dst in ({0}, {1}) and "
        "(afferent_section_id == 0 or efferent_section_id == 0)"
    )
    fltr = DatasetOperation.initialize(["GapJunction"], None, None, None, gj.circuit.morphologies)[
        0
    ]
    circuit = gj.circuit.df.withColumnRenamed("synapse_id", "efferent_junction_id").withColumn(
        "afferent_junction_id", F.col("efferent_junction_id")
    )

    for src, dst, expected in SOMA_DATA_BIDIRECTIONAL:
        df = circuit.where(query.format(src, dst)).toPandas()
        # with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        df = fltr._dendrite_match(df)
        # with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        df = fltr._soma_filter(df)
        assert 2 * len(expected) == len(df)


@pytest.mark.slow
def test_dendrite_sync(gj):
    """Verify that gap junctions are synchronized right"""
    query = "(src in {0} and dst in {0}) and afferent_section_id > 0"
    fltr = DatasetOperation.initialize(["GapJunction"], None, None, None, gj.circuit.morphologies)[
        0
    ]
    circuit = gj.circuit.df.withColumnRenamed("synapse_id", "efferent_junction_id").withColumn(
        "afferent_junction_id", F.col("efferent_junction_id")
    )

    for pair, expected in DENDRO_DATA:
        df = circuit.where(query.format(pair)).toPandas()
        df = fltr._dendrite_match(df)
        assert len(df) == expected

    df = fltr._dendrite_match(circuit.toPandas())
    for col in ("afferent_junction_id", "efferent_junction_id"):
        unique_counts = np.unique(np.unique(getattr(df, col), return_counts=True)[1])
        assert len(unique_counts) == 1
        assert unique_counts[0] == 1


@pytest.mark.slow
def test_dense_ids(gj):
    """Verify that all filters play nice together."""
    total = gj.circuit.df.count()
    fltrs = DatasetOperation.initialize(
        [
            "DenseID",
        ],
        None,
        None,
        None,
        gj.circuit.morphologies,
    )
    for f in fltrs:
        gj.circuit = f(gj.circuit)
    extrema = gj.circuit.df.agg(
        F.min("synapse_id").alias("lower"), F.max("synapse_id").alias("upper")
    ).collect()[0]
    assert extrema["lower"] == 0
    assert extrema["upper"] == total - 1
    assert gj.circuit.df.count() == total


@pytest.mark.slow
def test_touch_reduction(gj):
    """Verify that touch reduction hits the mark"""
    total = gj.circuit.df.count()
    fltrs = DatasetOperation.initialize(
        ["DenseID", "TouchReduction"],
        Recipe(str(Path(__file__).parent / "recipe" / "gap_junctions.xml")),
        None,
        None,
        None,
    )
    for f in fltrs:
        gj.circuit = f(gj.circuit)
    assert abs(gj.circuit.df.count() / float(total) - 0.5) < 0.01


@pytest.mark.slow
def test_gap_junctions(gj):
    """Verify that all filters play nice together."""
    fltrs = DatasetOperation.initialize(
        ["SomaDistance", "GapJunction", "GapJunctionProperties"],
        Recipe(str(Path(__file__).parent / "recipe" / "gap_junctions.xml")),
        None,
        None,
        gj.circuit.morphologies,
    )
    for f in fltrs:
        gj.circuit = f(gj.circuit)
    assert gj.circuit.df.count() > 0
    assert "gsyn" in gj.circuit.df.columns
    assert gj.circuit.df.first()["gsyn"] == 0.75
