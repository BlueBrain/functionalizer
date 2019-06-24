"""Test gap-junction mode
"""
import copy
import pandas
import pytest

from pyspark.sql import functions as F
from spykfunc.filters import DatasetOperation


# (src, dst), num_connections
DENDRO_DATA = [
    ((987, 990), 10),  # 6 with exact or abs() == 1 match
    ((975, 951), 8),   # 2 with exact or abs() == 1 match
]

# src, dst, [(pre_section, pre_segment)]
SOMA_DATA = [
    (872, 998, [(107, 69)]),
    (858, 998, [(129, 4), (132, 7)]),
    (812, 968, [(132, 18)]),
    (810, 983, [(43, 67), (147, 42), (152, 49)])
]

SOMA_DATA_BIDIRECTIONAL = [
    (872, 998, []),
    (858, 998, [(129, 4)]),
    (812, 968, [(132, 18)]),
    (810, 983, [])
]


@pytest.mark.slow
def test_soma_distance(gj):
    """Verify that soma_distances are larger than soma radii.

    Also check that temporary columns are dropped.
    """
    circuit = copy.copy(gj.circuit)
    circuit.df = circuit.df.where("src == 873 and dst == 999")
    fltr = DatasetOperation.initialize(["SomaDistance"],
                                       None,
                                       None,
                                       gj.circuit.morphologies)[0]
    res = fltr.apply(circuit)
    assert 'valid_touch' not in res.schema
    assert res.count() == 36


@pytest.mark.slow
def test_soma_filter(gj):
    """Verify filter results based on the 1000 neuron test circuit.

    Matches the selection of dendro-soma touches.
    """
    query = "src == {} and dst == {} and post_section == 0"
    fltr = DatasetOperation.initialize(["GapJunction"],
                                       None,
                                       None,
                                       gj.circuit.morphologies)[0]
    circuit = gj.circuit.df.withColumnRenamed('synapse_id', 'pre_junction') \
                           .withColumn('post_junction', F.col('pre_junction'))
    trim_touches = fltr._create_soma_filter_udf(circuit)

    for src, dst, expected in SOMA_DATA:
        df = circuit.where(query.format(src, dst)).toPandas()
        df = trim_touches.func(df)
        assert set(expected) == set(zip(df.pre_section, df.pre_segment))

@pytest.mark.slow
def test_soma_filter_bidirectional(gj):
    """Verify filter results based on the 1000 neuron test circuit.

    Ensures that dendro-soma touches are bi-directional.
    """
    query = "src in ({0}, {1}) and dst in ({0}, {1}) and (post_section == 0 or pre_section == 0)"
    fltr = DatasetOperation.initialize(["GapJunction"],
                                       None,
                                       None,
                                       gj.circuit.morphologies)[0]
    circuit = gj.circuit.df.withColumnRenamed('synapse_id', 'pre_junction') \
                           .withColumn('post_junction', F.col('pre_junction'))
    match_touches = fltr._create_dendrite_match_udf(circuit)
    trim_touches = fltr._create_soma_filter_udf(circuit)

    for src, dst, expected in SOMA_DATA_BIDIRECTIONAL:
        df = circuit.where(query.format(src, dst)).toPandas()
        # with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        df = match_touches.func(df)
        # with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        df = trim_touches.func(df)
        assert 2 * len(expected) == len(df)


@pytest.mark.slow
def test_dendrite_sync(gj):
    """Verify that gap junctions are synchronized right
    """
    query = "(src in {0} and dst in {0}) and post_section > 0"
    fltr = DatasetOperation.initialize(["GapJunction"],
                                       None,
                                       None,
                                       gj.circuit.morphologies)[0]
    circuit = gj.circuit.df.withColumnRenamed('synapse_id', 'pre_junction') \
                           .withColumn('post_junction', F.col('pre_junction'))
    match_touches = fltr._create_dendrite_match_udf(circuit)

    for pair, expected in DENDRO_DATA:
        df = circuit.where(query.format(pair)).toPandas()
        df = match_touches.func(df)
        assert len(df) == expected


@pytest.mark.slow
def test_gap_junctions(gj):
    """Verify that all filters play nice together.
    """
    fltrs = DatasetOperation.initialize(
        [
            "SomaDistance",
            "GapJunction"
        ],
        None,
        None,
        gj.circuit.morphologies,
    )
    for f in fltrs:
        gj.circuit = f(gj.circuit)
    assert gj.circuit.df.count() > 0
