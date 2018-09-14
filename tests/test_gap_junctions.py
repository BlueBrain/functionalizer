"""Test gap-junction mode
"""
import pandas
import pytest

from pyspark.sql import functions as F
from spykfunc.filters import GapJunctionFilter, SomaDistanceFilter


# (src, dst), num_connections
DENDRO_DATA = [
    ((987, 990), 6),
    ((975, 951), 2),
]

# src, dst, [(pre_section, pre_segment)]
SOMA_DATA = [
    (872, 998, [(107, 69)]),
    (858, 998, [(129, 4), (132, 7)]),
    (812, 968, [(132, 18)]),
    (810, 983, [(43, 67), (147, 42), (152, 49)])
]


@pytest.mark.slow
def test_soma_distance(gj):
    """Verify that soma_distances are larger than soma radii.

    Also check that temporary columns are dropped.
    """
    fltr = SomaDistanceFilter(gj._circuit.morphologies)
    res = fltr.apply(gj.circuit.where("src == 873 and dst == 999"))
    assert 'valid_touch' not in res.schema
    assert res.count() == 36


@pytest.mark.slow
def test_soma_filter(gj):
    """Verify filter results based on the 1000 neuron test circuit.
    """
    query = "src == {} and dst == {} and post_section == 0"
    fltr = GapJunctionFilter(gj._circuit.morphologies)
    trim_touches = fltr._create_soma_filter_udf(gj.circuit)

    for src, dst, expected in SOMA_DATA:
        df = gj.circuit.where(query.format(src, dst)).toPandas()
        df = trim_touches.func(df)
        assert set(expected) == set(zip(df.pre_section, df.pre_segment))


@pytest.mark.slow
def test_dendrite_sync(gj):
    """Verify that gap junctions are synchronized right
    """
    query = "(src in {0} and dst in {0}) and post_section > 0"
    fltr = GapJunctionFilter(gj._circuit.morphologies)
    circuit = gj.circuit.withColumnRenamed('rand_idx', 'pre_junction') \
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
    fltrs = [
        SomaDistanceFilter(gj._circuit.morphologies),
        GapJunctionFilter(gj._circuit.morphologies)
    ]
    for f in fltrs:
        gj.circuit = f.apply(gj.circuit).cache()
    assert gj.circuit.count() > 0
