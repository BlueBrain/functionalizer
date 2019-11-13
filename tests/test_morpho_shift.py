"""Test the shifting of synapses of ChC cells et al.
"""
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock

import pyspark.sql.functions as F
import pytest
import sparkmanager as sm

from spykfunc.circuit import Circuit

NEURONS = [
    u'{"layer":23,"id":39167,"mtype_i":8,"mtype":"L23_CHC","electrophysiology":4,'
    u'"syn_class_index":1,"position":[933.0420086834877,1816.8584704754185,510.11526138663635],'
    u'"rotation":[0.0,0.9907887468577957,0.0,-0.13541661308701744],'
    u'"morphology":"rp140328_ChC_4_idA_-_Scale_x1.000_y1.050_z1.000_-_Clone_4","layer_i":5}',
    u'{"layer":23,"id":101,"mtype_i":108,"mtype":"L24_CHB","electrophysiology":4,'
    u'"syn_class_index":1,"position":[933.0420086834877,1816.8584704754185,510.11526138663635],'
    u'"rotation":[0.0,0.9907887468577957,0.0,-0.13541661308701744],'
    u'"morphology":"rp140328_ChC_4_idA_-_Scale_x1.000_y1.050_z1.000_-_Clone_4","layer_i":5}',
    u'{"layer":4,"id":42113,"mtype_i":18,"mtype":"L3_TPC:A","electrophysiology":5,'
    u'"syn_class_index":0,"position":[943.2136315772983,1726.1433241483917,496.33558039342364],'
    u'"rotation":[0.0,-0.5188810149187988,0.0,0.8548464729744385],'
    u'"morphology":"dend-C240797B-P3_axon-sm110131a1-3_INT_idA_-_Clone_0","layer_i":2}'
]

TOUCHES = [
    u'{"src":101,"dst":42113,"pre_section":8,"pre_segment":2,"post_section":337,"post_segment":4,'
    u'"pre_offset":3.4448159,"post_offset":0.012562983,"distance_soma":107.856514,"branch_order":8}',
    u'{"src":39167,"dst":42113,"pre_section":8,"pre_segment":2,"post_section":337,"post_segment":4,'
    u'"pre_offset":3.4448159,"post_offset":0.012562983,"distance_soma":107.856514,"branch_order":8}',
    u'{"src":39167,"dst":42113,"pre_section":56,"pre_segment":29,"post_section":385,"post_segment":8,'
    u'"pre_offset":3.4924245,"post_offset":0.8277372,"distance_soma":261.3008,"branch_order":17}',
    u'{"src":39167,"dst":42113,"pre_section":196,"pre_segment":21,"post_section":338,"post_segment":7,'
    u'"pre_offset":4.610659,"post_offset":0.42679042,"distance_soma":169.00676,"branch_order":11}'
]
#
# pathways = sm.createDataFrame([((8 << 16) | 18, True)], ["pathway_i", "reposition"])


def mock_group():
    from collections import namedtuple
    rule = namedtuple("rule", ["type", "fromMType", "toMType"])
    yield rule("AIS", "*CHC", None)


def mock_mtypes(neurons):
    vals = [(r.mtype_i, r.mtype) for r in neurons.collect()]
    ms = [str(n) for n in range(max((i for i, _ in vals)) + 1)]
    for i, m in vals:
        ms[i] = m
    return ms


@pytest.mark.slow
def test_shift():
    """Make sure that ChC cells are treated right.

    Move synapses to AIS while keeping other touches untouched.
    """
    from spykfunc.filters.implementations.synapse_reposition import SynapsesReposition, SynapseReposition
    from spykfunc.dataio.morphologies import MorphologyDB

    sm.create("test_shift")

    neurons = sm.read.json(sm.parallelize(NEURONS))
    touches = sm.read.json(sm.parallelize(TOUCHES))

    recipe = MagicMock()

    population = MagicMock()
    population.df = neurons
    population.mtypes = mock_mtypes(neurons)

    c = Circuit(
        population,
        population,
        touches,
        recipe,
        Path(__file__).parent / "circuit_O1_partial" / "morphologies" / "h5"
    )

    touches.show()
    fltr = SynapseReposition(recipe, c.source, c.target, c.morphologies)
    fltr.reposition = fltr.convert_reposition(c.source, c.target, mock_group())
    result = fltr.apply(c)
    result.select(touches.columns).show()

    shifted = result.where(result.src == 39167)
    assert shifted.select("post_section").distinct().rdd.keys().collect() == [1]
    assert shifted.select("post_segment").distinct().rdd.keys().collect() == [0]
    assert shifted.select("post_offset").distinct().rdd.keys().collect() == [0.5]

    untouched = result.where(result.src == 101)
    assert untouched.select("post_section").distinct().rdd.keys().collect() == [337]
    assert untouched.select("post_segment").distinct().rdd.keys().collect() == [4]
    assert untouched.select("post_offset").distinct().rdd.keys().collect() == [0.012562983]
