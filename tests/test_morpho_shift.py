"""Test the shifting of synapses of ChC cells et al.
"""

from collections import defaultdict
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock

import pyspark.sql.functions as F
import pytest
from fz_td_recipe import Recipe

import sparkmanager as sm

from spykfunc.circuit import Circuit
from spykfunc.schema import LEGACY_MAPPING
from spykfunc.utils.conf import Configuration


class MockLoader:
    def __init__(self, touches):
        self.df = touches
        self.input_size = 0

    @property
    def metadata(self):
        return dict()


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
    from spykfunc.filters.implementations.synapse_reposition import SynapseReposition
    from spykfunc.io.morphologies import MorphologyDB

    conf = Configuration(".")

    sm.create("test_shift", conf("spark"))

    neurons = sm.read.json(sm.parallelize(NEURONS))
    touches = sm.read.json(sm.parallelize(TOUCHES))

    for name, alias in LEGACY_MAPPING.items():
        if name in touches.columns:
            touches = touches.withColumnRenamed(name, alias)

    recipe = Recipe(StringIO(RECIPE))

    population = MagicMock()
    population.df = neurons
    population.mtype_values = mock_mtypes(neurons)

    c = Circuit(
        population,
        population,
        MockLoader(touches),
        [Path(__file__).parent / "circuit_O1_partial" / "morphologies" / "h5"],
    )

    fltr = SynapseReposition(recipe, c.source, c.target, c.morphologies)
    result = fltr.apply(c).select(touches.columns)

    shifted = result.where(result.src == 39167).toPandas()
    assert shifted["afferent_section_id"].unique() == [1]
    assert shifted["afferent_segment_id"].unique() == [0]
    (offset,) = shifted["afferent_segment_offset"].unique()
    assert abs(offset - 0.5) < 1e-5
    (fraction,) = shifted["afferent_section_pos"].unique()
    assert abs(fraction - 0.00353) < 1e-5
    (dist,) = shifted["distance_soma"].unique()
    assert abs(dist - 0.5) < 1e-5

    untouched_before = touches.where(touches.src == 101).toPandas()
    untouched_after = result.where(result.src == 101).toPandas()

    print(untouched_after.compare(untouched_before))
    assert untouched_after.equals(untouched_before)


RECIPE = """
<blueColumn>
  <SynapsesReposition>
    <shift fromMType="*CHC" toMType="*" type="AIS"/>
  </SynapsesReposition>
</blueColumn>
"""

NEURONS = [
    """
        [
          {
            "layer": 23,
            "id": 39167,
            "mtype_i": 8,
            "mtype": "L23_CHC",
            "electrophysiology": 4,
            "syn_class_index": 1,
            "position": [
              933.0420086834877, 1816.8584704754185, 510.11526138663635
            ],
            "rotation": [
              0, 0.9907887468577957, 0, -0.13541661308701744
            ],
            "morphology": "rp140328_ChC_4_idA_-_Scale_x1.000_y1.050_z1.000_-_Clone_4",
            "layer_i": 5
          },
          {
            "layer": 23,
            "id": 101,
            "mtype_i": 108,
            "mtype": "L24_CHB",
            "electrophysiology": 4,
            "syn_class_index": 1,
            "position": [
              933.0420086834877, 1816.8584704754185, 510.11526138663635
            ],
            "rotation": [
              0, 0.9907887468577957, 0, -0.13541661308701744
            ],
            "morphology": "rp140328_ChC_4_idA_-_Scale_x1.000_y1.050_z1.000_-_Clone_4",
            "layer_i": 5
          },
          {
            "layer": 4,
            "id": 42113,
            "mtype_i": 18,
            "mtype": "L3_TPC:A",
            "electrophysiology": 5,
            "syn_class_index": 0,
            "position": [
              943.2136315772983, 1726.1433241483917, 496.33558039342364
            ],
            "rotation": [
              0, -0.5188810149187988, 0, 0.8548464729744385
            ],
            "morphology": "dend-C240797B-P3_axon-sm110131a1-3_INT_idA_-_Clone_0",
            "layer_i": 2
          }
        ]
    """
]

TOUCHES = [
    """
        [
          {
            "src": 101,
            "dst": 42113,
            "pre_section": 8,
            "pre_segment": 2,
            "post_branch_type": 3,
            "post_section": 337,
            "post_section_fraction": 0.666,
            "post_segment": 4,
            "pre_offset": 3.4448159,
            "post_offset": 0.012562983,
            "distance_soma": 107.856514,
            "branch_order": 8
          },
          {
            "src": 39167,
            "dst": 42113,
            "pre_section": 8,
            "pre_segment": 2,
            "post_branch_type": 2,
            "post_section": 337,
            "post_section_fraction": 0.666,
            "post_segment": 4,
            "pre_offset": 3.4448159,
            "post_offset": 0.012562983,
            "distance_soma": 107.856514,
            "branch_order": 8
          },
          {
            "src": 39167,
            "dst": 42113,
            "pre_section": 56,
            "pre_segment": 29,
            "post_branch_type": 2,
            "post_section": 385,
            "post_section_fraction": 0.666,
            "post_segment": 8,
            "pre_offset": 3.4924245,
            "post_offset": 0.8277372,
            "distance_soma": 261.3008,
            "branch_order": 17
          },
          {
            "src": 39167,
            "dst": 42113,
            "pre_section": 196,
            "pre_segment": 21,
            "post_branch_type": 2,
            "post_section": 338,
            "post_section_fraction": 0.666,
            "post_segment": 7,
            "pre_offset": 4.610659,
            "post_offset": 0.42679042,
            "distance_soma": 169.00676,
            "branch_order": 11
          }
        ]
    """
]
