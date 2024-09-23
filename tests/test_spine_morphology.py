from pathlib import Path

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest

from functionalizer.filters.implementations.spine_morphologies import (
    _create_spine_morphology_udf,
    _read_spine_morphology_attributes,
)


@pytest.fixture
def spine_path():
    return Path(__file__).parent / "circuit_1000n" / "morphologies" / "spines"


@pytest.fixture
def synapse_df():
    return pd.DataFrame(
        {
            "syn_type_id": [0, 100, 100, 0, 0, 100],
            "spine_length": [2.85, 2.651, 3.448, 2.974, 4.0, 5.0],
            "src_morph_class": ["PYR", "PYR", "PYR", "PYR", "INT", "INT"],
        }
    )


def test_spine_morphology_attributes(spine_path):
    names, df = _read_spine_morphology_attributes(spine_path)

    morpho_idx = np.argwhere(names == "spine_1")[0][0]
    npt.assert_almost_equal(df["spine_length"][df["spine_morphology"] == morpho_idx], [3.2322371])

    npt.assert_almost_equal(
        np.sort(df["spine_length"]),
        [
            2.3049822,
            2.4150288,
            2.6514323,
            2.6535368,
            2.6568942,
            2.8530729,
            2.9747055,
            3.1374645,
            3.2322371,
            3.4484346,
            3.5840621,
            3.8539293,
            3.9895439,
            4.0587602,
        ],
        decimal=6,
    )

    spine_name_id = [(names[a], b) for a, b in zip(df["spine_morphology"], df["spine_psd_id"])]
    assert sorted(spine_name_id) == [
        ("spine_1", 0),
        ("spine_12", 0),
        ("spine_14", 0),
        ("spine_14", 1),
        ("spine_15", 0),
        ("spine_2", 0),
        ("spine_2", 1),
        ("spine_3", 0),
        ("spine_3", 1),
        ("spine_4", 0),
        ("spine_5", 0),
        ("spine_6", 0),
        ("spine_6", 1),
        ("spine_7", 0),
    ]


def test_spine_morphology_assignment(spine_path, synapse_df):
    names, udf = _create_spine_morphology_udf(spine_path)
    result = next(udf([synapse_df]))

    spine_morphology = names[result["spine_morphology"]]
    spine_psd_ids = np.array(result["spine_psd_id"])
    spine_sharing_ids = np.array(result["spine_sharing_id"])
    npt.assert_equal(spine_morphology, ["", "spine_2", "spine_6", "", "", ""])
    npt.assert_equal(spine_psd_ids, [-1, 1, 1, -1, -1, -1])
    npt.assert_equal(spine_sharing_ids, [-1, -1, -1, -1, -1, -1])
