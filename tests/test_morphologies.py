"""Test morphology functions for equivalency with the C++ functionalizer
"""
from collections import defaultdict
from pathlib import Path
import numpy
import pytest

from spykfunc.io.morphologies import MorphologyDB


BRANCHES = [
    (
        "dend-tr050310_6_c4_axon-rp110630_P1_idC",
        [
            # fmt: off
            0, 132.18, 20.6616, 197.666, 11.7541, 43.959, 47.0888, 267.17, 20.7209,
            107.226, 9.33673, 169.664, 9.4362, 94.7953, 48.2851, 244.396, 21.4044,
            34.115, 31.5182, 75.279, 52.6377, 62.2925, 22.4175, 76.8159, 55.101,
            135.747, 30.5198, 19.908, 6.55923, 93.9804, 103.546, 107.535, 125.276,
            139.701, 113.91, 66.837, 33.1291, 328.225, 77.5597, 14.3771, 18.8147,
            2.33996, 64.3589, 68.572, 8.08045, 51.0868, 3.80301, 80.2843, 40.0352,
            49.4334, 71.856, 76.8566, 38.1878, 48.6568, 109.134, 14.2268, 26.6224,
            84.6673, 8.16457, 14.839, 102.282, 41.3356, 13.4225, 125.346, 8.4715,
            162.027, 12.5508, 11.6807, 173.684, 76.9058, 8.31051, 23.5777, 72.4474,
            124.921, 210.651, 9.77778, 7.74113, 148.665, 9.95863, 30.1846, 180, 180,
            17.0028, 210, 190, 15.1033, 70.785, 3.42925, 172.281, 69.3278, 13.3833,
            162.726, 153.663, 19.9406, 61.2161, 52.9709, 76.6491, 149.838, 10.5803,
            157.291, 6.67928, 20.9932, 173.029, 144.49, 2.97763, 109.941, 9.50827,
            170.88, 10.5124, 105.891, 153.587, 175.531, 40.1722, 4.27765, 46.7419,
            11.2988, 111.506, 93.4091, 45.1883, 9.48695, 63.2782, 12.2135, 17.5391,
            4.27125, 8.54578, 59.4224, 21.8769, 31.369, 24.8572, 42.2817, 24.8928,
            2.86545, 11.931, 13.2365, 6.41926, 31.7242, 45.9559, 4.46186, 81.2201,
            9.80629, 26.5642, 3.87344, 51.6063, 90.0313, 14.1999, 7.66007, 9.48332,
            138.413, 106.606, 323.416, 15.7057, 20.3323, 10.019, 30.7951, 17.3479,
            54.1447, 47.9378, 297.446, 124.14, 19.7865, 53.6463, 105.74, 271.465,
            203.393, 56.5392, 82.9948, 121.559, 75.5623, 246.45, 121.32, 33.6281,
            122.507, 129.817, 173.395, 36.2571, 171.008, 92.8878, 327.679, 48.4904,
            88.0602, 75.1607, 32.4518, 112.27, 48.4888, 35.4142, 148.699, 174.396,
            91.6863, 109.477,
            # fmt: on
        ],
    ),
    (
        "dend-tkb060123a2_ch2_ct_x_db_60x_2_axon-tkb060510b1_ch1_cc2_n_db_60x_1",
        [
            # fmt: off
            0, 71.0547, 3.75654, 54.0999, 37.1057, 5.37436, 483.896, 557.172, 554.995,
            94.1962, 412.639, 37.2518, 13.5508, 7.35681, 303.816, 24.0367, 48.6754,
            157.591, 6.5072, 304.277, 57.5945, 6.13939, 375.335, 47.9491, 44.4981,
            5.14659, 5.58165, 511.016, 17.7526, 12.0649, 9.74879, 71.1072, 79.0603,
            54.9319, 94.0531, 10, 9.99998, 25.1408, 102.447, 88.8251, 1.82648, 14.6064,
            15.2439, 88.5967, 13.6705, 103.458, 17.2938, 4.10089, 145.94, 18.9672, 70,
            20, 20, 40, 40, 20, 40, 40.0001, 40.0003, 30, 10.798, 5.30122, 2.26685,
            112.723, 2.68425, 113.871, 12.8848, 38.2569, 64.5713, 11.8212, 66.0622,
            17.3089, 6.85736, 42.3731, 25.429, 33.8111, 64.6123, 32.3638, 57.5178, 10,
            40, 42.6586, 39.9999, 40, 19.7414, 2.35286, 6.93979, 25.006, 14.2464,
            7.17661, 28.4848, 11.7129, 12.2503, 3.69727, 14.4968, 37.0986, 39.0241,
            32.353, 19.1593, 125.673, 518.464, 9.06394, 8.13279, 18.2162, 9.18307,
            207.21, 15.906, 89.4897, 165.644, 174.002, 14.6415, 10.0222, 12.4058,
            121.818, 90.119, 83.4715, 131.963, 6.14685, 22.212, 128.787, 30.7898,
            4.69236, 234.653, 43.1667, 9.80148, 12.6162, 89.4479, 8.28856, 41.0761,
            15.9793, 10.7143, 124.215, 44.8663, 83.3136, 107.414, 44.6673, 111.554,
            122.725, 29.9997, 30, 123.081, 94.9788, 124.554, 9.99987, 39.9999, 10.5897,
            103.791, 87.2502, 85.6715, 21.5828, 63.0463, 5.62465, 123.7313766,
            # fmt: on
        ],
    ),
]

LENGTHS = [
    (("tkb060509a2_ch3_mc_n_el_100x_1", 684, 5), 45.9962),
    (("tkb060509a2_ch3_mc_n_el_100x_1", 693, 7), 38.9572),
    (("tkb060509a2_ch3_mc_n_el_100x_1", 693, 12), 45.6227),
    (("tkb060509a2_ch3_mc_n_el_100x_1", 693, 16), 50.6435),
    (("rp110120_L5-3_idE", 505, 30), 74.6781),
    (("rp110120_L5-3_idE", 505, 37), 81.0722),
    (("rp110120_L5-3_idE", 505, 42), 86.4777),
    (("rp110120_L5-3_idE", 505, 47), 92.2397),
]

RADII = [
    ("C210301C1_cor", 13.5753),
    ("C251197A-I1", 13.2682),
    ("C280199C-I4", 14.7659),
    ("C290500B-I3", 14.0372),
    ("dend-C031000B-P3_axon-sm110120c1-2_INT_idD", 8.09863),
    ("dend-C080501A5_axon-C060114A2", 13.8904),
    ("dend-C090905B_axon-C251197A-P4", 9.8054),
]


@pytest.fixture
def morphos():
    return MorphologyDB(Path(__file__).parent / "circuit_1000n" / "morphologies" / "h5")


def test_branch_lengths(morphos):
    """Ensure that all branch lengths are in agreement

    This assumes the C functionalizer as reference point.
    """
    for name, branches in BRANCHES:
        assert numpy.allclose(
            [morphos.pathlengths(name, s.id + 1)[-1] for s in morphos[name].sections], branches[1:]
        )


def test_path_length(morphos):
    """Ensure that the path length is calculated correctly"""
    mine = numpy.array(
        [morphos.distance_to_soma(name, sec, seg) for (name, sec, seg), _ in LENGTHS]
    )
    funcz = numpy.array([length for _, length in LENGTHS])
    assert numpy.allclose(mine, funcz)


def test_soma_radii(morphos):
    """Ensure that soma radii are calculated correctly

    This assumes the C functionalizer as reference point.
    """
    mine = numpy.array([morphos.soma_radius(name) for name, _ in RADII])
    funcz = numpy.array([radius for _, radius in RADII])
    assert numpy.allclose(mine, funcz)
