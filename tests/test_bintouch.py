from spykfunc.dataio import touches
import os.path
import numpy
numpy.set_printoptions(formatter={'int': '{: 4d}'.format,
                                  'float': '{: 7.2f}'.format})

BASE_DIR = os.path.join(os.path.dirname(__file__), "circuitBuilding_1000neurons/BlueDetector_output")
_DEBUG = False


def test_bin_touch():
    assert os.path.isdir(BASE_DIR), "{} doesnt exist. please create link to TestData".format(BASE_DIR)

    # Create obj
    touch = touches.TouchInfo(os.path.join(BASE_DIR, "touches.0"))
    touch_window = touch.touches[5:20]
    record = touch_window[0]

    if _DEBUG:
        print(touch.header)
        print(len(touch.neuron_stats))
        for t in touch_window:
            print(t)

    assert len(touch_window) == 15
    assert len(record) == 4, "Size 4"
    assert numpy.array_equal(record[0], [24, 161, 58])
    assert numpy.array_equal(record[1], [14, 77, 27])
    assert record[2] == 18
    assert numpy.allclose(record[3], [ 720.94, 3.22, 2.19], 1.e-2)


if __name__ == "__main__":
    _DEBUG = True
    test_bin_touch()
