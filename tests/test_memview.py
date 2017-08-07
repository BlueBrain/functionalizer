import sys
import os.path
from tst_neuron_memview import NeuronData
sys.path.append(os.path.abspath(os.path.dirname(__file__)))


def test_memview():
    dloader = NeuronData()
    buf = dloader.neuronbuf

    arr = buf.asarray()
    assert len(arr) == 2

    print(arr[1])
    assert arr[1][0] == 11

    print("Changing internal struct")
    dloader.change_item_id(1, 222)
    print(arr[1])
    assert arr[1][0] == 222


if __name__ == '__main__':
    test_memview()
