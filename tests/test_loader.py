#!/usr/bin/env python
"""
A Pyton executable showing/testing the use of NeuronData
"""

from spykfunc.dataio import cppneuron, common
import logging
import os
morphotool = None

CURDIR = os.path.dirname(__file__)
mvd_file = os.path.join(CURDIR, "circuit_1000n/circuit.mvd3")
morpho_dir = os.path.join(CURDIR, "circuit_1000n/morphologies/h5")


def test_loader():
    da = cppneuron.NeuronData()
    da.set_loader(cppneuron.MVD_Morpho_Loader(mvd_file, CURDIR))

    da.load_globals()
    print("nr neurons: {}".format(da.nNeurons))
    assert da.nNeurons == 1000

    print("Loading neurons")
    da.load_neurons(common.Range(250, 100))
    assert len(da.neurons) == 100  # Pure mem view, we had to implement __len__

    nrn = da.neurons[0]
    print(nrn)
    assert nrn[0] == 250

    if morphotool:
        if os.path.isdir(morpho_dir):
            print("Loading morphos")
            da.load_morphologies()
        else:
            logging.warning("Cant find path for loading morphologies. Please create a link to the TestData directory")
    else:
        print("Morphotool not available. Skipping morpho load test")


if __name__ == '__main__':
    test_loader()
