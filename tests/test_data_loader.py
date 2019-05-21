#!/usr/bin/env python
"""
A Pyton executable testing data loader
"""
import os.path
from spykfunc.data_loader import NeuronDataSpark
from spykfunc.dataio.cppneuron import NeuronData, MVD_Morpho_Loader
from spykfunc.recipe import Recipe
import logging

CURDIR = os.path.dirname(__file__)
mvd_file = os.path.join(CURDIR, "v5circuit.mvd3")
morpho_dir = os.path.join(CURDIR, "circuit_1000n/morphologies/h5")
mega_recipe = os.path.join(CURDIR, "v5builderRecipeAllPathways.xml")


def test_loader():
    # Hack to create a NeuronDataSpark without init (which would start a spark session)
    nrData = NeuronData.__new__(NeuronDataSpark)
    nrData.set_loader(MVD_Morpho_Loader(mvd_file, morpho_dir))

    nrData.load_globals()
    recipe = Recipe(mega_recipe)

    import spykfunc
    print(spykfunc)
    print(nrData)


if __name__ == "__main__":
    logging.warning("Started")
    test_loader()
    logging.warning("Finished")
