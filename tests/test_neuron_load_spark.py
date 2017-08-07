import os.path
from pyspark.sql import SparkSession
from spykfunc.dataio.cppneuron import MVD_Morpho_Loader
from spykfunc.data_loader import NeuronDataSpark

BASE_DIR = os.path.dirname(__file__)
mvd_file = os.path.join(BASE_DIR, "circuitBuilding_1000neurons/circuits/circuit.mvd3")
morpho_dir = os.path.join(BASE_DIR, "/circuitBuilding_1000neurons/morphologies/h5")


def test_load_neuron_morpos():
    assert os.path.isfile(mvd_file)
    sf = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir), SparkSession.builder.getOrCreate(), )
    sf.load_mvd_neurons_morphologies(total_parts=10)
    sf.neuronDF.show()


if __name__ == "__main__":
    test_load_neuron_morpos()
