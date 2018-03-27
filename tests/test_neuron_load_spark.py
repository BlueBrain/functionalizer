import os.path
from spykfunc.dataio.cppneuron import MVD_Morpho_Loader
from spykfunc.data_loader import NeuronDataSpark
import sparkmanager as sm

BASE_DIR = os.path.dirname(__file__)
mvd_file = os.path.join(BASE_DIR, "circuit_1000n/circuit.mvd3")
morpho_dir = os.path.join(BASE_DIR, "circuit_1000n/morphologies/h5")


def test_load_neuron_morpos():
    assert os.path.isfile(mvd_file)
    sm.create("test_neuron_load")
    sf = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir))
    sf.load_mvd_neurons_morphologies()
    sf.neuronDF.show()


if __name__ == "__main__":
    test_load_neuron_morpos()
