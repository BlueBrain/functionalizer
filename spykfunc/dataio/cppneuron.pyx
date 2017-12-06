from __future__ import absolute_import

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport utility
from libcpp cimport bool
from libc.stdlib cimport malloc, free
from libc.stdio cimport printf
from libc.string cimport memcpy
# Cy import
from .structbuf cimport StructBuffer
from .common cimport *
cimport MVD, MVD3
# Py import
import os
from collections import defaultdict
from .structbuf import StructType, TYPES

# MorphoLib
from . import morphotool

cdef int DEBUG=0

############### Data types ##################
ctypedef unsigned int uint

cdef struct NeuronInfo:
    uint id
    uint morphology
    uint electrophysiology
    uint syn_class_index
    double position[3]
    double rotation[4]


#Neuron map not copying data
cdef class NeuronBuffer(StructBuffer):
    block_t = StructType( (
        ("id", TYPES.UNSIGNED_INT),
        ("morphology", TYPES.UNSIGNED_INT),
        ("electrophysiology", TYPES.UNSIGNED_INT),
        ("syn_class_index", TYPES.UNSIGNED_INT),
        ("position", TYPES.Array(TYPES.DOUBLE,3)),
        ("rotation", TYPES.Array(TYPES.DOUBLE,4))
    ))

    # We must keep a reference to the object owning the buffer
    # to avoid it being garbage collected and free the memory block
    cdef NeuronData _neuron_data_owner

    def __init__(self, NeuronData data_owner):
        cdef Py_ssize_t s_size=sizeof(NeuronInfo)
        super(NeuronBuffer, self).__init__(self.block_t)
        self._neuron_data_owner = data_owner



##################################################
# Data structure holder and Loaders
##################################################

cdef class NeuronData(NeuronDataI):
    """
     A class to hold Neuron and Morphology data coming from C/C++ data-structures
     which interface them using memory-views and NumPy ndarrays
    """

    # Common objects defined in interface
    # cdef readonly NeuronLoaderI _loader
    # cdef readonly size_t nNeurons
    cdef readonly bool globals_loaded
    cdef readonly vector[string] mtypeVec
    cdef readonly vector[string] etypeVec
    cdef readonly vector[string] synaClassVec

    #Neuron info - Depending on the range
    cdef readonly NeuronBuffer neurons
    cdef readonly vector[string] neuronNames
    cdef readonly object nameMap
    #The actual C-alloc'ed array
    cdef NeuronInfo *_neurons

    # MorphologyDB -
    cdef public object morphologies


    def __init__(self, nr_neurons=0):
        self.neurons = NeuronBuffer(self)
        # Nr neurons can be initialized
        # so that one can load_neurons without load_globals again
        # which can be particularly interesting for distributed loading
        self.globals_loaded = False
        self.nNeurons = nr_neurons
        self.nameMap = defaultdict(list)
        self.morphologies = {}

    def set_loader(self, NeuronLoaderI loader):
        self._loader = loader

    def load_globals(self):
        self._loader.load_globals(self)
        self.globals_loaded = True

    def load_neurons(self, _Part_t part=None):
        # We delegate memory allocation to the loader, that we must free
        return self._loader.load_neurons(self, part)

    def load_morphologies(self, morphos=None):
        if morphos is None:
            morphos = self.nameMap.keys()
        return self._loader.load_morphologies(self, morphos)

    def set_name_map(self, dict name_map):
        self.nameMap = name_map

    def __dealloc__(self):
        # Dealloc data...
        free(self._neurons)



# ==================================================================================
cdef class MVD_Morpho_Loader(NeuronLoaderI):
    """
    A loader of Neurons and morphologies using bindings for MVD tool
    """
    # Paths
    cdef readonly string mvd_filename
    cdef readonly string morphology_dir

    def __init__(self, str mvd_filename, str morphology_dir):
        self.mvd_filename = mvd_filename.encode("utf8")
        self.morphology_dir = morphology_dir.encode("utf8")

    def load_globals(self, NeuronData neuron_data_dst):
        #Load and then set ptr
        cdef MVD3.MVD3File *f = new MVD3.MVD3File(self.mvd_filename)

        # --- Global info ---
        #Nr
        neuron_data_dst.nNeurons = f.getNbNeuron()
        #ETypes
        cdef vector[string] list_etypes = f.listAllEtypes()
        neuron_data_dst.etypeVec.swap(list_etypes)
        #MTypes
        cdef vector[string] list_mtypes = f.listAllMtypes()
        neuron_data_dst.mtypeVec.swap(list_mtypes)
        #SynapseClasses
        cdef vector[string] list_syn_class = f.listAllSynapseClass()
        neuron_data_dst.synaClassVec.swap(list_syn_class)
        del f


    def load_neurons(self, NeuronData neuron_data, _Part_t part=None):
        cdef MVD3.MVD3File *f = new MVD3.MVD3File(self.mvd_filename)
        cdef size_t total_neurons = neuron_data.nNeurons

        # Range
        cdef MVD.Range mvd_range
        if part is not None:
            mvd_range.offset, mvd_range.count = part.get_offset_count(total_neurons)
        cdef size_t subset_count = mvd_range.count or total_neurons
        cdef size_t subset_offset = mvd_range.offset

        # Neuron morphologies
        neuron_data.neuronNames = f.getMorphologies(mvd_range)

        # # Neuron Positions and Rotations
        cdef MVD.Positions *pos = new MVD.Positions(f.getPositions(mvd_range))
        cdef MVD.Rotations *rot = new MVD.Rotations(f.getRotations(mvd_range))

        # Neuron Mtype, Etype and
        cdef vector[size_t] index_mtypes = f.getIndexMtypes(mvd_range)
        cdef vector[size_t] index_etypes = f.getIndexEtypes(mvd_range)
        cdef vector[size_t] index_syn_class = f.getIndexSynapseClass(mvd_range)

        # Populate structure
        # if DEBUG: print("Alloc'ting", subset_count, "neuron structs")
        cdef NeuronInfo *_neurons = <NeuronInfo*>malloc( subset_count * sizeof(NeuronInfo) )

        cdef size_t cur_neuron
        for cur_neuron in range(subset_count):
            _neurons[cur_neuron].id = subset_offset + cur_neuron
            _neurons[cur_neuron].morphology = index_mtypes[cur_neuron]
            _neurons[cur_neuron].electrophysiology = index_etypes[cur_neuron]
            _neurons[cur_neuron].syn_class_index = index_syn_class[cur_neuron]
            memcpy(<void*>_neurons[cur_neuron].position, <void*>(pos.data()+cur_neuron*3), 3*sizeof(double))
            memcpy(<void*>_neurons[cur_neuron].rotation, <void*>(rot.data()+cur_neuron*4), 4*sizeof(double))

            # names to multimap
            neuron_data.nameMap[neuron_data.neuronNames[cur_neuron]].append(int(_neurons[cur_neuron].id))

        #Attach _neurons to object so that we can keep a reference to be later freed
        neuron_data._neurons = _neurons
        neuron_data.neurons.set_ptr(_neurons, subset_count)
        del f
        return neuron_data.neurons


    def load_morphologies(self, NeuronData neuron_data, morphos):
        # import progressbar
        # bar = progressbar.ProgressBar()
        for morpho in morphos:
            self.load_morphology(neuron_data, morpho)

    def load_morphology(self, NeuronData neuron_data, string morpho_name):
        assert morphotool, "Morphotool isnt available."
        morph = neuron_data.morphologies[morpho_name] = morphotool.MorphoReader(os.path.join(self.morphology_dir, morpho_name + ".h5")).create_morpho_tree()
        return morph

    def get_params(self):
        return {
            "mvd_filename" : self.mvd_filename,
            "morphology_dir": self.morphology_dir
        }
