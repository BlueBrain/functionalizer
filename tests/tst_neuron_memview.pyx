from spykfunc.dataio.structbuf cimport StructType, StructBuffer
from libc.stdlib cimport malloc, free
from libc.stdio cimport printf
from spykfunc.dataio.structbuf import TYPES


ctypedef unsigned int uint

cdef struct NeuronInfo:
    uint id
    char[2] t
    uint morphology
    uint electrophysiology
    uint syn_class_index
    double position[3]
    double rotation[3]


#Neuron map not copying data
cdef class NeuronBuffer(StructBuffer):
    block_t = StructType( (
        ("id", TYPES.UNSIGNED_INT),
        ("t", TYPES.String(2)),
        ("morphology", TYPES.UNSIGNED_INT),
        ("electrophysiology", TYPES.UNSIGNED_INT),
        ("syn_class_index", TYPES.UNSIGNED_INT),
        ("position", TYPES.Array(TYPES.DOUBLE,3)),
        ("rotation", TYPES.Array(TYPES.DOUBLE, 3))
    ))

    def __init__(self):
        cdef Py_ssize_t s_size=sizeof(NeuronInfo)
        super(self.__class__, self).__init__(self.block_t)


# Data info and loader methods
cdef class NeuronData:

    cdef public NeuronBuffer neuronbuf

    #Raw neuron info
    cdef NeuronInfo * _neurons

    def __cinit__(self):
        self.neuronbuf = NeuronBuffer()
        self._neurons = <NeuronInfo *>malloc( 2*sizeof(StructBuffer) )
        cdef NeuronInfo *neurons = self._neurons


        printf("Neurons at %p\n", neurons)
        neurons[0].id = 1
        neurons[0].t = <char*>"aa"
        neurons[0].morphology = 3
        neurons[0].electrophysiology = 4
        neurons[0].syn_class_index = 5
        neurons[0].position = [1,1,1]
        neurons[0].rotation = [2,2,2]

        neurons[1].id = 11
        neurons[1].t = <char*>"bb"
        neurons[1].morphology = 31
        neurons[1].electrophysiology = 41
        neurons[1].syn_class_index = 51
        neurons[1].position = [11,11,11]
        neurons[1].rotation = [21,21,21]

        self.neuronbuf.set_ptr( neurons, 2)

    def change_item_id(self, int i, int newid):
        self._neurons[i].id = newid

    def __dealloc__(self):
        free(<void*>self._neurons)
        print "Deallocated neurons"
