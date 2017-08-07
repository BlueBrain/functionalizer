from libcpp.string cimport string

# ---------------------------------------------------
# To work paralell we define Part and Range object
# ---------------------------------------------------
cdef class _Part_t:
    pass

cdef class Part(_Part_t):
    cdef readonly int _part_nr
    cdef readonly int _total_parts

cdef class Range(_Part_t):
    cdef readonly int offset
    cdef readonly int count

# -------------------------------------------------------------------
# Interface for Data and Loaders
# -------------------------------------------------------------------
cdef class NeuronDataI:
    cdef readonly NeuronLoaderI _loader
    cdef readonly size_t nNeurons

cdef class NeuronLoaderI:
    pass


