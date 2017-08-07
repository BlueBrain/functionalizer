from libcpp.string cimport string

# ---------------------------------------------------
# To work paralell we define Part and Range object
# ---------------------------------------------------
cdef class _Part_t:
    def get_offset_count(self, int total=0): pass

cdef class Part(_Part_t):
    # cdef readonly int _part_nr
    # cdef readonly int _total_parts

    def __init__(self, part_nr=0, total_parts=0):
        self._part_nr = part_nr
        self._total_parts = total_parts

    def get_offset_count(self, int total):
        offset = total * self._part_nr / self._total_parts
        count = total / self._total_parts
        if self._part_nr >= self._total_parts - 1:
            count = total - (self._total_parts - 1) * count
        return (offset, count)

cdef class Range(_Part_t):
    # cdef readonly int offset
    # cdef readonly int count

    def __init__(self, offset=0, count=0):
        self.offset = offset
        self.count = count

    def get_offset_count(self, int total=0):
        return (self.offset, self.count)


# -------------------------------------------------------------------
# Interface for Data and Loaders
# -------------------------------------------------------------------
cdef class NeuronDataI:
    def set_loader(self, NeuronLoaderI loader): pass
    def load_globals(self): pass
    def load_neurons(self, _Part_t part=None): pass
    def load_morphology(self, string morphology): pass
    def set_name_map(self, dict name_map): pass


cdef class NeuronLoaderI:
    def load_globals(self, NeuronDataI neuron_data_dst): pass
    def load_neurons(self, NeuronDataI neuron_data, Part part): pass
    def load_morphologies(self, NeuronDataI neuron_data, Part part): pass
    # Params to construct another loader which is similar, for sub-processes
    def get_params(self): pass

