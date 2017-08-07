from libcpp.string cimport string

cdef class Pointer:
    cdef void* ptr

cdef class Struct:
    cdef StructType struct_t
    cdef void* mem
    cdef int size

cdef class _TYPE(object):
    cdef string name
    cdef string  _dtype_repr
    cdef string _pystruct_repr

cdef class StructType(_TYPE):
    cdef string _dtype_spec
    cdef string _pystruct_spec
    cdef dict _names_to_i
    cdef object _dtype

cdef class StructBuffer:
    cdef StructType struct_t
    cdef void *buf_ptr
    cdef string block_format
    cdef const char* _sformat
    cdef Py_ssize_t block_count
    cdef Py_ssize_t block_size
    cdef Py_ssize_t size_bytes
    cdef void init(self, string struct_format, int struct_size)
    cdef void set_ptr(self, void* ptr, Py_ssize_t buffer_count)


