from libcpp.string cimport string
from libc.stdio cimport sprintf
from cpython.mem cimport PyMem_Malloc, PyMem_Free
import numpy as np

cdef class Pointer:
    def __repr__(self):
        cdef char r[20]
        sprintf(r, "<Pointer %p>", self.ptr)
        return r


cdef class Struct:
    def __init__(self, StructType t, Pointer p=None):
        self.struct_t = t
        if p:
            self.mem = p.ptr
        else:
            self.mem = PyMem_Malloc(t.size_bytes)
        self.size = t.size


cdef class _TYPE(object):
    def __init__(self, str name, str dtype_repr, str pystruct_repr):
        self.name = name
        self._dtype_repr = dtype_repr
        self._pystruct_repr = pystruct_repr

    def __repr__(self):
        return "<type:%s>" % (self.name,)

    @property
    def dtype(self):
        return self._dtype_repr

    @property
    def pystruct_t(self):
        return self._pystruct_repr


class TYPES(object):
    BOOL = _TYPE("bool", "b", "?")
    INT  = _TYPE("int", "i4", "i")
    UNSIGNED_INT = _TYPE("unsigned int", "u4", "I")
    LONGLONG = _TYPE("long int", "i8", "q")
    UNSIGNED_LONGLONG = _TYPE("unsigned long", "u8", "Q")
    FLOAT = _TYPE("float", "f4", "f")
    DOUBLE = _TYPE("double", "f8", "d")
    CHAR   = _TYPE("char", "S1", "c")

    class Array(_TYPE):
        def __init__(self, basetype, length):
            self._basetype = basetype
            self.length = length
        def __repr__(self):
            return "<ArrayType: %s[%d]>" % (self._basetype, self.length)
        @property
        def dtype(self):
            return "%d%s" % (self.length, self._basetype.dtype)
        @property
        def pystruct_t(self):
            return "%d%s" % (self.length, self._basetype.pystruct_t)

    class String(_TYPE):
        def __init__(self, length):
            self.length = length
        def __repr__(self):
            return "<StringType: char[%d]>" % (self.length,)
        @property
        def dtype(self):
            return "S%d" % (self.length,)
        @property
        def pystruct_t(self):
            return "%ds" % (self.length,)


cdef class StructType(_TYPE):

    def __init__(self, fields):
        dtype_spec = []  #dtype spec with numpy syntax
        pystruct_spec = []
        self._names_to_i = {}

        for i, (fname, ftype) in enumerate(fields):
            dtype_spec.append((fname, ftype.dtype))
            pystruct_spec.append( ftype.pystruct_t )

        #Create dtype
        self._dtype = np.dtype(dtype_spec, align=True)

        self._dtype_spec = ",".join([a[1] for a in dtype_spec])
        self._pystruct_spec = "".join(pystruct_spec)

    @property
    def dtype(self):
        return self._dtype_spec

    @property
    def pystruct_t(self):
        return self._pystruct_spec

    @property
    def size_bytes(self):
        return self._dtype.itemsize


cdef class StructBuffer:
    # NOTE: member declaration in .pxd

    def __init__(self, StructType struct_type=None):
        if struct_type:
            self.struct_t = struct_type
            self.init(struct_type.pystruct_t.encode("utf8"), struct_type.size_bytes)
        else:
            self.struct_t = None

    cdef void init(self, string block_format, int block_size):
        # print "Backend initting"
        self.block_format = block_format
        self._sformat = self.block_format.c_str()
        self.block_size = block_size

    cdef void set_ptr( self, void* ptr, Py_ssize_t block_count):
        self.buf_ptr = ptr
        self.block_count = block_count
        self.size_bytes = block_count * self.block_size

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        cdef const char* f = self.block_format.c_str()
        # printf("Buf %p type: %s\n", self.buf_ptr, f)
        # printf("block_size: %d\n", self.block_size)
        # printf("Blocks: %d \n", self.block_count)
        # printf("bytes: %d \n", self.size_bytes)
        buffer.buf = self.buf_ptr
        buffer.format = <char*>self._sformat
        buffer.itemsize = self.block_size
        buffer.len = self.size_bytes
        buffer.obj = self
        buffer.ndim = 1
        buffer.readonly = 1
        buffer.shape = NULL
        buffer.strides = NULL
        buffer.suboffsets = NULL
        buffer.internal = NULL

    def asarray(self):
        return np.asarray(self)

    def __getitem__(self, i):
        return self.asarray()[i]

    def __len__(self):
        return self.block_count