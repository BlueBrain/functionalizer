# distutils: language = c++

from cython.operator cimport dereference as deref

cimport numpy as np
import numpy as np
from libc.stdint cimport uint64_t
from .threefry cimport threefry4x64, \
                       counter_engine, \
                       random_engine_mapper_64, \
                       b_gamma, \
                       b_normal, \
                       b_poisson, \
                       b_uniform

ctypedef counter_engine[threefry4x64] rng3fry

cdef class RNGThreefry:
    cdef random_engine_mapper_64* engine

    def __cinit__(self):
        cdef rng3fry rng = rng3fry()
        self.engine = new random_engine_mapper_64(rng)

    def __call__(self):
        return deref(self.engine)()

    def __dealloc__(self):
        del self.engine

    cdef void reset(self, random_engine_mapper_64* eng, uint64_t key):
        del self.engine
        self.engine = new random_engine_mapper_64(eng.derivate(key))

    def derivate(self, uint64_t key):
        cdef RNGThreefry child = RNGThreefry()
        child.reset(self.engine, key)
        return child

    def seed(self, uint64_t seed):
        self.engine.seed(seed)
        return self

    cpdef double gamma(self, double m, double sd):
        cdef double shape = m * m / (sd * sd)
        cdef double scale = sd * sd / m
        cdef b_gamma* dist = new b_gamma(shape, scale)
        cdef double res = deref(dist)(deref(self.engine))
        del dist
        return res

    cpdef double truncated_normal(self, double m, double sd):
        cdef b_normal* dist = new b_normal()
        cdef double res = deref(dist)(deref(self.engine))
        while res > 1.0 or res < -1.0:
            res = deref(dist)(deref(self.engine))
        del dist
        return sd * res + m

    cpdef int poisson(self, int m):
        cdef b_poisson* dist = new b_poisson(m)
        cdef int res = deref(dist)(deref(self.engine))
        del dist
        return res

    cpdef double uniform(self):
        cdef b_uniform* dist = new b_uniform(0.0, 1.0)
        cdef double res = deref(dist)(deref(self.engine))
        del dist
        return res

cpdef np.ndarray[float] gamma(RNGThreefry rng,
                              np.ndarray[long] key,
                              np.ndarray[float] m, np.ndarray[float] sd):
    cdef int i, n = len(key)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    assert len(m) == len(sd) == n
    for i in range(n):
        res[i] = rng.derivate(key[i]).gamma(m[i], sd[i])
    return res

cpdef np.ndarray[float] truncated_normal(RNGThreefry rng,
                                         np.ndarray[long] key,
                                         np.ndarray[float] m, np.ndarray[float] sd):
    cdef int i, n = len(key)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    cdef RNGThreefry derivative
    assert len(m) == len(sd) == n
    for i in range(n):
        derivative = rng.derivate(key[i])
        res[i] = derivative.truncated_normal(m[i], sd[i])
        while res[i] < 0:
            res[i] = derivative.truncated_normal(m[i], sd[i])
    return res

cpdef np.ndarray[float] uniform(RNGThreefry rng,
                                np.ndarray[long] key):
    cdef int i, n = len(key)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    for i in range(n):
        res[i] = rng.derivate(key[i]).uniform()
    return res

cpdef np.ndarray[int] poisson(RNGThreefry rng,
                              np.ndarray[long] key,
                              np.ndarray[short] k):
    cdef int i, n = len(key)
    cdef np.ndarray[int] res = np.empty(n, dtype=np.int32)
    assert len(k) == n
    for i in range(n):
        if k[i] >= 1:
            res[i] = 1 + rng.derivate(key[i]).poisson(k[i] - 1)
        else:
            res[i] = 1
    return res
