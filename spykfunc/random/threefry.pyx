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

    cpdef double gamma(self, double m, double sd):
        cdef double shape = m * m / (sd * sd)
        cdef double scale = sd * sd / m
        cdef b_gamma* dist = new b_gamma(shape, scale)
        res = deref(dist)(deref(self.engine))
        del dist
        return res

    cpdef double truncated_normal(self, double m, double sd):
        cdef b_normal* dist = new b_normal()
        cdef double res = deref(dist)(deref(self.engine))
        while res > 1.0 or res < -1.0:
            res = deref(dist)(deref(self.engine))
        del dist
        return sd * res + m

    cpdef double poisson(self, int m):
        cdef b_poisson* dist = new b_poisson(m)
        res = deref(dist)(deref(self.engine))
        del dist
        return res

cpdef np.ndarray[float] gamma(RNGThreefry rng,
                              np.ndarray[int] src, np.ndarray[int] dst,
                              np.ndarray[float] m, np.ndarray[float] sd):
    cdef RNGThreefry r = rng
    cdef int i, last = -1, n = len(src)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    assert len(dst) == len(m) == len(sd) == n
    for i in range(n):
        if last != src[i]:
            last = src[i]
            r = rng.derivate(last)
        res[i] = r.derivate(dst[i]).gamma(m[i], sd[i])
    return res

cpdef np.ndarray[float] truncated_normal(RNGThreefry rng,
                                         np.ndarray[int] src, np.ndarray[int] dst,
                                         np.ndarray[float] m, np.ndarray[float] sd):
    cdef RNGThreefry r = rng
    cdef int i, last = -1, n = len(src)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    assert len(dst) == len(m) == len(sd) == n
    for i in range(n):
        if last != src[i]:
            last = src[i]
            r = rng.derivate(last)
        res[i] = r.derivate(dst[i]).truncated_normal(m[i], sd[i])
    return res

cpdef np.ndarray[float] poisson(RNGThreefry rng,
                                 np.ndarray[int] src, np.ndarray[int] dst,
                                 np.ndarray[short] k):
    cdef RNGThreefry r = rng
    cdef int i, last = -1, n = len(src)
    cdef np.ndarray[float] res = np.empty(n, dtype=np.float32)
    assert len(dst) == len(k) == n
    for i in range(n):
        if last != src[i]:
            last = src[i]
            r = rng.derivate(last)
        if k[i] > 1:
            res[i] = r.derivate(dst[i]).poisson(k[i] - 1)
        else:
            res[i] = 1
    return res
