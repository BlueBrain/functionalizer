# distutils: language = c++

from libc.stdint cimport uint64_t

cdef extern from "hadoken/random/random.hpp" namespace "hadoken":
    cdef cppclass threefry4x64 "hadoken::threefry<4, std::uint64_t>"

    cdef cppclass counter_engine[T]:
        ctypedef uint64_t result_type
        counter_engine() except +
        void seed(result_type)
        counter_engine[T] derivate(result_type)
        result_type operator()()

    cdef cppclass random_engine_mapper_64:
        ctypedef uint64_t result_type
        random_engine_mapper_64(random_engine_mapper_64&) except +
        random_engine_mapper_64(counter_engine[threefry4x64]&) except +
        void seed(result_type)
        random_engine_mapper_64 derivate(result_type)
        result_type operator()()

cdef extern from "boost/random.hpp" namespace "boost::random":
    cdef cppclass normal_distribution[T]:
        normal_distribution() except +
        T operator()(random_engine_mapper_64&)

    cdef cppclass gamma_distribution[T]:
        gamma_distribution(T, T) except +
        T operator()(random_engine_mapper_64&)

    cdef cppclass poisson_distribution[U, T]:
        poisson_distribution(T) except +
        T operator()(random_engine_mapper_64&)

    cdef cppclass uniform_real_distribution[T]:
        uniform_real_distribution(T, T) except +
        T operator()(random_engine_mapper_64&)

ctypedef normal_distribution[double] b_normal
ctypedef gamma_distribution[double] b_gamma
ctypedef poisson_distribution[int, double] b_poisson
ctypedef uniform_real_distribution[double] b_uniform
