cmake_minimum_required(VERSION 3.15...3.27)

project("${SKBUILD_PROJECT_NAME}"
    VERSION 0.0.0
    DESCRIPTION "Helper modules for performant touch filtering"
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(RANDOM123_INCLUDE_PATH ${CMAKE_SOURCE_DIR}/../../../../deps/random123/include)
add_library(Random123 INTERFACE IMPORTED)
set_target_properties(Random123 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${RANDOM123_INCLUDE_PATH}
)

add_subdirectory(${CMAKE_SOURCE_DIR}/../../../../deps/pybind11 deps/pybind11)

pybind11_add_module(_udfs SYSTEM _udfs.cpp)
target_link_libraries(_udfs PRIVATE Random123)
install(TARGETS _udfs LIBRARY DESTINATION functionalizer/filters/udfs)
