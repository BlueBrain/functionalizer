cmake_minimum_required(VERSION 3.2)

project(Spykfunc VERSION 0.0.0 DESCRIPTION "Helper modules for performant touch filtering")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_VERBOSE_MAKEFILE ON)

find_package(MorphoKit REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

find_path(RANDOM123_INCLUDE_PATH NAMES Random123/threefry.h)
add_library(Random123 INTERFACE IMPORTED)
set_target_properties(Random123 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${RANDOM123_INCLUDE_PATH}
)

add_subdirectory(deps/pybind11)

pybind11_add_module(_udfs SYSTEM _udfs.cpp)
target_link_libraries(_udfs PRIVATE Random123)
