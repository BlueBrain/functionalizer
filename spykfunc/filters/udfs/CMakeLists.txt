cmake_minimum_required(VERSION 3.2)

project(Spykfunc VERSION 0.0.0 DESCRIPTION "Helper modules for performant touch filtering")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_VERBOSE_MAKEFILE ON)

find_package(Boost 1.60 REQUIRED COMPONENTS random)
find_package(MorphoKit REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

add_subdirectory(deps/pybind11)

pybind11_add_module(_udfs SYSTEM _udfs.cpp)
target_include_directories(_udfs SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/deps/hadoken/include)
target_link_libraries(_udfs PUBLIC Boost::random)
