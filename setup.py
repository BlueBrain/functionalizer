#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
from setuptools import setup, Extension
from setuptools.command.test import test as TestCommand
import sys
import os
import os.path as osp
import glob

SPYKFUNC_VERSION = "0.5.0"
BUILD_TYPE = os.getenv('BUILD_TYPE', "RELEASE").upper()
BASE_DIR = osp.dirname(__file__)

assert BUILD_TYPE in ["RELEASE", "DEVEL"], "Build types allowed: DEVEL, RELEASE"

if BUILD_TYPE == "RELEASE":
    assert glob.glob(osp.join(BASE_DIR, 'spykfunc/dataio/*.cpp'))
elif BUILD_TYPE == "DEVEL":
    from Cython.Build import cythonize


# *******************************
# Handle test
# *******************************
class PyTest(TestCommand):
    user_options = [('addopts=', None, 'Arguments to pass to pytest')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.addopts = ""

    def run_tests(self):
        import shlex
        import pytest
        errno = pytest.main(shlex.split(self.addopts))
        sys.exit(errno)


# *******************************
# Extensions setup
# *******************************
_ext_dir = osp.join(BASE_DIR, 'spykfunc/dataio/')
_ext_mod = 'spykfunc.dataio.'
_filename_ext = '.pyx' if BUILD_TYPE == 'DEVEL' else '.cpp'

ext_mods = {
    'common': {},
    'structbuf': {},
    'cppneuron': dict(
        include_dirs=[osp.join(BASE_DIR, '../deps/hadoken/include'),
                      osp.join(BASE_DIR, '../deps/mvd-tool/include'),
                      osp.join(BASE_DIR, '../deps/mvd-tool/deps/highfive/include')],
        libraries=['hdf5']
    ),
}

# Quick attempt find INCLUDE_DIRS required by cppneuron
_libs_env = ['HDF5_ROOT', 'BOOST_ROOT']
for lib in _libs_env:
    lib_ROOT = os.getenv(lib)
    if lib_ROOT is not None and lib_ROOT != '/usr':
        ext_mods['cppneuron']['include_dirs'].append(os.path.join(lib_ROOT, "include"))

extensions = [
    Extension(_ext_mod + name, [_ext_dir + name + _filename_ext],
              language='c++',
              **opts)
    for name, opts in ext_mods.items()
]

if BUILD_TYPE == 'DEVEL':
    extensions.append(
        Extension('tests.tst_neuron_memview',
                  ['tests/tst_neuron_memview' + _filename_ext],
                  language="c++"))
    extensions = cythonize(extensions,
                           cplus=True,
                           build_dir="build",
                           include_path=[osp.join(BASE_DIR, 'spykfunc/dataio/mvdtool')])


# *******************************
# Main setup
# *******************************
def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []

    setup(
        # name and other metadata are in setup.cfg
        version=SPYKFUNC_VERSION,
        packages=['spykfunc'],
        ext_modules=extensions,
        install_requires=[
            'pyspark',
            'future',
            'docopt',
            'enum34;python_version<"3.4"',
            'numpy',
            'lazy-property',
            'h5py',
            'lxml',
            'progress'
        ],
        # Setup and testing
        setup_requires=['setuptools_scm'] + sphinx,
        tests_require=['pytest', 'pytest-cov'],
        extras_require={
            'dev': ['cython<0.26', 'flake8']
        },
        cmdclass={'test': PyTest},
        entry_points={
            'console_scripts': [
                'spykfunc = spykfunc.commands:spykfunc'],
        },
        package_data={'spykfunc': ['data/*']},
    )


if __name__ == '__main__':
    setup_package()
