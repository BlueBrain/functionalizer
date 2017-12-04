#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand
import sys
import os
import glob

SPYKFUNC_VERSION = "0.4.dev1"

force_rebuild_cython = os.getenv('FORCE_CYTHONIZE', False)
if not force_rebuild_cython and glob.glob('spykfunc/dataio/*.cpp'):
    build_mode = 'release'
else:
    build_mode = 'devel'
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
_ext_dir = 'spykfunc/dataio/'
_ext_mod = 'spykfunc.dataio.'
_filename_ext = '.pyx' if build_mode == 'devel' else '.cpp'

ext_mods = {
    'common': {},
    'structbuf': {},
    'cppneuron': dict(
        include_dirs=['../deps/hadoken/include',
                      '../deps/mvd-tool/include',
                      '../deps/mvd-tool/deps/highfive/include'],
        libraries=['hdf5']
    ),
}

# Handle simple include cases
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
extensions.append(
    Extension('tst_neuron_memview',
              ['tests/tst_neuron_memview' + _filename_ext],
              language="c++"))

if build_mode == 'devel':
    extensions = cythonize(extensions,
                           cplus=True,
                           build_dir = "build",
                           include_path=['spykfunc/dataio/mvdtool'])


# *******************************
# Main setup
# *******************************
def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []

    setup(
        # name and other metadata are in setup.cfg
        version=SPYKFUNC_VERSION,
        # use_scm_version=True,
        packages=find_packages(),
        ext_modules=extensions,
        install_requires=[
            'future',
            'docopt',
            'enum34;python_version<"3.4"',
            'numpy',
            'lazy-property',
            'py4j',
            'h5py',
            'lxml',
            'progress'
            #'morphotool'  # Do we really need it?
        ],
        # Setup and testing
        setup_requires=['setuptools_scm'] + sphinx,
        tests_require=['pytest', 'pytest-cov'],
        extras_require={
            'dev': ['cython<0.26', 'flake8']
        },
        cmdclass={'test': PyTest},

        scripts=['bin/spykfunc',
                 'spykfunc/commands.py'],

        data_files=[('share/spykfunc', ['java/spykfunc_udfs.jar'])],
        include_package_data=True,
    )


if __name__ == '__main__':
    setup_package()
