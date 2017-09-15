#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand
import sys
import glob
#try:
    # Attempt import Cython
from Cython.Build import cythonize
build_mode = 'devel'
# except ImportError:
#     build_mode = 'release'
#     assert glob.glob('spykfunc/dataio/*.cpp'), \
#         'Cpp extension sources not found. Please install Cython.'


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
                           include_path=['spykfunc/dataio/mvdtool'])


# *******************************
# Main setup
# *******************************
def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []

    setup(
        # name and other metadata are in setup.cfg
        version="0.1.dev0",
        # use_scm_version=True,
        packages=find_packages(),
        ext_modules=extensions,
        install_requires=[
            'future',
            'enum34;python_version<"3.4"',
            'numpy',
            'lazy-property',
            'py4j',
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

        data_files=[('share/spykfunc', ['java/random.jar'])],
        include_package_data=True,
    )


if __name__ == '__main__':
    setup_package()
