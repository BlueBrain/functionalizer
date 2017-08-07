#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
import sys
from Cython.Build import cythonize
from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    user_options = [('addopts=', None, "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.addopts = ""

    def run_tests(self):
        import shlex
        import pytest
        errno = pytest.main(shlex.split(self.addopts))
        sys.exit(errno)


extensions = [
    Extension('*', ['spykfunc/dataio/*.pyx'],
              include_dirs=["../deps/hadoken/include", "../deps/mvd-tool/include", "../deps/mvd-tool/deps/highfive/include"],
              libraries=['hdf5'],
              language="c++"
    ),
    Extension('tst_neuron_memview', ['tests/tst_neuron_memview.pyx'],
              language="c++")
]


def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    setup(
        # name and other metadata are in setup.cfg
        version="0.1.dev0",
        # use_scm_version=True,
        packages=find_packages(),
        ext_modules=cythonize(extensions,
                              cplus=True,
                              inplace=True,
                              include_path=['spykfunc/dataio/mvdtool'],
                              ),

        install_requires=[
            'future',
            'enum34;python_version<"3.4"'
        ],
        # Setup and testing
        setup_requires=['setuptools_scm'] + sphinx,
        tests_require=['pytest', 'pytest-cov'],
        cmdclass={'test': PyTest},
    )


if __name__ == "__main__":
    setup_package()
