#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
from setuptools import setup, Extension
from setuptools.command.test import test as TestCommand
from setuptools.command.install import install as InstallCommand
import sys
import os
import os.path as osp
import glob
import shutil

import numpy as np

BUILD_TYPE = os.getenv('BUILD_TYPE', "RELEASE").upper()
BASE_DIR = osp.dirname(__file__)
EXAMPLES_DESTINATION = "share/spykfunc/examples"
_TERMINAL_CTRL = "\033[{}m"

assert BUILD_TYPE in ["RELEASE", "DEVEL"], "Build types allowed: DEVEL, RELEASE"

if BUILD_TYPE == "RELEASE":
    assert glob.glob(osp.join(BASE_DIR, 'spykfunc/*/*.cpp'))
elif BUILD_TYPE == "DEVEL":
    from Cython.Build import cythonize


# *******************************
# Customize commands
# *******************************
class PyTest(TestCommand):
    user_options = [
        ('addopts=', None, 'Arguments to pass to pytest'),
        ('fast', None, 'Skip slow tests')
    ]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.addopts = None
        self.fast = False
        self.test_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        if not self.fast:
            self.test_args.append('--run-slow')
        if self.addopts:
            import shlex
            self.test_args.extend(shlex.split(self.addopts))

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


class Install(InstallCommand):
    """Post-installation for installation mode."""
    def run(self):
        InstallCommand.run(self)
        print("{}Going to install examples to INSTALL_PREFIX/{}{}"
              .format(_TERMINAL_CTRL.format(32),    # Green
                      EXAMPLES_DESTINATION,
                      _TERMINAL_CTRL.format(0)))    # reset


# *******************************
# Extensions setup
# *******************************
_filename_ext = '.pyx' if BUILD_TYPE == 'DEVEL' else '.cpp'

_ext_dir = osp.join(BASE_DIR, 'spykfunc/')

ext_mods = {
    'spykfunc.dataio.common': {},
    'spykfunc.dataio.structbuf': {},
    'spykfunc.dataio.cppneuron': dict(
        include_dirs=[osp.join(BASE_DIR, 'deps/hadoken/include'),
                      osp.join(BASE_DIR, 'deps/mvd-tool/include')],
        library_dirs=[],
        libraries=['hdf5']
    ),
    'spykfunc.filters.udfs.matching': dict(
        include_dirs=[np.get_include()],
    ),
    'spykfunc.random.threefry': dict(
        include_dirs=[osp.join(BASE_DIR, 'deps/hadoken/include'),
                      np.get_include()],
    ),
}

# Quick attempt find INCLUDE_DIRS required by cppneuron
_libs_env = ['HDF5_ROOT', 'BOOST_ROOT', 'HIGHFIVE_ROOT']
_cppneuron = 'spykfunc.dataio.cppneuron'
for lib in _libs_env:
    lib_ROOT = os.getenv(lib)
    if lib_ROOT is not None and lib_ROOT != '/usr':
        ext_mods[_cppneuron]['include_dirs'].append(os.path.join(lib_ROOT, "include"))
        for _libdir in ('lib64', 'lib'):
            full_libdir = osp.join(lib_ROOT, _libdir)
            if(os.path.isdir(full_libdir)):
                ext_mods[_cppneuron]['library_dirs'].append(full_libdir)

extensions = [
    Extension(name, [name.replace('.', '/') + _filename_ext],
              language='c++',
              extra_compile_args=['-std=c++11'],
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
                           include_path=[osp.join(BASE_DIR, 'spykfunc/dataio/mvdtool'),
                                         osp.join(BASE_DIR, 'deps/mvd-tool/python/include')])
    for name in ext_mods:
        path = name.replace('.', '/') + '.cpp'
        src = osp.join("build", path)
        dst = osp.join(BASE_DIR, path)
        print("{}Updating Cython-generated extension '{}'{}"
              .format(_TERMINAL_CTRL.format(32),    # Green
                      path,
                      _TERMINAL_CTRL.format(0)))    # reset
        shutil.copy(src, dst)


# *******************************
# Main setup
# *******************************
def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    maybe_sphinx = ['sphinx', 'sphinx_rtd_theme'] if needs_sphinx else []
    maybe_cython = ["cython<0.26"] if BUILD_TYPE == "DEVEL" else []

    setup(
        # name and other metadata are in setup.cfg
        name="spykfunc",
        summary="A PySpark implementation of the BBP Functionalizer",
        use_scm_version=True,
        packages=[
            'spykfunc',
            'spykfunc.dataio',
            'spykfunc.filters',
            'spykfunc.filters.udfs',
            'spykfunc.random',
            'spykfunc.tools',
            'spykfunc.tools.analysis',
            'spykfunc.utils',
        ],
        ext_modules=extensions,
        package_data={
            'spykfunc': ['data/*']
        },
        data_files=[
            (EXAMPLES_DESTINATION, glob.glob(osp.join(BASE_DIR, "examples", "*.py")) +
             glob.glob(osp.join(BASE_DIR, "examples", "*.sh")))
        ],
        #  ----- Requirements -----
        install_requires=[
            'docopt',
            'enum34;python_version<"3.4"',
            'funcsigs',
            'future',
            'h5py',
            'hdfs',
            'jprops',
            'lazy-property',
            'lxml',
            'numpy',
            'pandas',
            'pathlib2;python_version<"3.4"',
            'progress',
            'pyarrow',
            'sparkmanager>=0.7.0',
        ],
        setup_requires=['setuptools_scm'] + maybe_sphinx + maybe_cython,
        tests_require=['mock', 'pytest', 'pytest-cov'],
        extras_require={
            # Dependencies if the user wants a dev env
            'dev': ['cython<0.26', 'flake8'],
            'plot': ['seaborn', 'requests', 'bb5']
        },
        cmdclass={'test': PyTest,
                  'install': Install},
        entry_points={
            'console_scripts': [
                'spykfunc = spykfunc.commands:spykfunc',
                'spykfunc_plot = spykfunc.tools.scaling:run [plot]',
                'parquet-compare = spykfunc.tools.parquet_compare:run',
            ],
        },
    )


if __name__ == '__main__':
    setup_package()
