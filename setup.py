#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
import glob
import os
import os.path as osp
import platform
import re
import subprocess
import sys

from distutils.version import LooseVersion
from pathlib import Path
from setuptools import setup, Command, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.test import test as TestCommand

BASE_DIR = osp.dirname(__file__)
EXAMPLES_DESTINATION = "share/spykfunc/examples"
_TERMINAL_CTRL = "\033[{}m"


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


def find_cmake():
    for candidate in ['cmake', 'cmake3']:
        try:
            out = subprocess.check_output([candidate, '--version'])
            cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)',
                                                   out.decode()).group(1))
            if cmake_version >= '3.2.0':
                return candidate
        except OSError:
            pass

    raise RuntimeError("CMake >= 3.2.0 must be installed to build Spykfunc")


class CMakeBuild(build_ext):
    def run(self):
        cmake = find_cmake()
        for ext in self.extensions:
            self.build_extension(ext, cmake)

    def build_extension(self, ext, cmake):
        source = Path(self.build_temp).resolve() / self.get_ext_filename(ext.name)
        extdir = str(source.parent)
        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if 'BOOST_ROOT' in os.environ:
            cmake_args += ['-DBOOST_ROOT={}'.format(os.environ['BOOST_ROOT'])]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(
                cfg.upper(),
                extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
            build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE={}'.format(cfg)]
            build_args += ['--', '-j']

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        try:
            proc = subprocess.Popen(
                "echo $CXX", shell=True, stdout=subprocess.PIPE)
            output = subprocess.check_call([cmake, ext.sourcedir] + cmake_args,
                                           cwd=self.build_temp)
            output = subprocess.check_call([cmake, '--build', '.'] + build_args,
                                           cwd=self.build_temp)
        except subprocess.CalledProcessError as exc:
            print("Status : FAIL", exc.returncode, exc.output)
            raise

        # Put into the local source directory
        target = Path(self.get_ext_fullpath(ext.name)).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        self.copy_file(source, target)

        # Add it to the library to package
        libs = Path(self.build_lib).resolve() / self.get_ext_filename(ext.name)
        libs.parent.mkdir(parents=True, exist_ok=True)
        self.copy_file(source, libs)


# *******************************
# Customize commands
# *******************************
class Documentation(Command):
    description = "Generate & optionally upload documentation to docs server"
    user_options = [("upload", None, "Upload to BBP internal docs server")]
    finalize_options = lambda self: None

    def initialize_options(self):
        self.upload = False

    def run(self):
        self._create_metadata_file()
        self.reinitialize_command('build_ext', inplace=1)
        self.run_command('build_ext')
        self.run_command('build_sphinx')  # requires metadata file
        if self.upload:
            self._upload()

    def _create_metadata_file(self):
        import textwrap
        import time
        md = self.distribution.metadata
        with open("docs/metadata.md", "w") as mdf:
            mdf.write(textwrap.dedent(f"""\
                ---
                name: {md.name}
                version: {md.version}
                description: {md.description}
                homepage: {md.url}
                license: {md.license}
                maintainers: {md.author}
                repository: {md.project_urls.get("Source", '')}
                issuesurl: {md.project_urls.get("Tracker", '')}
                contributors: {md.maintainer}
                updated: {time.strftime("%d/%m/%Y")}
                ---
                """))

    def _upload(self):
        from docs_internal_upload import docs_internal_upload
        print("Uploading....")
        docs_internal_upload(
            "docs/_build/html",
            metadata_path="docs/metadata.md",
            duplicate_version_error=False
        )


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


# *******************************
# Main setup
# *******************************
def setup_package():
    maybe_sphinx = [
        'sphinx<3.0.0',
        'sphinx-bluebrain-theme',
        'docs-internal-upload'
    ] if 'build_docs' in sys.argv else []

    setup(
        # name and other metadata are in setup.cfg
        name="spykfunc",
        summary="A PySpark implementation of the BBP Functionalizer",
        use_scm_version=True,
        project_urls={
            "Tracker": "https://bbpteam.epfl.ch/project/issues/projects/FUNCZ/summary",
            "Source": "https://bbpcode.epfl.ch/code/#/admin/projects/building/Spykfunc",
        },
        packages=[
            'recipe',
            'recipe.parts',
            'sparkmanager',
            'spykfunc',
            'spykfunc.dataio',
            'spykfunc.filters',
            'spykfunc.filters.implementations',
            'spykfunc.filters.udfs',
            'spykfunc.tools',
            'spykfunc.utils',
        ],
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
            'hdfs',
            'jprops',
            'lazy-property',
            'lxml',
            'morphokit',
            'mvdtool>=2',
            'numpy',
            'pandas',
            'pathlib2;python_version<"3.4"',
            'progress',
            'pyarrow',
            'pyspark>=3',
            'six',
        ],
        setup_requires=['setuptools_scm'] + maybe_sphinx,
        tests_require=['mock', 'pytest', 'pytest-cov'],
        ext_modules=[CMakeExtension('spykfunc.filters.udfs._udfs', 'spykfunc/filters/udfs')],
        cmdclass={
            'build_docs': Documentation,
            'build_ext': CMakeBuild,
            'test': PyTest,
        },
        entry_points={
            'console_scripts': [
                'spykfunc = spykfunc.commands:spykfunc',
                'parquet-compare = spykfunc.tools.compare:run',
                'parquet-coalesce = spykfunc.tools.coalesce:run',
            ],
        },
        scripts=[
            'scripts/sm_cluster',
            'scripts/sm_run',
            'scripts/sm_startup',
            'scripts/sm_shutdown',
        ],
        dependency_links=[
            "https://bbpteam.epfl.ch/repository/devpi/simple/docs_internal_upload"
        ],
    )


if __name__ == "__main__":
    setup_package()
