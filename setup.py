#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup file for spykfunc.
"""
import os
import platform
import re
import subprocess
import sys

from pathlib import Path
from setuptools import setup, Command, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.test import test as TestCommand


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        source = Path(self.build_temp).resolve() / self.get_ext_filename(ext.name)
        extdir = str(source.parent)
        cmake_args = [
            "-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=" + extdir,
            "-DPYTHON_EXECUTABLE=" + sys.executable,
        ]

        cfg = "Debug" if self.debug else "Release"
        build_args = ["--config", cfg]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        output = subprocess.check_call(["cmake", ext.sourcedir] + cmake_args, cwd=self.build_temp)
        output = subprocess.check_call(["cmake", "--build", "."] + build_args, cwd=self.build_temp)

        # Put into the local source directory
        target = Path(self.get_ext_fullpath(ext.name)).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        self.copy_file(source, target)

        # Add it to the library to package
        libs = Path(self.build_lib).resolve() / self.get_ext_filename(ext.name)
        libs.parent.mkdir(parents=True, exist_ok=True)
        self.copy_file(source, libs)


setup(
    # name and other metadata are in setup.cfg
    use_scm_version=True,
    ext_modules=[CMakeExtension("spykfunc.filters.udfs._udfs", "src/spykfunc/filters/udfs")],
    cmdclass={"build_ext": CMakeBuild},
)
