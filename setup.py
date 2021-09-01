#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup file for spykfunc.
"""
import os
import platform
import re
import subprocess
import sys

from distutils.version import LooseVersion
from pathlib import Path
from setuptools import setup, Command, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.test import test as TestCommand


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


def find_cmake():
    for candidate in ["cmake", "cmake3"]:
        try:
            out = subprocess.check_output([candidate, "--version"])
            cmake_version = LooseVersion(
                re.search(r"version\s*([\d.]+)", out.decode()).group(1)
            )
            if cmake_version >= "3.2.0":
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
        cmake_args = [
            "-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=" + extdir,
            "-DPYTHON_EXECUTABLE=" + sys.executable,
        ]

        cfg = "Debug" if self.debug else "Release"
        build_args = ["--config", cfg]

        if "BOOST_ROOT" in os.environ:
            cmake_args += ["-DBOOST_ROOT={}".format(os.environ["BOOST_ROOT"])]

        if platform.system() == "Windows":
            cmake_args += [
                "-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}".format(cfg.upper(), extdir)
            ]
            if sys.maxsize > 2 ** 32:
                cmake_args += ["-A", "x64"]
            build_args += ["--", "/m"]
        else:
            cmake_args += ["-DCMAKE_BUILD_TYPE={}".format(cfg)]
            build_args += ["--", "-j"]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        try:
            proc = subprocess.Popen("echo $CXX", shell=True, stdout=subprocess.PIPE)
            output = subprocess.check_call(
                [cmake, ext.sourcedir] + cmake_args, cwd=self.build_temp
            )
            output = subprocess.check_call(
                [cmake, "--build", "."] + build_args, cwd=self.build_temp
            )
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


setup(
    # name and other metadata are in setup.cfg
    use_scm_version={
        "write_to": "spykfunc/version.py"
    },
    ext_modules=[
        CMakeExtension("spykfunc.filters.udfs._udfs", "spykfunc/filters/udfs")
    ],
    cmdclass={"build_ext": CMakeBuild},
)
