#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for spykfunc.
"""
import sys
from setuptools import setup, find_packages
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


def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    setup(
        # name and other metadata are in setup.cfg
        version="0.1.dev0",
        # use_scm_version=True,
        packages=find_packages(),
        install_requires=[
            'future',
            'enum34;python_version<"3.4"'
        ],
        tests_require=['pytest', 'pytest-cov'],
        setup_requires=['setuptools_scm'] + sphinx,
        cmdclass={'test': PyTest}
    )


if __name__ == "__main__":
    setup_package()
