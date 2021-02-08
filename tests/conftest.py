#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    conftest.py for spykfunc.
    More about conftest.py under: https://pytest.org/latest/plugins.html
"""
from __future__ import print_function, absolute_import, division
import os
import pytest

from spykfunc import filters
from spykfunc.definitions import RunningMode as RM
from spykfunc.functionalizer import Functionalizer

DATADIR = os.path.join(os.path.dirname(__file__), "circuit_1000n")

ARGS = (
    os.path.join(DATADIR, "builderRecipeAllPathways.xml"),
    (os.path.join(DATADIR, "nodes.h5"), "All"),
    (os.path.join(DATADIR, "nodes.h5"), "All"),
    os.path.join(DATADIR, "morphologies/h5"),
    os.path.join(DATADIR, "touches/*.parquet")
)

filters.load()


@pytest.fixture(scope='session', name='fz')
def fz_fixture(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('filters')
    cdir = tmpdir.join('check')
    odir = tmpdir.join('out')
    return Functionalizer(
        filters=RM.FUNCTIONAL.value,
        checkpoint_dir=str(cdir),
        output_dir=str(odir)
    ).init_data(*ARGS)


@pytest.fixture(scope='session', name='gj')
def gj_fixture(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('gap_junctions')
    cdir = tmpdir.join('check')
    odir = tmpdir.join('out')
    args = list(ARGS[:-1]) + [os.path.join(DATADIR, "gap_junctions/touches*.parquet")]
    return Functionalizer(
        filters=RM.GAP_JUNCTIONS.value,
        checkpoint_dir=str(cdir),
        output_dir=str(odir)
    ).init_data(*args)


def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true",
                     default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed ({})".format(previousfailed.name))
