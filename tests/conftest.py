#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
conftest.py for spykfunc.
More about conftest.py under: https://pytest.org/latest/plugins.html
"""

from pathlib import Path

import pytest
from spykfunc import filters
from spykfunc.definitions import RunningMode as RM
from spykfunc.functionalizer import Functionalizer

DATADIR = Path(__file__).parent / "circuit_1000n"
CONFIGURATION = Path(__file__).parent.parent / "src" / "spykfunc" / "data" / "desktop.properties"

CIRCUIT_CONFIG = DATADIR / "circuit_config.json"

ARGS = (
    DATADIR / "recipe.json",
    CIRCUIT_CONFIG,
    None,
    None,
    None,
    None,
    [str(DATADIR / "touches" / "*.parquet")],
)

filters.load()


def create_functionalizer(tmpdir, filters=None):
    filters = filters or RM.FUNCTIONAL.value
    cdir = tmpdir / "check"
    odir = tmpdir / "out"
    return Functionalizer(
        filters=filters,
        configuration=CONFIGURATION,
        checkpoint_dir=str(cdir),
        output_dir=str(odir),
    )


@pytest.fixture
def circuit_config():
    return CIRCUIT_CONFIG


@pytest.fixture(scope="session", name="fz")
def fz_fixture(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("filters")
    return create_functionalizer(tmpdir, RM.FUNCTIONAL.value).init_data(*ARGS)


@pytest.fixture(scope="session", name="gj")
def gj_fixture(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("gap_junctions")
    args = [DATADIR / ".." / "recipe" / "recipe_gap_junctions.json"]
    args.extend(ARGS[1:-1])
    args.append([str(DATADIR / "gap_junctions/touches*.parquet")])
    return create_functionalizer(
        tmpdir,
        RM.GAP_JUNCTIONS.value,
    ).init_data(*args)


def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow")


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
