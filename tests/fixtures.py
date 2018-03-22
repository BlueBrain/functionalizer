import os
import pytest
import spykfunc

CURDIR = os.path.dirname(__file__)

ARGS = (
    os.path.join(CURDIR, "circuit_1000n/builderRecipeAllPathways.xml"),
    os.path.join(CURDIR, "circuit_1000n/circuit.mvd3"),
    os.path.join(CURDIR, "circuit_1000n/touches/*.parquet")
)


@pytest.fixture(scope='session')
def fz(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('filters')
    cdir = tmpdir.join('check')
    odir = tmpdir.join('out')
    kwargs = {
        'checkpoint-dir': str(cdir),
        'output-dir': str(odir)
    }
    return spykfunc.session(*ARGS, **kwargs)
