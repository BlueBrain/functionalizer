import os
import pytest
import spykfunc

DATADIR = os.path.join(os.path.dirname(__file__), "circuit_1000n")

ARGS = (
    os.path.join(DATADIR, "builderRecipeAllPathways.xml"),
    os.path.join(DATADIR, "circuit.mvd3"),
    os.path.join(DATADIR, "morphologies/h5"),
    os.path.join(DATADIR, "touches/*.parquet")
)


@pytest.fixture(scope='session', name='fz')
def fz_fixture(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('filters')
    cdir = tmpdir.join('check')
    odir = tmpdir.join('out')
    kwargs = {
        'checkpoint-dir': str(cdir),
        'output-dir': str(odir)
    }
    return spykfunc.session(*ARGS, **kwargs)
