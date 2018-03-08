"""Test the various filters
"""

import os
import pytest
import spykfunc

CURDIR = os.path.dirname(__file__)

ARGS = (
    os.path.join(CURDIR, "circuit_1000n/builderRecipeAllPathways.xml"),
    os.path.join(CURDIR, "circuit_1000n/circuit.mvd3"),
    os.path.join(CURDIR, "circuit_1000n/touches/*.parquet")
)

NUM_AFTER_DISTANCE = 2264809
NUM_AFTER_TOUCH = 2218004
NUM_AFTER_FILTER = 169000  # To be used with tolerance defined below

TOLERANCE = 0.02  # Statistical tolerance in %


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


@pytest.mark.slow
@pytest.mark.incremental
class TestFilters(object):
    def test_distance(self, fz):
        """Test the distance rules: deterministic
        """
        fz.filter_by_soma_axon_distance()
        assert fz.circuit.count() == NUM_AFTER_DISTANCE

    def test_touch_filter(self, fz):
        """Test the bouton touch filter: deterministic
        """
        fz.filter_by_touch_rules()
        assert fz.circuit.count() == NUM_AFTER_TOUCH

    def test_reduce_and_cut(self, fz):
        """Test the bouton touch filter: deterministic
        """
        fz.run_reduce_and_cut()
        count = fz.circuit.count()
        assert abs(count - NUM_AFTER_FILTER) < TOLERANCE * NUM_AFTER_FILTER

    def test_resume(self, fz, tmpdir_factory):
        tmpdir = tmpdir_factory.mktemp('filters')
        cdir = tmpdir.join('check')
        odir = tmpdir.join('out')
        kwargs = {
            'checkpoint-dir': str(cdir),
            'output-dir': str(odir)
        }
        fz2 = spykfunc.session(*ARGS, **kwargs)
        fz2.process_filters()
        original = fz.circuit.count()
        count = fz2.circuit.count()
        assert count != original
        assert abs(count - NUM_AFTER_FILTER) < TOLERANCE * NUM_AFTER_FILTER

    def test_overwrite(self, fz, tmpdir_factory):
        tmpdir = tmpdir_factory.mktemp('filters')
        cdir = tmpdir.join('check')
        odir = tmpdir.join('out')
        kwargs = {
            'checkpoint-dir': str(cdir),
            'output-dir': str(odir)
        }
        fz2 = spykfunc.session(*ARGS, **kwargs)
        fz2.process_filters(overwrite=True)
        original = fz.circuit.count()
        count = fz2.circuit.count()
        assert count != original
        assert abs(count - NUM_AFTER_FILTER) < TOLERANCE * NUM_AFTER_FILTER
