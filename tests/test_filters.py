"""Test the various filters
"""

import os
import pandas as pd
import pytest
import spykfunc
import pyspark.sql.functions as F
from spykfunc.definitions import RunningMode
from conftest import ARGS
import sparkmanager as sm

NUM_AFTER_DISTANCE = 2264809
NUM_AFTER_TOUCH = 2218004
NUM_AFTER_FILTER = 170113


@pytest.mark.slow
@pytest.mark.incremental
class TestFilters(object):
    """Sequential tests of filters.
    """

    def test_distance(self, fz):
        """Test the distance rules: deterministic
        """
        fz.filter_by_rules(mode=RunningMode.S2S)
        assert fz.circuit.count() == NUM_AFTER_DISTANCE

    def test_touch_filter(self, fz):
        """Test the bouton touch filter: deterministic
        """
        fz.filter_by_rules(mode=RunningMode.S2F)
        assert fz.circuit.count() == NUM_AFTER_TOUCH

    def test_reduce_and_cut(self, fz):
        """Test the reduce and cut filter: not deterministic
        """
        fz.run_reduce_and_cut()
        assert fz.circuit.count() == NUM_AFTER_FILTER

    def test_resume(self, fz, tmpdir_factory):
        """Make sure that resuming "works"
        """
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
        assert count == original

    def test_checkpoint_schema(self, fz, tmpdir_factory):
        """To conserve space, only touch columns should be written to disk
        """
        basedir = tmpdir_factory.getbasetemp().join('filters0').join('check')
        files = [
            'filter_reduced_touches.ptable',
            'filter_rules_RunningMode.S2F.parquet',
            'filter_rules_RunningMode.S2S.parquet'
        ]
        for fn in files:
            df = sm.read.load(str(basedir.join(fn)))
            assert all('src_' not in s and 'dst_' not in s for s in df.schema.names)

    def test_overwrite(self, fz, tmpdir_factory):
        """Test that overwriting checkpointed data works
        """
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
        assert count == original

    def test_writeout(self, fz):
        """Simple test that saving results works.
        """
        fz.export_results()

        df = sm.read.load(os.path.join(fz.output_directory, "nrn.parquet"))
        props = df.groupBy("pre_gid", "post_gid", "u", "d", "f", "gsyn", "dtc").count().cache()
        conns = props.groupBy("pre_gid", "post_gid").count()

        assert props.where("count > 1").count() > 0, \
            "need at least one connection with more than one touch"
        assert conns.where("count > 1").count() == 0, \
            "can only have one property setting per connection"

        props = df.where((F.col("pre_gid") == 618) & (F.col("post_gid") == 608)) \
                  .select("gsyn", "u", "d", "f", "dtc").toPandas()
        want = pd.DataFrame([(0.41551604866981506, 0.484548956155777, 656.5263671875,
                              11.101598739624023, 1.8590598106384277)],
                            dtype='float64',
                            columns=["gsyn", "u", "d", "f", "dtc"])
        assert props.drop_duplicates().equals(want)

    def test_writeout_hdf5(self, fz):
        """Simple test for h5 export.
        """
        fz.export_results(format_hdf5=True)
