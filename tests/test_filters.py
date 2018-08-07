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
NUM_AFTER_FILTER = 169778


@pytest.mark.slow
@pytest.mark.incremental
class TestFilters(object):
    """Sequential tests of filters.
    """

    def test_neuron_columns(self, fz):
        """Test that non-essential columns are not broadcasted
        """
        assert 'layer' not in fz._circuit.neurons.columns
        assert 'morphology' not in fz._circuit.neurons.columns

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

        cols = ["u_syn", "depression_time", "facilitation_time",
                "conductance", "decay_time", "n_rrp_vesicles"]

        df = sm.read.load(os.path.join(fz.output_directory, "circuit.parquet"))
        props = df.groupBy("connected_neurons_pre",
                           "connected_neurons_post",
                           *cols).count().cache()
        conns = props.groupBy("connected_neurons_pre", "connected_neurons_post").count()

        assert props.where("count > 1").count() > 0, \
            "need at least one connection with more than one touch"
        assert conns.where("count > 1").count() == 0, \
            "can only have one property setting per connection"

        props = df.where((F.col("connected_neurons_pre") == 432) &
                         (F.col("connected_neurons_post") == 179)) \
                  .select(*cols).toPandas()
        want = pd.DataFrame([(0.155061, 289.549713, 12.866484,
                              0.825946, 7.958006, 1.0)],
                            dtype='float32',
                            columns=cols)
        assert props.drop_duplicates().round(5).equals(want.round(5))

    def test_writeout_hdf5(self, fz):
        """Simple test for h5 export.
        """
        fz.export_results(format_hdf5=True)
