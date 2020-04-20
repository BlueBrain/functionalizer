"""Test the various filters
"""

import os
import numpy as np
import pandas as pd
import pytest
import pyspark.sql.functions as F
from conftest import ARGS, DATADIR
import sparkmanager as sm

from spykfunc.definitions import RunningMode as RM
from spykfunc.functionalizer import Functionalizer

NUM_AFTER_DISTANCE = 2264809
NUM_AFTER_TOUCH = 2218004
NUM_AFTER_FILTER = 169920


@pytest.mark.slow
def test_fixed_probabilities(tmpdir_factory):
    def layer_counts(circuit):
        mdf = circuit.source.mtype_df
        res = (
            circuit.df.join(
                mdf.withColumn("mtype", F.split(mdf.mtype_name, "_").getItem(0)),
                [F.col("src_mtype_i") == F.col("mtype_i")],
            )
            .groupby(F.col("mtype"))
            .count()
            .toPandas()
        )
        return dict(zip(res["mtype"], res["count"]))

    tmpdir = tmpdir_factory.mktemp("fixed_probabilities")
    cdir = tmpdir.join("check")
    odir = tmpdir.join("out")
    fz = Functionalizer(
        filters=["ReduceAndCut"], checkpoint_dir=str(cdir), output_dir=str(odir)
    ).init_data(os.path.join(DATADIR, "builderRecipeAllPathways_fixed.xml"), *ARGS[1:])

    before = layer_counts(fz.circuit)
    fz.process_filters()
    after = layer_counts(fz.circuit)

    assert after["L1"] == before["L1"]
    assert after["L23"] < before["L23"]
    assert "L4" not in after
    assert "L5" not in after
    assert "L6" not in after


@pytest.mark.slow
@pytest.mark.incremental
class TestFilters(object):
    """Sequential tests of filters.
    """

    def test_distance(self, fz):
        """Test the distance rules: deterministic
        """
        fz.process_filters(filters=["BoutonDistance"])
        fz.circuit.df.show()
        assert fz.circuit.df.count() == NUM_AFTER_DISTANCE

    def test_touch_filter(self, fz):
        """Test the bouton touch filter: deterministic
        """
        fz.process_filters(filters=["BoutonDistance", "TouchRules"])
        assert fz.circuit.df.count() == NUM_AFTER_TOUCH

    def test_reduce_and_cut(self, fz):
        """Test the reduce and cut filter: not deterministic
        """
        fz.process_filters(filters=["BoutonDistance", "TouchRules", "ReduceAndCut"])
        assert fz.circuit.df.count() == NUM_AFTER_FILTER

    def test_resume(self, fz, tmpdir_factory):
        """Make sure that resuming "works"
        """
        tmpdir = tmpdir_factory.mktemp("filters")
        cdir = tmpdir.join("check")
        odir = tmpdir.join("out")
        fz2 = Functionalizer(
            filters=RM.FUNCTIONAL.value, checkpoint_dir=str(cdir), output_dir=str(odir)
        ).init_data(*ARGS)
        fz2.process_filters()
        original = fz.circuit.touches.count()
        count = fz2.circuit.touches.count()
        assert count == original

    def test_checkpoint_schema(self, fz, tmpdir_factory):
        """To conserve space, only touch columns should be written to disk
        """
        basedir = tmpdir_factory.getbasetemp().join("filters0").join("check")
        files = [
            f
            for f in os.listdir(str(basedir))
            if f.endswith(".ptable") or f.endswith(".parquet")
        ]
        for fn in files:
            df = sm.read.load(str(basedir.join(fn)))
            assert all("src_" not in s and "dst_" not in s for s in df.schema.names)

    def test_overwrite(self, fz, tmpdir_factory):
        """Test that overwriting checkpointed data works
        """
        tmpdir = tmpdir_factory.mktemp("filters")
        cdir = tmpdir.join("check")
        odir = tmpdir.join("out")
        kwargs = {
            "functional": None,
            "checkpoint-dir": str(cdir),
            "output-dir": str(odir),
        }
        fz2 = Functionalizer(
            filters=RM.FUNCTIONAL.value, checkpoint_dir=str(cdir), output_dir=str(odir)
        ).init_data(*ARGS)
        fz2.process_filters(overwrite=True)
        original = fz.circuit.touches.count()
        count = fz2.circuit.touches.count()
        assert count == original

    def test_writeout(self, fz):
        """Simple test that saving results works.
        """
        fz.process_filters()
        fz.export_results()

        cols = [
            "u_syn",
            "depression_time",
            "facilitation_time",
            "conductance",
            "decay_time",
            "n_rrp_vesicles",
        ]
        dtypes = ["float32"] * 5 + ["int16"]

        df = sm.read.load(os.path.join(fz.output_directory, "circuit.parquet"))
        props = (
            df.groupBy("connected_neurons_pre", "connected_neurons_post", *cols)
            .count()
            .cache()
        )
        conns = props.groupBy("connected_neurons_pre", "connected_neurons_post").count()

        assert (
            props.where("count > 1").count() > 0
        ), "need at least one connection with more than one touch"
        assert (
            conns.where("count > 1").count() == 0
        ), "can only have one property setting per connection"

        props = (
            df.where(
                (F.col("connected_neurons_pre") == 56)
                & (F.col("connected_neurons_post") == 36)
            )
            .select(*cols)
            .toPandas()
        )
        want = pd.DataFrame(
            [(0.40889, 693.155029, 13.12169, 0.15601, 1.70221, 1)], columns=cols
        )
        for col, dtype in zip(cols, dtypes):
            want[col] = want[col].astype(dtype)
        have = props.drop_duplicates().round(5)
        want = want.round(5)
        assert have.equals(want)
