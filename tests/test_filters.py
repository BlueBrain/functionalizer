"""Test the various filters
"""

import os
import numpy as np
import pandas as pd
import pytest
import pyspark.sql.functions as F
from conftest import ARGS, DATADIR, create_functionalizer
import sparkmanager as sm

from spykfunc.definitions import RunningMode as RM
from spykfunc.utils.spark import cache_broadcast_single_part
from spykfunc import schema

NUM_AFTER_DISTANCE = 226301
NUM_AFTER_TOUCH = 221686
NUM_AFTER_FILTER = 16460


@pytest.mark.slow
def test_fixed_probabilities(tmp_path_factory):
    def layer_counts(circuit):
        mdf = cache_broadcast_single_part(
            sm.createDataFrame(
                enumerate(circuit.source.mtype_values),
                schema.indexed_strings(["mtype_i", "mtype_name"]),
            )
        )
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

    tmpdir = tmp_path_factory.mktemp("fixed_probabilities")
    fz = create_functionalizer(tmpdir, ["ReduceAndCut"]).init_data(
        os.path.join(DATADIR, "builderRecipeAllPathways_fixed.xml"), *ARGS[1:]
    )

    before = layer_counts(fz.circuit)
    fz.process_filters()
    after = layer_counts(fz.circuit)

    assert after["L1"] == before["L1"]
    assert after["L23"] < before["L23"]
    assert "L4" not in after
    assert "L5" not in after
    assert "L6" not in after


@pytest.mark.slow
class TestFilters(object):
    """Sequential tests of filters."""

    def test_distance(self, fz):
        """Test the distance rules: deterministic"""
        fz.process_filters(filters=["BoutonDistance"])
        fz.circuit.df.show()
        assert fz.circuit.df.count() == NUM_AFTER_DISTANCE

    def test_touch_filter(self, fz):
        """Test the bouton touch filter: deterministic"""
        fz.process_filters(filters=["BoutonDistance", "TouchRules"])
        assert fz.circuit.df.count() == NUM_AFTER_TOUCH

    def test_reduce_and_cut(self, fz):
        """Test the reduce and cut filter: not deterministic"""
        fz.process_filters(filters=["BoutonDistance", "TouchRules", "ReduceAndCut"])
        assert fz.circuit.df.count() == NUM_AFTER_FILTER

    def test_resume(self, fz, tmp_path_factory):
        """Make sure that resuming "works" """
        tmpdir = tmp_path_factory.mktemp("filters")
        fz2 = create_functionalizer(tmpdir).init_data(*ARGS)
        fz2.process_filters()
        original = fz.circuit.touches.count()
        count = fz2.circuit.touches.count()
        assert count == original

    def test_checkpoint_schema(self, fz, tmp_path_factory):
        """To conserve space, only touch columns should be written to disk"""
        basedir = tmp_path_factory.getbasetemp() / "filters0" / "check"
        files = [
            f for f in os.listdir(str(basedir)) if f.endswith(".ptable") or f.endswith(".parquet")
        ]
        for fn in files:
            df = sm.read.load(str(basedir / fn))
            assert all("src_" not in s and "dst_" not in s for s in df.schema.names)

    def test_overwrite(self, fz, tmp_path_factory):
        """Test that overwriting checkpointed data works"""
        tmpdir = tmp_path_factory.mktemp("filters")
        fz2 = create_functionalizer(tmpdir).init_data(*ARGS)
        fz2.process_filters(overwrite=True)
        original = fz.circuit.touches.count()
        count = fz2.circuit.touches.count()
        assert count == original

    def test_writeout(self, fz):
        """Simple test that saving results works."""
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

        df = sm.read.load(os.path.join(fz.output_directory, "circuit.parquet"))
        props = df.groupBy("source_node_id", "target_node_id", *cols).count().cache()
        conns = props.groupBy("source_node_id", "target_node_id").count()

        assert (
            props.where("count > 1").count() > 0
        ), "need at least one connection with more than one touch"
        assert (
            conns.where("count > 1").count() == 0
        ), "can only have one property setting per connection"

        props = (
            df.where((F.col("source_node_id") == 58) & (F.col("target_node_id") == 36))
            .select(*cols)
            .toPandas()
        )
        want = pd.DataFrame(
            [(0.118928, 635.527222, 20.275032, 0.602809, 1.771395, 1)], columns=cols
        )
        want["n_rrp_vesicles"] = want["n_rrp_vesicles"].astype("int16")
        have = props.drop_duplicates().round(5)
        want = want.round(5)
        assert have.equals(want)
