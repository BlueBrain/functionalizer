# pylint: disable=unsupported-assignment-operation,unsubscriptable-object
# pandas seems to trigger these warnings in 1.4+
"""Compare two outputs of Spykfunc (coalesced)."""

import argparse
import os
import sys

import libsonata
import pandas as pd
import pyarrow.parquet as pq

from functionalizer.schema import LEGACY_MAPPING


def load_parquet(path: str) -> pd.DataFrame:
    """Load Parquet into pandas.

    Ensures that certain properties are met w.r.t. metadata. Also renames columns from
    legacy formats to the latest standard.
    """
    schema = pq.ParquetDataset(os.path.join(path, "_metadata")).schema
    meta = {k.decode(): v.decode() for k, v in schema.to_arrow_schema().metadata.items()}

    assert "source_population_name" in meta
    assert "source_population_size" in meta
    assert "target_population_name" in meta
    assert "target_population_size" in meta

    dataset = pq.ParquetDataset(path)
    return dataset.read().to_pandas().rename(columns=LEGACY_MAPPING)


def run():
    """Entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("circuit", help="the circuit file with neuron definitions")
    parser.add_argument("baseline", help="the output directory to compare to")
    parser.add_argument("comparison", help="the output directory to compare with")
    parser.add_argument(
        "--relative",
        help="threshold for relative change",
        dest="thres_rel",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--absolute",
        help="threshold for absolute change",
        dest="thres_abs",
        type=float,
        default=0.0,
    )
    args = parser.parse_args()

    base = load_parquet(args.baseline)
    comp = load_parquet(args.comparison)

    difference = set(base.columns) ^ set(comp.columns)
    assert len(difference) == 0, f"Changed columns: {','.join(difference)}"

    pop = libsonata.NodeStorage(args.circuit).open_population("All")
    sel = libsonata.Selection([(0, len(pop))])
    mtypes = pd.DataFrame(
        {"id": sel.flatten(), "mtype": pop.get_attribute("mtype", sel)}
    ).set_index("id")

    base = base.join(mtypes, on="source_node_id")
    base = base.join(mtypes, on="target_node_id", lsuffix="_pre", rsuffix="_post")
    comp = comp.join(mtypes, on="source_node_id")
    comp = comp.join(mtypes, on="target_node_id", lsuffix="_pre", rsuffix="_post")

    base_stats = base.groupby(["mtype_pre", "mtype_post"]).size()
    comp_stats = comp.groupby(["mtype_pre", "mtype_post"]).size()

    combined = pd.DataFrame({"base": base_stats, "comp": comp_stats}).fillna(0)
    combined["diff_abs"] = (combined.base - combined.comp).abs()
    combined["diff_rel"] = combined.diff_abs / combined[["base", "comp"]].max(1)
    combined = combined.fillna(0).sort_values(["diff_rel", "diff_abs"], ascending=False)

    combined = combined[(combined.diff_abs > args.thres_abs) & (combined.diff_rel > args.thres_rel)]

    if len(combined) > 0:
        added = combined[combined.base == 0]
        lost = combined[combined.comp == 0]

        changed = combined[(combined.base > 0) & (combined.comp > 0)]

        if len(changed) > 0:
            print("\nDifferences in connections")
            print("==========================")
            print(changed.to_string(max_rows=20))
        if len(added) > 0:
            print(f"\nConnections added in {args.comparison}")
            print("=====================" + "=" * len(args.comparison))
            print(added.to_string(max_rows=20))
        if len(lost) > 0:
            print(f"\nConnections removed from {args.comparison}")
            print("=========================" + "=" * len(args.comparison))
            print(lost.to_string(max_rows=20))

        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    run()
