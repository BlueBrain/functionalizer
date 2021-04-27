"""Compare two outputs of Spykfunc (coalesced)
"""
import argparse
import sys

import libsonata
import pandas as pd
import pyarrow.parquet as pq

from spykfunc.schema import LEGACY_MAPPING


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

    base = (
        pq.ParquetDataset(args.baseline)
        .read()
        .to_pandas()
        .rename(columns=LEGACY_MAPPING)
    )
    comp = (
        pq.ParquetDataset(args.comparison)
        .read()
        .to_pandas()
        .rename(columns=LEGACY_MAPPING)
    )

    pop = libsonata.NodeStorage(args.circuit.encode()).open_population("All")
    mtypes = pd.DataFrame({"mtype": pop.enumeration_values("mtype")})

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

    combined = combined[
        (combined.diff_abs > args.thres_abs) & (combined.diff_rel > args.thres_rel)
    ]

    if len(combined) > 0:
        added = combined[combined.base == 0]
        lost = combined[combined.comp == 0]

        changed = combined[(combined.base > 0) & (combined.comp > 0)]

        if len(changed) > 0:
            print("\nDifferences in connections")
            print("==========================")
            print(changed.to_string(max_rows=20))
        if len(added) > 0:
            print("\nConnections added")
            print("=================")
            print(added.to_string(max_rows=20))
        if len(lost) > 0:
            print("\nConnections removed")
            print("===================")
            print(lost.to_string(max_rows=20))

        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    run()
