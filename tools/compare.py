"""Compare two outputs of Spykfunc (coalesced)
"""
import argparse
import sys

import mvdtool
import pandas as pd


def run():
    """Entry point.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("circuit", help="the circuit file with neuron definitions")
    parser.add_argument("baseline", help="the output directory to compare to")
    parser.add_argument("comparison", help="the output directory to compare with")
    parser.add_argument("--relative",
                        help="threshold for relative change",
                        dest="thres_rel",
                        type=float,
                        default=0.)
    parser.add_argument("--absolute",
                        help="threshold for absolute change",
                        dest="thres_abs",
                        type=float,
                        default=0.)
    args = parser.parse_args()

    base = pd.read_parquet(args.baseline)
    comp = pd.read_parquet(args.comparison)

    circuit = mvdtool.MVD3.File(args.circuit.encode())
    mtypes = pd.DataFrame({'mtype': circuit.all_mtypes})

    base = base.join(mtypes, on='connected_neurons_pre')
    base = base.join(mtypes,
                     on='connected_neurons_post',
                     lsuffix='_pre',
                     rsuffix='_post')
    comp = comp.join(mtypes, on='connected_neurons_pre')
    comp = comp.join(mtypes,
                     on='connected_neurons_post',
                     lsuffix='_pre',
                     rsuffix='_post')

    base_stats = base.groupby(['mtype_pre', 'mtype_post']).size()
    comp_stats = comp.groupby(['mtype_pre', 'mtype_post']).size()

    combined = pd.DataFrame({'base': base_stats, 'comp': comp_stats}).fillna(0)
    combined['diff_abs'] = (combined.base - combined.comp).abs()
    combined['diff_rel'] = combined.diff_abs / combined[['base', 'comp']].max(1)
    combined = combined.fillna(0) \
                       .sort_values(['diff_rel', 'diff_abs'], ascending=False)

    combined = combined[(combined.diff_abs > args.thres_abs)
                      & (combined.diff_rel > args.thres_rel)]

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


if __name__ == '__main__':
    run()
