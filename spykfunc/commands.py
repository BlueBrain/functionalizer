#!/usr/bin/env pyspark

from spykfunc.functionalizer import session
import sys
import argparse


# ------------------------------------
# Executed from the SHELL
# ------------------------------------
def _create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("recipe_file", help="the XML recipe file")
    parser.add_argument("mvd_file",    help="the input mvd file")
    parser.add_argument("morpho_dir",  help="the H5 morphology database directory")
    parser.add_argument("touch_files", help="the input detector binary file")
    parser.add_argument("--s2s",
                        help="s2s pruning only. If omitted s2f will be run",
                        action="store_true", dest="s2s", default=False)

    return parser


# Singleton
arg_parser = _create_parser()


def run_functionalizer():
    # Will exit with code 2 if problems in args
    options = arg_parser.parse_args()
    fuzer = session(options)

    status = fuzer.process_filters()
    if status > 0:
        return status

    status = fuzer.export_results()
    return status


# Defaults to execute run_functionalizer command
if __name__ == "__main__":
    sys.exit(run_functionalizer())
