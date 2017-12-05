#!/usr/bin/env pyspark

import sys
import argparse


# ------------------------------------
# Executed from the SHELL
# ------------------------------------
def _create_parser():
    parser = argparse.ArgumentParser(description="spykfunc is a pyspark implementation of pyspark.")
    parser.add_argument("recipe_file", help="the XML recipe file")
    parser.add_argument("mvd_file",    help="the input mvd file")
    parser.add_argument("morpho_dir",  help="the H5 morphology database directory")
    parser.add_argument("touch_files", help="The first binary touch file (touches.0)")
    parser.add_argument("--s2s",
                        help="s2s pruning only. If omitted s2f will be run",
                        action="store_true", dest="s2s", default=False)
    parser.add_argument("--no-hdf5",
                        help="Dont create result to HDF5, write out in parquet",
                        action="store_true", dest="resultparquet", default=False)
    parser.add_argument("--output-dir",
                        help="Specify output directory. Defaults to ./spykfunc_output")
    parser.add_argument("--spark-opts",
                        help="All arguments to configure the spark session. Use with quotation marks. E.g. "
                             "--spark-opts \"--master spark://111.222.333.444:7077 --spark.conf.xx 123\"")
    return parser


# Singleton parser
arg_parser = _create_parser()


# *****************************************************
# Application scripts
# *****************************************************

def spykfunc():
    """ The main entry-point Spykfunc script. It will launch Spykfunc with a spark instance
        (created if not provided), run the default filters and export to NRN format (hdf5).
    """
    # Will exit with code 2 if problems in args
    options = arg_parser.parse_args()

    # If everything seems ok, import functionalizer.
    # Like this we can use the parser without starting with pyspark or spark-submit
    # NOTE: Scripts must be executed from pyspark or spark-submit to import pyspark
    from spykfunc.functionalizer import session
    fuzer = session(options)
    if fuzer is None:
        return 1

    status = fuzer.process_filters()
    if status > 0:
        return status

    status = fuzer.export_results(format_parquet=options.resultparquet)
    return status


# Defaults to execute run_functionalizer command
if __name__ == "__main__":
    sys.exit(spykfunc())
