#!/usr/bin/env pyspark

import sys
import argparse
from . import utils


# ------------------------------------
# Executed from the SHELL
# ------------------------------------
def _create_parser():
    parser = argparse.ArgumentParser(description="spykfunc is a pyspark implementation of pyspark.")
    parser.add_argument("recipe_file", help="the XML recipe file")
    parser.add_argument("mvd_file",    help="the input mvd file")
    parser.add_argument("morpho_dir",  help="the H5 morphology database directory")
    parser.add_argument("touch_files", 
                        help="The touch files (parquets). A litertal blob expression is also accepted.",
                        nargs="+")
    parser.add_argument("--s2s",
                        help="s2s pruning only. If omitted s2f will be run",
                        action="store_true", dest="s2s", default=False)
    parser.add_argument("--format-hdf5",
                        help="Dont create result to HDF5, write out in parquet",
                        action="store_true", dest="resultparquet", default=False)
    parser.add_argument("--output-dir",
                        help="Specify output directory. Defaults to ./spykfunc_output")
    parser.add_argument("--spark-opts",
                        help="All arguments to configure the spark session. Use with quotation marks. E.g. "
                             "--spark-opts \"--master spark://111.222.333.444:7077 --spark.conf.xx 123\"")
    parser.add_argument("--overwrite",
                        help="Overwrite the result of selected intermediate steps, forcing their recomputation"
                             "Possible values: F (for filtered), E (for extended with synapse properties)"
                             "or both: \"FE\"",
                        default="")
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
    from spykfunc.functionalizer import session, ExtendedCheckpointAvail
    logger = utils.get_logger(__name__)

    try:
        fuzer = session(options)
        try:
            fuzer.process_filters(overwrite="F" in options.overwrite.upper())
        except ExtendedCheckpointAvail:
            # If a ExtendedCheckpoint is available and we don't want to overwrite
            pass
        fuzer.export_results(format_parquet=options.resultparquet,
                             overwrite="E" in options.overwrite.upper())
    except Exception:
        logger.error(utils.format_cur_exception())
        return 1

    logger.info("Functionalizer job complete.")
    return 0


# Defaults to execute run_functionalizer command
if __name__ == "__main__":
    sys.exit(spykfunc())
