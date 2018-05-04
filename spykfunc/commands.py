#!/usr/bin/env pyspark

import sys
import argparse
from . import utils


# ------------------------------------
# Executed from the SHELL
# ------------------------------------
def _create_parser():
    class _ConfDumpAction(argparse._HelpAction):
        """Dummy class to list default configuration and exit, just like `--help`.
        """
        def __call__(self, parser, namespace, values, option_string=None):
            from spykfunc.utils import Configuration
            kwargs = dict(overrides=namespace.overrides)
            if namespace.configuration:
                kwargs['configuration'] = namespace.configuration
            Configuration(namespace.output_dir, **kwargs).dump()
            parser.exit()

    parser = argparse.ArgumentParser(description="spykfunc is a pyspark implementation of functionalizer.")
    parser.add_argument("recipe_file", help="the XML recipe file")
    parser.add_argument("mvd_file",    help="the input mvd file")
    parser.add_argument("morpho_dir",  help="the H5 morphology database directory")
    parser.add_argument("touch_files",
                        help="The touch files (parquets). A litertal blob expression is also accepted.",
                        nargs="+")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--s2s",
                       help="s2s pruning only. If omitted s2f will be run",
                       action="store_true", default=False)
    group.add_argument("--s2f",
                       help="Run s2f. The default action",
                       action="store_true", default=True)
    parser.add_argument("--format-hdf5",
                        help="Write result to HDF5 rather than parquet",
                        action="store_true", default=False)
    parser.add_argument("--name",
                        help="Name that will show up in the Spark logs. Defaults to 'Functionalizer'")
    parser.add_argument("--checkpoint-dir",
                        help="Specify directory to store checkpoints. Defaults to OUTPUT_DIR/_checkpoints")
    parser.add_argument("--output-dir", default="spykfunc_output",  # see also `spykfunc/functionalizer.py`!
                        help="Specify output directory. Defaults to ./spykfunc_output")
    parser.add_argument("-c", "--configuration",
                        help="A configuration file to use. See `--dump-defaults` for default settings")
    parser.add_argument("-p", "--property", dest='overrides', action='append', default=[],
                        help="Override single properties of the configuration, i.e.,"
                             "`-p spark.master=spark://1.2.3.4:7077`. May be specified multiple times.")
    parser.add_argument("--dump-configuration", action=_ConfDumpAction,
                        help="Show the configuration including modifications via options prior to this "
                             "flag and exit")
    parser.add_argument("--overwrite",
                        help="Overwrite the result of selected intermediate steps, forcing their recomputation"
                             "Possible values: F (for filtered, implies E) or E (for extended with synapse properties)",
                        choices=("F", "E"), const="F", nargs="?", default="")
    parser.add_argument("--no-morphos",
                        help="Run spykfunc without morphologies. "
                             "Note: ChC cells wont be patched and branch_type field won't be part of the result",
                        action="store_true", default=False)
    return parser


# Singleton parser
arg_parser = _create_parser()


# *****************************************************
# Application scripts
# *****************************************************

def spykfunc():
    """ The main entry-point Spykfunc script. It will launch Spykfunc with a spark instance
        (created if not provided), run the default filters and export.
    """
    # Will exit with code 2 if problems in args
    options = arg_parser.parse_args()

    # If everything seems ok, import functionalizer.
    # Like this we can use the parser without starting with pyspark or spark-submit
    # NOTE: Scripts must be executed from pyspark or spark-submit to import pyspark
    from spykfunc.functionalizer import session
    logger = utils.get_logger(__name__)

    try:
        fuzer = session(options)
        fuzer.process_filters(overwrite="F" in options.overwrite.upper())
        fuzer.export_results(overwrite="E" in options.overwrite.upper())
    except Exception:
        logger.error(utils.format_cur_exception())
        return 1

    logger.info("Functionalizer job complete.")
    return 0


# Defaults to execute run_functionalizer command
if __name__ == "__main__":
    sys.exit(spykfunc())
