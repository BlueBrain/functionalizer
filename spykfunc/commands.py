#!/usr/bin/env pyspark

import sys
import argparse
from . import utils
from ._filtering import DatasetOperation
from . import filters # noqa
from .definitions import RunningMode as RM


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

    class _SplitAction(argparse.Action):
        """Dummy class to allow spiltting a comma separted list.
        """
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values[0].split(','))

    class _Formatter(argparse.HelpFormatter):
        """Dummy class to allow line-breaks in help

        An optional leading 'i|' will indent lines by four spaces.
        """
        def _split_lines(self, text, width):
            sw = 4
            res = []
            for line in text.splitlines():
                if line.startswith("i|"):
                    res.extend(" " * sw + l for l in super()._split_lines(line[2:], width - sw))
                else:
                    res.extend(super()._split_lines(line, width))
            return res

    parser = argparse.ArgumentParser(
        description="spykfunc is a pyspark implementation of functionalizer.",
        formatter_class=_Formatter
    )
    parser.add_argument("recipe_file", help="the XML recipe file")
    parser.add_argument("mvd_file",    help="the input mvd file")
    parser.add_argument("morpho_dir",  help="the H5 morphology database directory")
    parser.add_argument("touch_files",
                        help="the touch files (parquets); "
                             "a litertal blob expression is also accepted.",
                        nargs="+")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--s2s", "--structural", dest="filters",
                       help="structural pruning only with filters:\ni|" +
                            ", ".join(RM.STRUCTURAL),
                       action="store_const", const=RM.STRUCTURAL)
    group.add_argument("--s2f", "--functional", dest="filters",
                       help="functional pruning and filtering using:\ni|" +
                            ", ".join(RM.FUNCTIONAL),
                       action="store_const", const=RM.FUNCTIONAL)
    group.add_argument("--gap-junctions", dest="filters",
                       help="run filters for gap-junctions:\ni|" +
                            ", ".join(RM.GAP_JUNCTIONS),
                       action="store_const", const=RM.GAP_JUNCTIONS)
    group.add_argument("--filters", dest="filters",
                       help="run a list of custom filters (comma-separated), available:\ni|" +
                            ", ".join(DatasetOperation.modules()),
                       action=_SplitAction)
    parser.add_argument("--format-hdf5",
                        help="write/convert result to HDF5 (nrn.h5) rather than parquet",
                        action="store_true", default=False)
    parser.add_argument("--name",
                        help="name that will show up in the Spark logs, "
                             "defaults to 'Functionalizer'")
    parser.add_argument("--cache-dir",
                        help="specify directory to cache circuits converted to parquet, "
                             "defaults to OUTPUT_DIR/_mvd")
    parser.add_argument("--checkpoint-dir",
                        help="specify directory to store checkpoints, "
                             "defaults to OUTPUT_DIR/_checkpoints")
    parser.add_argument("--output-dir", default="spykfunc_output",  # see also `spykfunc/functionalizer.py`!
                        help="specify output directory, defaults to ./spykfunc_output")
    parser.add_argument("-c", "--configuration",
                        help="a configuration file to use; "
                             "see `--dump-configuration` for default settings")
    parser.add_argument("-p", "--spark-property", dest='overrides', action='append', default=[],
                        help="override single properties of the configuration, i.e.,\ni|"
                             "`--spark-property spark.master=spark://1.2.3.4:7077`\n"
                             "may be specified multiple times.")
    parser.add_argument("--dump-configuration", action=_ConfDumpAction,
                        help="show the configuration including modifications via options prior "
                             "to this flag and exit")
    parser.add_argument("--overwrite",
                        help="overwrite the result of selected intermediate steps, "
                             "forcing their recomputation; "
                             "possible values: F (for filtered, implies E) "
                             "or E (for extended with synapse properties)",
                        choices=("F", "E"), const="F", nargs="?", default="")
    parser.add_argument("--no-morphos",
                        help="run spykfunc without morphologies; "
                             "note: ChC cells wont be patched and "
                             "branch_type field won't be part of the result",
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
