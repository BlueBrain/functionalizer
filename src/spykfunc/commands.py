"""Command line interface for Spykfunc."""
import os
import sys
import argparse
from datetime import datetime
from . import filters, utils
from .filters import DatasetOperation
from .definitions import RunningMode as RM, SortBy


filters.load()


def _parse_args(args=None) -> argparse.Namespace:
    """Handle arguments passed through the commandline.

    Takes a few corner cases into account w.r.t. backwards compatible arguments, and adds
    SONATA specific checks to arguments.
    """
    import json
    import libsonata

    if args is None:
        args = sys.argv[1:]

    class _ValidFile:
        """Check that a path is a file."""

        def __repr__(self):
            return "file"

        def __call__(self, filename):
            if not os.path.isfile(filename):
                raise ValueError(f"'{filename}' is not a valid file")
            return filename

    class _ValidNodes:
        """Check that the specified nodes are present."""

        def __init__(self):
            self.__storage = None
            self.__population = None
            self.__what = "node file"

        def __repr__(self):
            return self.__what

        def __call__(self, arg):
            if self.__storage is None:
                try:
                    self.__storage = libsonata.NodeStorage(arg)
                except Exception as e:
                    raise ValueError(f"'{arg}' is not a valid file") from e
            elif self.__population is None:
                try:
                    self.__population = self.__storage.open_population(arg)
                except Exception as e:
                    self.__what = (
                        "node population " f"(out of {', '.join(self.__storage.population_names)})"
                    )
                    raise ValueError(f"'{arg}' is not a valid node population") from e
            else:
                self.__what = "node argument"
                raise ValueError(f"'{arg}' is extraneous")
            return arg

    class _ValidNodeset:
        """Check that the specified nodes are present."""

        def __init__(self):
            self.__json = None
            self.__key = None
            self.__what = "nodeset file"

        def __repr__(self):
            return self.__what

        def __call__(self, arg):
            if self.__json is None:
                try:
                    with open(arg) as fd:
                        self.__json = json.load(fd)
                except Exception as e:
                    raise ValueError(f"'{arg}' is not a valid file") from e
            elif self.__key is None:
                try:
                    self.__key = self.__json[arg]
                except Exception as e:
                    self.__what = f"nodeset identifier (out of {', '.join(self.__json)})"
                    raise ValueError(f"'{arg}' is not a valid nodeset identifier") from e
            else:
                self.__what = "nodeset argument"
                raise ValueError(f"'{arg}' is extraneous")
            return arg

    class _ValidPath:
        """Check that a path is a file or a directory."""

        def __repr__(self):
            return "path"

        def __call__(self, path):
            if not os.path.isfile(path) and not os.path.isdir(path):
                raise ValueError(f"'{path}' is not a valid file")
            return path

    class _ConfDumpAction(argparse._HelpAction):
        """Dummy class to list default configuration and exit, just like `--help`."""

        def __call__(self, parser, namespace, values, option_string=None):
            from spykfunc.utils import Configuration

            kwargs = dict(overrides=namespace.overrides)
            if namespace.configuration:
                kwargs["configuration"] = namespace.configuration
            Configuration(namespace.output_dir, **kwargs).dump()
            parser.exit()

    class _SplitAction(argparse.Action):
        """Dummy class to allow spiltting a comma separted list."""

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, list(filter(len, values.split(","))))

    class _Formatter(argparse.HelpFormatter):
        """Dummy class to allow line-breaks in help.

        An optional leading 'i|' will indent lines by four spaces.
        """

        def _split_lines(self, text, width):
            sw = 4
            res = []
            for line in text.splitlines():
                if line.startswith("i|"):
                    res.extend(" " * sw + r for r in super()._split_lines(line[2:], width - sw))
                else:
                    res.extend(super()._split_lines(line, width))
            return res

    parser = argparse.ArgumentParser(
        description="spykfunc is a pyspark implementation of functionalizer.",
        formatter_class=_Formatter,
    )
    gfilter = parser.add_argument_group("filter options")
    group = gfilter.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--s2s",
        "--structural",
        dest="filters",
        help="structural pruning only with filters:\ni|" + ", ".join(RM.STRUCTURAL.value),
        action="store_const",
        const=RM.STRUCTURAL.value,
    )
    group.add_argument(
        "--s2f",
        "--functional",
        dest="filters",
        help="functional pruning and filtering using:\ni|" + ", ".join(RM.FUNCTIONAL.value),
        action="store_const",
        const=RM.FUNCTIONAL.value,
    )
    group.add_argument(
        "--gap-junctions",
        dest="filters",
        help="run filters for gap-junctions:\ni|" + ", ".join(RM.GAP_JUNCTIONS.value),
        action="store_const",
        const=RM.GAP_JUNCTIONS.value,
    )
    group.add_argument(
        "--merge",
        dest="filters",
        help="merge input files without running any filters",
        action="store_const",
        const=[],
    )
    group.add_argument(
        "--filters",
        dest="filters",
        help="run a list of custom filters (comma-separated), available:\ni|"
        + ", ".join(DatasetOperation.modules()),
        action=_SplitAction,
    )
    ginput = parser.add_argument_group("input options")
    ginput.add_argument(
        "--from",
        dest="source",
        nargs=2,
        type=_ValidNodes(),
        metavar=("FILENAME", "POPULATION"),
        help="path and name for the source population",
    )
    ginput.add_argument(
        "--from-nodeset",
        dest="source_nodeset",
        nargs=2,
        type=_ValidNodeset(),
        metavar=("FILENAME", "NODESET"),
        default=[None] * 2,
        help="path and name for the source population",
    )
    ginput.add_argument(
        "--to",
        dest="target",
        nargs=2,
        type=_ValidNodes(),
        metavar=("FILENAME", "POPULATION"),
        help="path and name for the target population",
    )
    ginput.add_argument(
        "--to-nodeset",
        dest="target_nodeset",
        nargs=2,
        type=_ValidNodeset(),
        metavar=("FILENAME", "NODESET"),
        default=[None] * 2,
        help="path and name for the target population",
    )
    ginput.add_argument("--recipe", type=_ValidFile(), help="the XML recipe file")
    ginput.add_argument(
        "--morphologies",
        type=_ValidPath(),
        nargs="+",
        help="the morphology database path, and optionally the spine " "morphology directory",
    )
    goutput = parser.add_argument_group("output options")
    goutput.add_argument(
        "--cache-dir",
        help="specify directory to cache circuits converted to parquet, "
        "defaults to OUTPUT_DIR/_circuits",
    )
    goutput.add_argument(
        "--checkpoint-dir",
        help="specify directory to store checkpoints, " "defaults to OUTPUT_DIR/_checkpoints",
    )
    goutput.add_argument(
        "--output-dir",
        default="spykfunc_output",
        # see also `spykfunc/functionalizer.py`!
        help="specify output directory, defaults to ./spykfunc_output",
    )
    goutput.add_argument(
        "--output-order",
        help="which sorting to apply to the output, " "defaults to post-view.",
        choices=[v.name.lower() for v in SortBy],
        default="post",
        dest="order",
    )
    goutput.add_argument(
        "--overwrite",
        help="overwrite the result of selected intermediate steps, "
        "forcing their recomputation; "
        "possible values: F (for filtered, implies E) "
        "or E (for extended with synapse properties)",
        choices=("F", "E"),
        const="F",
        nargs="?",
        default="",
    )
    gadv = parser.add_argument_group("advanced options")
    gadv.add_argument(
        "--dry-run",
        help="do not run any filters, only validate the recipe.",
        default=False,
        action="store_true",
    )
    gadv.add_argument(
        "--strict",
        help="turn any warnings emitted into errors, useful for recipe validation",
        default=False,
        action="store_true",
    )
    gadv.add_argument(
        "--debug",
        help="enable additional debug output, may slow down execution (default)",
        default=True,
        action="store_true",
        dest="debug",
    )
    gadv.add_argument(
        "--no-debug",
        help="disable additional debug output",
        action="store_false",
        dest="debug",
    )
    gadv.add_argument(
        "--name",
        help="name that will show up in the Spark logs, " "defaults to 'Functionalizer'",
    )
    gadv.add_argument(
        "-c",
        "--configuration",
        help="a configuration file to use; " "see `--dump-configuration` for default settings",
    )
    gadv.add_argument(
        "-p",
        "--spark-property",
        dest="overrides",
        action="append",
        default=[],
        help="override single properties of the configuration, i.e.,\ni|"
        "`--spark-property spark.master=spark://1.2.3.4:7077`\n"
        "may be specified multiple times.",
    )
    gadv.add_argument(
        "--dump-configuration",
        action=_ConfDumpAction,
        help="show the configuration including modifications via options prior "
        "to this flag and exit",
    )
    parser.add_argument(
        "edges",
        nargs="+",
        help="the edge files (SONATA or parquet: also directories for parquet)",
    )

    args = parser.parse_args(args)

    if len(args.filters) > 0:
        missing = []
        if not args.recipe:
            missing.append("recipe")
        if not args.morphologies:
            missing.append("morphologies")
        if not args.source:
            missing.append("source nodes")
        if not args.target:
            missing.append("target nodes")
        if missing:
            parser.error(f"to use filters, please also specify: {','.join(missing)}.")
        if len(args.morphologies) > 2:
            parser.error("can only pass regular and spine morphologies")

    return args


# *****************************************************
# Application scripts
# *****************************************************


def spykfunc() -> int:
    """The main entry-point Spykfunc script.

    It will launch Spykfunc with a spark instance (created if not provided), run the
    default filters and export.
    """
    from spykfunc.functionalizer import Functionalizer

    start = datetime.now()

    # Will exit with code 2 if problems in args
    options = _parse_args()
    logger = utils.get_logger(__name__)

    try:
        args = vars(options)
        fz = Functionalizer(**args)
        fz.init_data(
            options.recipe,
            options.source,
            options.source_nodeset,
            options.target,
            options.target_nodeset,
            options.morphologies,
            options.edges,
        )
        fz.process_filters(overwrite="F" in options.overwrite.upper())
        fz.export_results(
            order=getattr(SortBy, options.order.upper()),
        )
    except Exception:
        logger.error(utils.format_cur_exception())
        return 1

    logger.info("Functionalizer job complete in %s.", datetime.now() - start)
    return 0


# Defaults to execute run_functionalizer command
if __name__ == "__main__":
    sys.exit(spykfunc())
