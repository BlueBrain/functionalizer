"""Debugging helpers to dump additional data while filtering."""
import pathlib

from spykfunc.utils import get_logger

logger = get_logger(__name__)


class CSVWriter:
    """Helper class to debug via CSV dumps."""

    def __init__(self, path: pathlib.Path):
        """Create the helper class.

        Args:
            path: the Spykfunc output directory to analyze
        """
        self._basedir = path / "_debug"
        if not self._basedir.is_dir():
            self._basedir.mkdir(parents=True)
        self._stage = 1

    def __call__(self, df, filename):
        """Write out a CSV file of a dataframe."""
        end = "" if filename.endswith(".csv") else ".csv"
        path = self._basedir / f"{self._stage:02d}_{filename}{end}"

        logger.debug("Writing debug information to %s", path)
        df.toPandas().to_csv(path, index=False)

        self._stage += 1


def _write_csv(*_):
    pass


def enable_debug(basepath: str):
    """Enable debugging and write data to `basepath`."""
    global _write_csv  # pylint: disable=global-statement
    logger.info("Activating debug output...")
    _write_csv = CSVWriter(pathlib.Path(basepath))
