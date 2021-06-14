import pathlib

from spykfunc.utils import get_logger

logger = get_logger(__name__)


class CSVWriter:
    """Helper class to debug via CSV dumps
    """
    def __init__(self, path: pathlib.Path):
        self._basedir = path / "_debug"
        if not self._basedir.is_dir():
            self._basedir.mkdir(parents=True)
        self._stage = 1

    def __call__(self, df, filename):
        """Write out a CSV file of a dataframe
        """
        end = "" if filename.endswith(".csv") else ".csv"
        path = self._basedir / f"{self._stage:02d}_{filename}{end}"

        logger.debug("Writing debug information to %s", path)
        df.toPandas().to_csv(path, index=False)

        self._stage += 1


def _write_csv(df, filename):
    pass


def enable_debug(basepath: str):
    global _write_csv
    logger.info("Activating debug output...")
    _write_csv = CSVWriter(pathlib.Path(basepath))
