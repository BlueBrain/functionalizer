import os

from spykfunc.utils import get_logger

logger = get_logger(__name__)

# Control variable that outputs intermediate calculations, and makes processing slower
_DEBUG = False


class CSVWriter:
    """Helper class to debug via CSV dumps
    """
    def __init__(self):
        if not os.path.isdir("_debug"):
            os.makedirs("_debug")
        self._stage = 1

    def __call__(self, df, filename):
        """Write out a CSV file of a dataframe
        """
        end = "" if filename.endswith(".csv") else ".csv"
        filename = f"_debug/{self._stage:02d}_{filename}{end}"

        logger.debug("Writing debug information to %s", filename)
        df.toPandas().to_csv(filename, index=False)

        self._stage += 1


def _write_csv(df, filename):
    pass


def enable_debug():
    global _DEBUG
    global _write_csv
    logger.info("Activating debug output...")
    _DEBUG = True
    _write_csv = CSVWriter()
