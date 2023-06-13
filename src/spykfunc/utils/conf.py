"""Small configuration shim module."""
import io
import os
import sys

from pathlib import Path
import jprops

from ._misc import get_logger

logger = get_logger(__name__)


class Configuration(dict):
    """Manage Spark and other configurations."""

    default_filename = Path(__file__).parent.parent / "data" / "default.properties"
    """:property: filename storing the defaults"""

    def __init__(self, outdir, filename=None, overrides=None):
        """Provide a configuaration dictionary.

        Optionally provided `kwargs` will override information loaded from
        the file provided by `filename`.

        :param outdir: output directory to save Spark event log and
                       warehouse information if not provided
        :param filename: alternative file to load
        """
        super().__init__()
        outdir = Path(outdir) / "_spark"
        self.__filename = Path(filename or self.default_filename)
        with self.__filename.open() as fd:
            for k, v in jprops.iter_properties(fd):
                self[k] = v
        if overrides:
            fd = io.StringIO("\n".join([str(s) for s in overrides]))
            for k, v in jprops.iter_properties(fd):
                self[k] = v

        self["spark.driver.extraJavaOptions"] = (
            f'"-Dderby.system.home={outdir.resolve()}" '
            f'{self.get("spark.driver.extraJavaOptions", "")}'
        )
        self.setdefault("spark.eventLog.dir", str(outdir.resolve() / "eventlog"))
        self.setdefault("spark.sql.warehouse.dir", str(outdir.resolve() / "warehouse"))
        for k in ["spark.eventLog.dir", "spark.sql.warehouse.dir"]:
            Path(self[k]).mkdir(parents=True, exist_ok=True)

        if master := os.environ.get("PYSPARK_MASTER"):
            logger.info("Connecting to PYSPARK_MASTER: %s", master)
            self.setdefault("spark.master", master)

        if parallelism := os.environ.get("PYSPARK_PARALLELISM"):
            logger.info(
                "Defaulting parallelism and shuffles to PYSPARK_PARALLELISM: %s", parallelism
            )
            self.setdefault("spark.default.parallelism", parallelism)
            self.setdefault("spark.sql.shuffle.partitions", parallelism)

    def __call__(self, prefix):
        """Yield all key, value pairs that match the prefix."""
        prefix = prefix.split(".")
        for k, v in self.items():
            path = k.split(".")[: len(prefix)]
            if path == prefix:
                yield k, v

    def dump(self):
        """Dump the default configuration to the terminal."""

        def path2str(s):
            if isinstance(s, Path):
                return str(s)
            return s

        seen = set()
        with open(self.__filename) as fd:
            for k, v in jprops.iter_properties(fd, comments=True):
                if k is jprops.COMMENT:
                    print("#" + v)
                    continue
                jprops.write_property(sys.stdout, k, self[k])
                seen.add(k)
        print("\n# below: non-default, generated, and user-overridden parameters")
        for k in sorted(set(self.keys()) - seen):
            jprops.write_property(sys.stdout, k, path2str(self[k]))
