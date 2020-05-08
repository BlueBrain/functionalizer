"""Small configuration shim module.
"""
from __future__ import print_function
import io
import sys

import jprops
try:
    from pathlib2 import Path
except ImportError:
    from pathlib import Path
from six import iteritems, text_type


class Configuration(dict):
    """Manage Spark and other configurations.
    """

    default_filename = Path(__file__).parent.parent / 'data' / 'default.properties'
    """:property: filename storing the defaults"""

    def __init__(self, outdir, filename=None, overrides=None):
        """Provide a configuaration dictionary

        Optionally provided `kwargs` will override information loaded from
        the file provided by `filename`.

        :param outdir: output directory to save Spark event log and
                       warehouse information if not provided
        :param filename: alternative file to load
        """
        super(Configuration, self).__init__()
        outdir = Path(outdir) / "_spark"
        self.__filename = Path(filename or self.default_filename)
        with self.__filename.open() as fd:
            for k, v in jprops.iter_properties(fd):
                self[k] = v
        if overrides:
            fd = io.StringIO("\n".join([text_type(s) for s in overrides]))
            for k, v in jprops.iter_properties(fd):
                self[k] = v

        self["spark.driver.extraJavaOptions"] = \
            "-Dderby.system.home={} {}".format(
                outdir.resolve(),
                self.get("spark.driver.extraJavaOptions", ""))
        self.setdefault("spark.eventLog.dir",
                        str(outdir.resolve() / "eventlog"))
        self.setdefault("spark.sql.warehouse.dir",
                        str(outdir.resolve() / "warehouse"))
        for k in ["spark.eventLog.dir", "spark.sql.warehouse.dir"]:
            Path(self[k]).mkdir(parents=True, exist_ok=True)

    def __call__(self, prefix):
        """Yield all key, value pairs that match the prefix
        """
        prefix = prefix.split('.')
        for k, v in iteritems(self):
            path = k.split('.')[:len(prefix)]
            if path == prefix:
                yield k, v

    def dump(self):
        """Dump the default configuration to the terminal
        """
        def path2str(s):
            if isinstance(s, Path):
                return str(s)
            return s
        seen = set()
        with open(self.__filename) as fd:
            for k, v in jprops.iter_properties(fd, comments=True):
                if k is jprops.COMMENT:
                    print('#' + v)
                    continue
                jprops.write_property(sys.stdout, k, self[k])
                seen.add(k)
        print('\n# below: non-default, generated, and user-overridden parameters')
        for k in sorted(set(self.keys()) - seen):
            jprops.write_property(sys.stdout, k, path2str(self[k]))
