from __future__ import print_function
import io
import jprops
import os
import sys
from six import iteritems, text_type


class Configuration(dict):
    """Manage Spark and other configurations.
    """

    default_filename = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'default.properties')
    """:property: filename storing the defaults"""
    jar_filename = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'spykfunc_udfs.jar')
    """:property: path to a jar needed for operations"""

    def __init__(self, outdir, filename=None, overrides=None):
        """Provide a configuaration dictionary

        Optionally provided `kwargs` will override information loaded from
        the file provided by `filename`.

        :param outdir: output directory to save Spark event log and
                       warehouse information if not provided
        :param filename: alternative file to load
        """
        super(Configuration, self).__init__()
        self.__filename = filename or self.default_filename
        with open(self.__filename) as fd:
            for k, v in jprops.iter_properties(fd):
                self[k] = v
        if overrides:
            fd = io.StringIO("\n".join([text_type(s) for s in overrides]))
            for k, v in jprops.iter_properties(fd):
                self[k] = v
        self["spark.jars"] = self.jar_filename
        self.setdefault("spark.driver.extraJavaOptions",
                        "-Dderby.system.home=" + os.path.abspath(outdir))
        self.setdefault("spark.eventLog.dir", os.path.abspath(os.path.join(outdir, "spark_eventlog")))
        self.setdefault("spark.sql.warehouse.dir", os.path.abspath(os.path.join(outdir, "spark_warehouse")))
        for k in ["spark.eventLog.dir", "spark.sql.warehouse.dir"]:
            if not os.path.exists(self[k]):
                os.makedirs(self[k])

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
        seen = set()
        with open(self.__filename) as fd:
            for k, v in jprops.iter_properties(fd, comments=True):
                if k is jprops.COMMENT:
                    print('#' + v)
                    continue
                jprops.write_property(sys.stdout, k, self[k])
                seen.add(k)
        for k in sorted(set(self.keys()) - seen):
            jprops.write_property(sys.stdout, k, self[k])
