"""Small configuration shim module.
"""
from __future__ import print_function
import io
import subprocess
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
    jar_filename = Path(__file__).parent.parent / 'data' / 'spykfunc_udfs.jar'
    """:property: path to a jar needed for operations"""
    lib_directory = Path(__file__).parent.parent / 'data'
    """:property: directory with compiled JNI libraries"""

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

        libs = ":".join(str(p) for p in self.find_libs(self.lib_directory))
        libs = str(self.lib_directory)
        self["spark.jars"] = self.jar_filename
        self["spark.driver.extraLibraryPath"] = libs
        self["spark.executor.extraLibraryPath"] = libs
        self["spark.driver.extraJavaOptions"] = \
            "-Dderby.system.home={} -Djava.library.path={} {}".format(
                outdir.resolve(),
                self.lib_directory.resolve(),
                self.get("spark.driver.extraJavaOptions", ""))
        self["spark.executor.extraJavaOptions"] = \
            "-Djava.library.path={} {}".format(
                self.lib_directory.resolve(),
                self.get("spark.executor.extraJavaOptions", ""))
        self.setdefault("spark.eventLog.dir", outdir.resolve() / "eventlog")
        self.setdefault("spark.sql.warehouse.dir", outdir.resolve() / "warehouse")
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

    @staticmethod
    def find_libs(path):
        """Find all necessary include paths for the libraries in `path`.
        """
        libs = {Path(path)}
        for lib in Path(path).glob("*.so"):
            for line in subprocess.check_output(["ldd", str(lib)]).decode().splitlines():
                if "=>" in line:
                    libs.add(Path(line.split()[2]).parent)
                elif line.startswith("\t/"):
                    libs.add(Path(line.split()[0]).parent)
        return libs

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
