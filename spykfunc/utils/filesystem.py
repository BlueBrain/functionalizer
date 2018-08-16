"""Auxiliary module to manage paths

This module ensures compatibility when running with/without a Hadoop
cluster, since the underlying Spark API behaves differently in the presence
of a Hadoop cluster.
"""
from datetime import datetime
import glob
import lxml.etree
import logging
import os
try:
    from pathlib2 import Path
except Exception:
    from pathlib import Path

import hdfs
import hdfs.util


L = logging.getLogger(__name__)


class AutoClient(hdfs.InsecureClient):
    """Simple client that attempts to parse the Hadoop configuration.
    """
    def __init__(self):
        super(AutoClient, self).__init__(self._find_host())
        self.status('/')  # attempt to access file system to verify connection

    @staticmethod
    def _find_host():
        try:
            tree = lxml.etree.parse(str(AutoClient._find_config()))
            return tree.xpath('//property[name="dfs.namenode.http-address"]/value').pop().text
        except IndexError:
            return 'localhost:50070'

    @staticmethod
    def _find_config():
        """Determine Hadoop configuration location.
        """
        cdir = os.environ.get("HADOOP_CONF_DIR", None)
        home = os.environ.get("HADOOP_HOME", None)
        if cdir:
            return Path(cdir) / 'hdfs-site.xml'
        elif home:
            return Path(home) / 'conf' / 'hdfs-site.xml'
        else:
            raise RuntimeError("cannot determine HADOOP setup")


class AttemptedInstance(object):
    """Class to automatically instantiate objects

    Only create the underlying object when requested, pass through all
    attribute requests.
    """
    def __init__(self, cls):
        self.__cls = cls
        self.__obj = None

    def __bool__(self):
        self.__ensure_instance()
        return (self.__obj is not False) and (self.__obj is not None)

    def __getattr__(self, attr):
        self.__ensure_instance()
        return getattr(self.__obj, attr)

    def __ensure_instance(self):
        if self.__obj is not None:
            return
        try:
            self.__obj = self.__cls()
        except Exception as e:
            L.warn("No HDFS cluster found, deactivating support")
            self.__obj = False


__client = AttemptedInstance(AutoClient)


def autosense_hdfs(local_p, hdfs_p):
    """Pick a local or HDFS path based on HDFS cluster presence.

    The HDFS path may include `{date}`, which will be formatted with the
    present date.

    :param local_p: a local path
    :param hdfs_p: a path to use with HDFS
    """
    if __client:
        return hdfs_p.format(date=datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    return local_p


def adjust_for_spark(p, local=None):
    """Adjust a file path to be used with both HDFS and local filesystems

    Add a "file://" prefix if the underlying directory exists and a HDFS
    setup is detected, and remove optional "hdfs://" prefixes.

    :param p: file path to adjust
    :param local: enforce usage of local filesystem when paths are ambiguous
    """
    p = str(p)
    if p.startswith("hdfs://"):
        if not __client:
            msg = "cannot use a fully qualified path '{}' without a running Hadoop cluster!".format(p)
            raise ValueError(msg)
        return p.replace("hdfs://", "")
    elif p.startswith("file://"):
        if not __client:
            return p.replace("file://", "")
        return p
    elif __client:
        if local or len(glob.glob(p)) > 0:
            return "file://" + os.path.abspath(p)
    return p


def exists(p):
    """Check if a path exists.
    """
    if p.startswith("file://") or not __client:
        return os.path.exists(p.replace("file://", ""))
    try:
        __client.status(p)
        return True
    except hdfs.util.HdfsError as err:
        if err.exception == 'FileNotFoundException':
            return False
        raise


def isdir(p):
    """Check if a path exists and is a directory.
    """
    if p.startswith("file://") or not __client:
        return os.path.isdir(p.replace("file://", ""))
    try:
        s = __client.status(p)
        return s['type'] == 'DIRECTORY'
    except hdfs.util.HdfsError as err:
        if err.exception == 'FileNotFoundException':
            return False
        raise
