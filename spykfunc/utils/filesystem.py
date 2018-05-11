"""Auxiliary module to manage paths

This module ensures compatibility when running with/without a Hadoop
cluster, since the underlying Spark API behaves differently in the presence
of a Hadoop cluster.
"""
import glob
import os
import snakebite.client


__client = None
if "HADOOP_HOME" in os.environ:
    __client = snakebite.client.AutoConfigClient()


def adjust_for_spark(p):
    """Adjust a path: add a "file://" prefix if the underlying directory exists and 
    """
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
        if len(glob.glob(p)) > 0:
            return "file://" + os.path.abspath(p)
    return p


def exists(p):
    """Check if a path exists.
    """
    if p.startswith("file://") or not __client:
        return os.path.exists(p.replace("file://", ""))
    return __client.test(p, exists=True)


def isdir(p):
    """Check if a path exists and is a directory.
    """
    if p.startswith("file://") or not __client:
        return os.path.isdir(p.replace("file://", ""))
    return __client.test(p, directory=True, exists=True)