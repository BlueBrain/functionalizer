from __future__ import absolute_import
""" A module of general purpose spark helper functions
"""
from contextlib import contextmanager
import sparkmanager as sm
from pyspark.sql import functions as F


@contextmanager
def number_shuffle_partitions(np):
    previous_np = int(sm.conf.get("spark.sql.shuffle.partitions"))
    sm.conf.set("spark.sql.shuffle.partitions", np)
    yield
    sm.conf.set("spark.sql.shuffle.partitions", previous_np)


def cache_broadcast_single_part(df, parallelism=1):
    """Caches, coalesce(1) and broadcasts df
    Requires immediate evaluation, otherwise spark-2.2.x doesnt optimize

    :param df: The dataframe to be evaluated and broadcasted
    :param parallelism: The number of tasks to use for evaluation. Default: 1
    """
    df = df.coalesce(parallelism).cache()
    df.count()
    if parallelism > 1:
        df = df.coalesce(1)
    return F.broadcast(df)


class BroadcastValue(object):
    """Transparent access to broadcasted indexable vars
    """
    def __init__(self, value):
        self._bcast_value = sm.sc.broadcast(value)

    def __getitem__(self, name):
        return self._bcast_value.value[name]
