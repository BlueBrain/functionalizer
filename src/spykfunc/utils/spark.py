"""A module of general purpose spark helper functions."""

from contextlib import contextmanager

import sparkmanager as sm
from pyspark.sql import functions as F

from . import get_logger

logger = get_logger(__name__)


@contextmanager
def number_shuffle_partitions(np):
    """Temporarily change the number of shuffle partitions.

    Note that for this to have any effect, the lazy evaluation of Spark
    needs to be triggered within the context this function is used with,
    otherwise calculations will use the restored value for the number of
    shuffle partitions.
    """
    previous_np = int(sm.conf.get("spark.sql.shuffle.partitions"))
    logger.debug("Temporarily using %d shuffle partitions", np)
    sm.conf.set("spark.sql.shuffle.partitions", np)
    yield
    logger.debug("Restoring usage of %d shuffle partitions", previous_np)
    sm.conf.set("spark.sql.shuffle.partitions", previous_np)


def cache_broadcast_single_part(df, parallelism=1):
    """Caches, coalesce(1) and broadcasts `df`.

    Requires immediate evaluation, otherwise spark-2.2.x doesnt optimize

    :param df: The dataframe to be evaluated and broadcasted
    :param parallelism: The number of tasks to use for evaluation. Default: 1
    """
    df = df.coalesce(parallelism).cache()
    df.count()
    if parallelism > 1:
        df = df.coalesce(1)
    return F.broadcast(df)
