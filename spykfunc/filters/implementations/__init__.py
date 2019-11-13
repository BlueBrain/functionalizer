"""Reference implementations of filters
"""
from __future__ import annotations

import numpy
import pandas

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spykfunc.filters.udfs import get_bins, uniform
from spykfunc.recipe import Attribute, GenericProperty


class Seeds(GenericProperty):
    """Container to store seeds
    """

    attributes = [
        Attribute(name="recipeSeed", kind=int, default=0),
        Attribute(name="columnSeed", kind=int, default=0),
        Attribute(name="synapseSeed", kind=int, default=0),
    ]

    singleton = True
    required = False


def add_random_column(df, name, seed, key, derivative):
    """Add a random column to a dataframe

    Args:
        df: The dataframe to augment
        name: Name for the random column
        seed: The seed to use for the RNG
        key: First key to derive the RNG with
        derivative: Column for second derivative of the RNG
    Returns:
        The dataframe with a random column
    """
    @F.pandas_udf('float')
    def _fixed_rand(col):
        return pandas.Series(uniform(seed, key, col.values))
    return df.withColumn(name, _fixed_rand(derivative.cast(T.LongType())))


def add_bin_column(df, name, boundaries, key):
    """Add a bin column for `key` based on `boundaries`

    Args:
        df: The dataframe to augment
        name: The name for the column containing the bin
        boundaries: The bin boundaries (one more than the number of bins)
        key: The column to bin
    Returns:
        A dataframe with an additional column
    """
    bins = numpy.asarray(boundaries, dtype=numpy.single)
    @F.pandas_udf('int')
    def _bin(col):
        return pandas.Series(get_bins(col.values, bins))
    return df.withColumn(name, _bin(key.cast(T.FloatType())))
