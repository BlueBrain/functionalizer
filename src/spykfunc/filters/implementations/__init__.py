"""Reference implementations of filters."""

from typing import List
import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame


def add_random_column(df: DataFrame, name: str, seed: int, key: int) -> DataFrame:
    """Add a random column to a dataframe.

    The dataframe is required to contain a column `synapse_id` that is used
    while seeding the random number generator.

    Args:
        df: The dataframe to augment
        name: Name for the random column
        seed: Used to seed the RNG
        key: Also used to seed the RNG
    Returns:
        The dataframe with a random column
    """

    def __generate(data):
        import spykfunc.filters.udfs as fcts

        for df in data:
            df[name] = fcts.uniform(seed, key, df["synapse_id"])
            yield df

    df = df.withColumn(name, F.lit(0.0))
    return df.mapInPandas(__generate, df.schema)


def add_bin_column(df: DataFrame, name: str, boundaries: List[float], key: str) -> DataFrame:
    """Add a bin column for `key` based on `boundaries`.

    Args:
        df: The dataframe to augment
        name: The name for the column containing the bin
        boundaries: The bin boundaries (one more than the number of bins)
        key: The column to bin
    Returns:
        A dataframe with an additional column
    """
    bins = np.asarray(boundaries, dtype=np.single)

    def __generate(data):
        import spykfunc.filters.udfs as fcts

        for df in data:
            df[name] = fcts.get_bins(df[key], bins)
            yield df

    df = df.withColumn(name, F.lit(-1))
    return df.mapInPandas(__generate, df.schema)
