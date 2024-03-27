"""A default filter plugin."""

import numpy as np
import pandas as pd

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.utils.spark import cache_broadcast_single_part


class SomaDistanceFilter(DatasetOperation):
    """Filter touches based on distance from soma.

    Removes all touches that are located within the soma.
    """

    def __init__(self, recipe, source, target):
        """Initialize the filter, using the morphology database."""
        super().__init__(recipe, source, target)
        self.__morphos = target.morphologies

    def apply(self, circuit):
        """Remove touches within the soma."""
        soma_radius = self._create_soma_radius_udf()
        radii = (
            circuit.target.df.select("morphology")
            .distinct()
            .withColumn("radius_soma", soma_radius(F.col("morphology")))
            .withColumnRenamed("morphology", "dst_morphology")
        )
        _n_parts = max(radii.rdd.getNumPartitions() // 20, 100)
        radii = cache_broadcast_single_part(radii, parallelism=_n_parts)
        return (
            circuit.df.join(radii, "dst_morphology")
            .where(F.col("distance_soma") >= F.col("radius_soma"))
            .drop("radius_soma")
        )

    def _create_soma_radius_udf(self):
        """Produce a UDF to calculate soma radii."""

        @F.pandas_udf("float")
        def soma_radius(morphos):
            def r(m):
                return self.__morphos.soma_radius(m)

            f = np.vectorize(r)
            return pd.Series(data=f(morphos.values), dtype="float")

        return soma_radius
