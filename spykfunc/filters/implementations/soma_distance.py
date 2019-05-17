"""A default filter plugin
"""
import numpy
import pandas

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.utils.spark import cache_broadcast_single_part


class SomaDistanceFilter(DatasetOperation):
    """Filter touches based on distance from soma

    Removes all touches that are located within the soma.
    """

    def __init__(self, recipe, morphos, stats):
        self.__morphos = morphos

    def apply(self, circuit):
        """Remove touches within the soma.
        """
        soma_radius = self._create_soma_radius_udf()
        radii = circuit.neurons.select('morphology_i') \
                               .distinct() \
                               .withColumn('radius_soma',
                                           soma_radius(F.col('morphology_i'))) \
                               .withColumnRenamed('morphology_i', 'dst_morphology_i')
        _n_parts = max(radii.rdd.getNumPartitions() // 20, 100)
        radii = cache_broadcast_single_part(radii, parallelism=_n_parts)
        return circuit.df.join(radii, 'dst_morphology_i') \
                         .where(F.col('distance_soma') >= F.col('radius_soma')) \
                         .drop('radius_soma')

    def _create_soma_radius_udf(self):
        """Produce a UDF to calculate soma radii
        """
        @F.pandas_udf('float')
        def soma_radius(morphos):
            def r(idx):
                return self.__morphos[idx].soma_radius()
            f = numpy.vectorize(r)
            return pandas.Series(data=f(morphos.values), dtype='float')
        return soma_radius
