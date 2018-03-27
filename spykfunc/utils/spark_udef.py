from pyspark.accumulators import AccumulatorParam
from future.utils import iteritems
from pyspark.sql import functions as F

__all__ = ["DictAccum", "ListAccum", "wrap_java_udf"]


class DictAccum(AccumulatorParam):
    """ A Spark accumulator to aggregate dictionaries
    """

    def zero(self, initialValue):
        return {}

    def addInPlace(self, a, b):
        for key, item in iteritems(b):
            if key in a:
                a[key].extend(item)
            else:
                a[key] = item
        return a


class ListAccum(AccumulatorParam):
    """ A Spark accumulator to aggregate lists
    """

    def zero(self, initialValue):
        return list(initialValue)

    def addInPlace(self, a, b):
        a += b
        return a


def wrap_java_udf(sc, java_f):
    """ Wraps a java function to be usable as a Python UDF

    :param sc: Spark context
    :param java_f: The java function
    :return: A Python UDF
    """
    return lambda col: F.Column(java_f(F._to_seq(sc, [col], F._to_java_column)))
