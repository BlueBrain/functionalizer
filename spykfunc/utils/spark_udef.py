from pyspark.accumulators import AccumulatorParam
from future.utils import iteritems
from pyspark.sql import functions as F

__all__ = ["DictAccum", "make_agg_f"]

class DictAccum(AccumulatorParam):
    def zero(self, initialValue):
        return {}

    def addInPlace(self, a, b):
        for key, item in iteritems(b):
            if key in a:
                a[key].extend(item)
            else:
                a[key] = item
        return a


def make_agg_f(sc, java_f):
    return lambda col: F.Column(java_f(F._to_seq(sc, [col], F._to_java_column)))
