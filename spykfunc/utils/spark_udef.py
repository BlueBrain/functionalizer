from pyspark.accumulators import AccumulatorParam
from future.utils import iteritems


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
