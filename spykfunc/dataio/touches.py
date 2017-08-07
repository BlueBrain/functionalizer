from lazy_property import LazyProperty
import numpy as np
from ._caching import CachedDataset, _DataSet
import logging


class NEURON_STATS_F:
    neuronID = 0
    touchesCount = 1
    binaryOffset = 2


# Data types
_header_dtype = np.dtype([
    ("architectureIdentifier", np.double),
    ("numberOfNeurons", np.longlong),
    ("gitVersion", "S16")
], align=True)

_neuron_touches_dtype = np.dtype([
    ("neuronID", np.int32),
    ("touchesCount", np.uint32),
    ("binaryOffset", np.longlong)
], align=True)

_touches_dtype = np.dtype([
    ("pre_ids", np.int32, 3),
    ("post_ids", np.int32, 3),
    ("branch_order", np.int32),
    ("distances", np.float32, 3),
], align=True)


class TouchInfo(object):
    def __init__(self, neuron_touches_file):
        # type: (str) -> None
        self._neuron_file = neuron_touches_file
        _last_dot = neuron_touches_file.rfind('.')
        self.touches_file = neuron_touches_file[:_last_dot] + 'Data' + neuron_touches_file[_last_dot:]

    @LazyProperty
    def _neuron_header_stats(self):
        neuron_f = open(self._neuron_file)
        try:
            header = np.rec.fromfile(neuron_f, dtype=_header_dtype, aligned=True, shape=1)[0]
        except:
            logging.fatal("Could not read header record from touches")
            return
        self._byte_swap = not np.equal(header.architectureIdentifier, np.double(1.001))
        return header, np.fromfile(neuron_f, dtype=_neuron_touches_dtype)

    @property
    def header(self):
        return self._neuron_header_stats[0]

    @property
    def neuron_stats(self):
        return self._neuron_header_stats[1]

    @LazyProperty
    def touch_count(self):
        return sum(stats[1] for stats in self.neuron_stats)

    @LazyProperty
    def touches(self):
        print("WARNING: TouchInfo.touches is lazily evaluated, returning an iterator."
              "         Please select a small region and/or avoid converting to a full array")
        return CachedDataset(_Touches(self))


# Raw accecs of touches
# Do not use directly, only implemented to interface to CachedDataset
class _Touches(_DataSet):
    _SIZEOF_TOUCH = _touches_dtype.itemsize

    def __init__(self, touch_info):
        # Some infos we read from the neuron_stats struct, e.g. length
        self._neuron_stats = touch_info.neuron_stats
        self._length = touch_info.touch_count
        self._touch_f = open(touch_info.touches_file)

    def __del__(self):
        self._touch_f.close()

    def __getitem__(self, index):
        # type: (slice) -> np.ndarray
        if type(index) is slice:
            true_idx = index.indices(self._length)
            offset = true_idx[0]
            count = true_idx[1] - true_idx[0]
        else:
            offset = index
            count = 1
        self._touch_f.seek(offset * self._SIZEOF_TOUCH)
        return np.fromfile(self._touch_f, dtype=_touches_dtype, count=count)

    def __setitem__(self, key, value):
        raise TypeError("Read-only structure")

    @property
    def dtype(self):
        return _touches_dtype

    @property
    def shape(self):
        return (self._length,)  # tuple

    def __len__(self):
        return self._length
