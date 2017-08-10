from __future__ import print_function
import sys
from lazy_property import LazyProperty
import numpy as np
from abc import ABCMeta, abstractproperty
from future.utils import with_metaclass
import itertools
import os.path
from ._caching import CachedDataset, _DataSet
from ..utils import show_wait_message, get_logger

logger = get_logger(__name__)

class NEURON_STATS_F:
    neuronID = 0
    touchesCount = 1
    binaryOffset = 2


class TouchInfo_Interface(with_metaclass(ABCMeta, object)):
    @abstractproperty
    def header(self):
        pass

    @abstractproperty
    def neuron_stats(self):
        pass

    @abstractproperty
    def touch_count(self):
        pass

    @abstractproperty
    def touches(self):
        pass


class TouchInfo(TouchInfo_Interface):
    def __new__(cls, files):
        if isinstance(files, str):
            return _TouchInfo(files)
        else:
            return object.__new__(TouchInfo)

    def __init__(self, files):
        self._touch_infos = [_TouchInfo(f) for f in files]

    @LazyProperty
    def header(self):
        glob_header = self._touch_infos[0].header.copy()
        # Header is the same for every node
        # glob_header.numberOfNeurons = sum(ti.header.numberOfNeurons
        #                                  for ti in self._touch_infos)
        return glob_header

    @property
    def neuron_stats(self):
        return itertools.chain(*[info.neuron_stats for info in self._touch_infos])

    @property
    def touch_count(self):
        return sum(info.touch_count for info in self._touch_infos)

    @property
    def touches(self):
        return itertools.chain(*[info.touches for info in self._touch_infos])


class _TouchInfo(TouchInfo_Interface):
    """
    Reader of Binary Neuron-Touches info
    """
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

    _neuron_touches_dtype_reversed = _neuron_touches_dtype.newbyteorder()

    def __init__(self, neuron_touches_file):
        # type: (str) -> None
        self._neuron_file = neuron_touches_file
        _last_dot = neuron_touches_file.rfind('.')
        self.touches_file = neuron_touches_file[:_last_dot] + 'Data' + neuron_touches_file[_last_dot:]
        self._header = None
        self._byte_swap = None

    def _read_header(self, f_handler):
        try:
            header = np.rec.fromfile(f_handler, dtype=_TouchInfo._header_dtype, aligned=True, shape=1)[0]
        except:
            logger.fatal("Could not read header record from touches")
            return

        self._byte_swap = not np.equal(header.architectureIdentifier, np.double(1.001))
        if self._byte_swap:
            header.numberOfNeurons = header.numberOfNeurons.byteswap()
            self._neuron_touches_dtype = _TouchInfo._neuron_touches_dtype_reversed
        # cache
        self._header = header
        return header

    @property
    def _neuron_stats(self):
        with open(self._neuron_file) as neuron_f:
            _ = self._read_header(neuron_f)
            with show_wait_message("Loading " + os.path.basename(self._neuron_file)):
                info = np.fromfile(neuron_f, dtype=self._neuron_touches_dtype)
        return info

    @property
    def header(self):
        return self._header or self._read_header(open(self._neuron_file))

    @property
    def neuron_stats(self):
        return self._neuron_stats

    @LazyProperty
    def touch_count(self):
        return int(self.neuron_stats['touchesCount'].sum())

    @LazyProperty
    def touches(self):
        print("WARNING: TouchInfo.touches is lazily evaluated, returning an iterator.\n"
              "         Please select a small region and/or avoid converting to a full array")
        _ = self.header  # Init endianness if needed
        return CachedDataset(_Touches(self))


# Raw accecs of touches
# Do not use directly, only implemented to interface to CachedDataset
class _Touches(_DataSet):
    """
    Reader for Binary touches
    """
    _touches_dtype = np.dtype([
        ("pre_ids", np.int32, 3),
        ("post_ids", np.int32, 3),
        ("branch_order", np.int32),
        ("distances", np.float32, 3),
    ], align=True)
    _touches_dtype_reversed = _touches_dtype.newbyteorder()

    _SIZEOF_TOUCH = _touches_dtype.itemsize

    def __init__(self, touch_info):
        # Some infos we read from the neuron_stats struct, e.g. length
        self._neuron_stats = touch_info.neuron_stats
        self._length = touch_info.touch_count
        self._touch_f = open(touch_info.touches_file)
        self._touches_dtype = _Touches._touches_dtype if not touch_info._byte_swap\
            else _Touches._touches_dtype_reversed

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
        return np.fromfile(self._touch_f, dtype=self._touches_dtype, count=count)

    def __setitem__(self, key, value):
        raise TypeError("Read-only structure")

    @property
    def dtype(self):
        return self._touches_dtype

    @property
    def shape(self):
        return (self._length,)  # tuple

    def __len__(self):
        return self._length
