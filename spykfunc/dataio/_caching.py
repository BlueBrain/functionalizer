from __future__ import absolute_import
import abc
from functools import reduce
from operator import mul
from itertools import islice

_DEBUG = True


class _DataSet:
    """ Datasets must support numpy-like slice syntax
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __getitem__(self, group_name):
        pass

    @abc.abstractmethod
    def __setitem__(self, key, value):
        pass

    @abc.abstractproperty
    def dtype(self):
        pass

    @abc.abstractproperty
    def shape(self):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass


class CachedDataset(_DataSet):
    _MB = 1024 * 1024

    # -----
    def __init__(self, ds, col_index=None, cache_size=2 * _MB, **kw):
        # type: (_DataSet) -> None
        if isinstance(ds, CachedDataset):
            self.__dict__ = ds.__dict__.copy()  # Get everything shallow copied over
            self._slice_start = kw.get("_slice_start")
            self._slice_stop = kw.get("_slice_stop")
        else:
            self._ds = ds
            self._item_size = ds.dtype.itemsize
            if len(ds.shape) > 1:
                self._item_size *= reduce(mul, ds.shape[1:])
            self._base_cached_lines = cache_size / self._item_size
            self._ds_len = len(ds)
            self._cache_len = 0
            self._cache_offset = 0
            # Slice (virtual region)  - default all
            self._slice_start = 0  # Absolute offset of the selection
            self._slice_stop = self._ds_len  # Absolute end of the selection
            self._col = col_index

    # -----
    def __getitem__(self, item):
        if type(item) is slice:
            slice_len = self._slice_stop - self._slice_start
            actual_indices = item.indices(slice_len)  # Enable negative indexing
            # n_items = actual_indices[1] - actual_indices[0]
            new_start = self._slice_start + actual_indices[0]
            new_stop = self._slice_start + actual_indices[1]
            if new_start > self._ds_len or new_stop > self._ds_len:
                raise IndexError("Range %d-%d invalid" % actual_indices)

            # Be lazy, dont evaluate yet
            if _DEBUG:
                print("Asking region...")
            return CachedDataset(self, _slice_start=new_start, _slice_stop=new_stop)
        else:
            # A single item is cached and evaluated
            actual_index = self._slice_start + item
            self._check_load_buffer(actual_index)
            if _DEBUG:
                print("Asking number")
            return self._cache[actual_index - self._cache_offset]

    # -----
    def __iter__(self):
        # Iterate the whole virtual region, eventually spanning over several buffers
        cur_slice_pos = self._slice_start
        while self._slice_stop > cur_slice_pos:
            self._check_load_buffer(cur_slice_pos)
            buffer_start = cur_slice_pos - self._cache_offset
            buffer_stop = min(self._cache_len, self._slice_stop - self._cache_offset)

            for elem in islice(self._cache, buffer_start, buffer_stop):
                yield elem
            cur_slice_pos += buffer_stop - buffer_start

    # -----
    def _check_load_buffer(self, start_offset):
        end_offset = self._cache_offset + self._cache_len
        if start_offset < self._cache_offset or start_offset >= end_offset:
            # Align to groups of 32 elems - mitigate reverse access
            start_offset = (start_offset // 32) << 5
            end_offset = min(start_offset + self._base_cached_lines, self._ds_len)

            if _DEBUG:
                print("Loading buffer with range %d-%d" % (start_offset, end_offset))
            # fill buffer
            if self._col is not None:
                self._cache = self._ds[start_offset:end_offset, self._col]
            else:
                self._cache = self._ds[start_offset:end_offset]
            self._cache_offset = start_offset
            self._cache_len = end_offset - start_offset

    # -----
    def __len__(self):
        return self._slice_stop - self._slice_start

    def __setitem__(self, key, value):
        pass

    @property
    def attrs(self):
        return self._ds.attrs

    @property
    def dtype(self):
        return self._ds.dtype

    @property
    def shape(self):
        return self._ds.shape

    def require_dataset(self, group):
        return self._ds.require_dataset(group)
