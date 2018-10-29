cimport numpy as np
import numpy as np

# **************************************************
# Gap-junction support
# **************************************************

ctypedef np.uint8_t uint8

cdef long __is_within(long r,
                         np.ndarray[long] rs,
                         np.ndarray[uint8] unmatched,
                         np.ndarray[short] pre_sec,
                         np.ndarray[short] pre_seg,
                         np.ndarray[short] post_sec,
                         np.ndarray[short] post_seg):
    """Find element `r` in list `rs` with tolerances.

    The first match is removed from the list.  A first pass requiring
    an exact match is performed, followed by a second pass with looser
    requirements.

    :param r: the segments/sections to search for in `rs`
    :param rs: a list segment/section coordinates
    :param unmatched: the status of the segment/section indices in `rs`
    :param {pre,post}_{sec,seg}: touch data
    """
    cdef long i, s
    assert rs.size == unmatched.size
    for i in range(rs.size):
        if not unmatched[i]:
            continue
        s = rs[i]
        if (pre_sec[r] == post_sec[s] and
            pre_sec[s] == post_sec[r] and
            pre_seg[r] == post_seg[s] and
            pre_seg[s] == post_seg[r]):
            unmatched[i] = False
            return s
    for i in range(rs.size):
        if not unmatched[i]:
            continue
        s = rs[i]
        if (pre_sec[r] == post_sec[s] and
            pre_sec[s] == post_sec[r] and
            abs(pre_seg[r] - post_seg[s]) <= 1 and
            abs(pre_seg[s] - post_seg[r]) <= 1):
            unmatched[i] = False
            return s
    return -1

cpdef np.ndarray[uint8] match_dendrites(np.ndarray[int] src,
                                        np.ndarray[int] dst,
                                        np.ndarray[short] pre_sec,
                                        np.ndarray[short] pre_seg,
                                        np.ndarray[int] pre_jct,
                                        np.ndarray[short] post_sec,
                                        np.ndarray[short] post_seg,
                                        np.ndarray[int] post_jct):
    cdef np.ndarray[int, ndim=2] connections = np.stack((src, dst)).T
    cdef np.ndarray[int, ndim=2] uniques = np.unique(connections, axis=0)
    cdef np.ndarray[uint8] accept = np.zeros_like(src, dtype=np.uint8)
    cdef np.ndarray[uint8] unmatched
    cdef np.ndarray[long] idx, rev
    cdef np.ndarray[int] x, y
    cdef long i, o

    for conn in uniques:
        if conn[0] > conn[1]:
            continue

        x = np.full_like(connections[:, 0], conn[0])
        y = np.full_like(connections[:, 1], conn[1])

        idx = np.where(np.logical_and(np.equal(connections[:, 0], x),
                                      np.equal(connections[:, 1], y)))[0]
        rev = np.where(np.logical_and(np.equal(connections[:, 0], y),
                                      np.equal(connections[:, 1], x)))[0]
        unmatched = np.ones_like(rev, dtype=np.uint8)
        for i in idx:
            o = __is_within(i, rev, unmatched,
                               pre_sec, pre_seg,
                               post_sec, post_seg)
            if o >= 0:
                accept[i] = 1
                accept[o] = 1
                pre_sec[i] = post_sec[o]
                pre_seg[i] = post_seg[o]
                post_sec[i] = pre_sec[o]
                post_seg[i] = pre_seg[o]
                post_jct[i] = pre_jct[o]
                post_jct[o] = pre_jct[i]
                continue
    return accept
