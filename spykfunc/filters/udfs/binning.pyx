cimport numpy as np
import numpy as np

# *************************
# Generic binning functions
# *************************

cpdef np.ndarray[int] get_bins(np.ndarray[float] target,
                               np.ndarray[float] boundaries):
    """Assign bins according to boundaries using target

    Bin values range from -1 for the underflow bin to `boundaries.size - 1`
    for the overflow bin.

    Args:
        target: The values to use for the binning
        boundaries: The bin delimitations, including minimum and maximum values.
    Returns:
        The bins that the values of target fall within
    """
    cdef long i
    cdef long j
    cdef np.ndarray[int] bins = np.full_like(target, -1, dtype=np.int32)
    for i in range(target.size):
        for j in reversed(range(boundaries.size)):
            if target[i] >= boundaries[j]:
                bins[i] = j
                break
    return bins
