""" A lightweight morphology reader for functionalities required by fzer
"""

import h5py
import numpy
from pathlib import Path


class _H5V1_Fields:
    START_OFFSET = 0
    TYPE = 1
    PARENT = 2


class NEURON_SECTION_TYPE:
    soma = 1
    axon = 2
    dentrite_basal = 3
    dentrite_apical = 4
    unknown = 0


class GLIA_SECTION_TYPE:
    soma = 1
    glia_process = 2
    glia_endfoot = 3
    unknown = 0


class Morphology(object):
    """Access to neuron morphology properties

    :param path: the filename associated with the morphology
    """
    def __init__(self, path):
        self._path = path

        structure = self._h5get('/structure')

        self.cell_types = structure[:, _H5V1_Fields.TYPE]
        self.offsets = structure[:, _H5V1_Fields.START_OFFSET]
        self.parents = structure[:, _H5V1_Fields.PARENT]

        self.first_axon_section = int(
            numpy.flatnonzero(self.cell_types == NEURON_SECTION_TYPE.axon)[0]
        )

        self.__branch_lengths = None
        self.__paths = None
        self.__r = None
        self.__section_lengths = None
        self.__segment_distances = None

    def _h5get(self, name):
        with h5py.File(self._path, 'r') as f:
            ds = f[name]
            offset = ds.id.get_offset()
            assert ds.chunks is None
            assert ds.compression is None
            assert offset > 0
            dtype = ds.dtype
            shape = ds.shape
        return numpy.memmap(self._path, mode='r', shape=shape, offset=offset, dtype=dtype).copy()

    def expand(self, b, dim):
        """Expand an array to match offsets

        Single elements will be copied such that offsets loaded from H5 are
        obeyed, and multiplicity is equal to the difference in offsets.

        :param b: the array
        :param dim: the extend
        """
        vals = numpy.zeros(shape=(dim,), dtype=b.dtype)
        for i in range(len(self.offsets) - 1):
            vals[self.offsets[i]:self.offsets[i + 1]] = b[i]
        vals[self.offsets[-1]:-1] = b[-1]
        return vals

    def __load_coordinates(self, radius_only=False):
        """Load coordinates and calculate distances
        """
        if radius_only and self.__r:
            return self.__r
        elif self.__branch_lengths is not None:
            return self.__r
        points = self._h5get('/points')
        xs = points[:, 0]
        ys = points[:, 1]
        zs = points[:, 2]

        soma = self.expand(self.cell_types, points.shape[0]) == NEURON_SECTION_TYPE.soma
        n = numpy.count_nonzero(soma)
        soma_x = xs[soma].sum() / n
        soma_y = ys[soma].sum() / n
        soma_z = zs[soma].sum() / n

        self.__r = numpy.sqrt(numpy.power(xs[soma] - soma_x, 2) +
                              numpy.power(ys[soma] - soma_y, 2) +
                              numpy.power(zs[soma] - soma_z, 2)).max()
        if radius_only:
            return self.__r

        xs[soma] = soma_x
        ys[soma] = soma_y
        zs[soma] = soma_z

        segments = numpy.sqrt(numpy.power(xs - numpy.roll(xs, -1), 2) +
                              numpy.power(ys - numpy.roll(ys, -1), 2) +
                              numpy.power(zs - numpy.roll(zs, -1), 2))
        # Fix soma segments and the last one
        segments[soma] = 0
        segments[-1] = 0

        n = self.offsets.size

        self.__branch_lengths = numpy.zeros_like(self.offsets, dtype=float)
        self.__paths = numpy.zeros(shape=(n, n), dtype=int)
        self.__section_lengths = numpy.full(shape=(n,), fill_value=-1, dtype=float)
        self.__segment_distances = numpy.zeros_like(segments, dtype=float)
        for sec in range(self.offsets.size):
            lower = self.offsets[sec]
            if sec + 1 < self.offsets.size:
                upper = self.offsets[sec + 1]
            else:
                upper = segments.size
            self.__segment_distances[lower:upper] = numpy.cumsum(segments[lower:upper])
            if upper >= 2 and self.cell_types[sec] != NEURON_SECTION_TYPE.soma:
                self.__branch_lengths[sec] = self.__segment_distances[upper - 2]
        return self.__r

    @property
    def branch_lengths(self):
        """:property: returns the branch lengths for all sections
        """
        self.__load_coordinates()
        return self.__branch_lengths

    def segment_distances(self, section, segment):
        """Get the cumulative segment lengths of a section

        :param int section: index of the section to get the segment lengths
                            for
        """
        self.__load_coordinates()
        lower = self.offsets[section]
        if section + 1 < self.offsets.size:
            upper = self.offsets[section + 1]
        else:
            upper = self.__segment_distances.size
        segment += upper if segment < 0 else lower
        return self.__segment_distances[segment]

    def soma_radius(self, cache=False):
        """Returns the radius of the soma
        """
        return self.__load_coordinates(radius_only=not cache)

    def _section_lengths(self, section):
        """Calculates distance from soma assuming complete sections

        :param int section: a partial section up to which the distance
                            should be calculated (i.e. this section is not
                            included in the length)
        """
        self.__load_coordinates()
        if self.__section_lengths[section] < 0.:
            path = self.path(section)
            size = path[0]
            self.__paths[section, :] = path
            # the first value in the
            self.__section_lengths[section] = self.branch_lengths[path[2:size]].sum()
        soma_distance = self.__section_lengths[section]
        size = self.__paths[section, 0]
        path = self.__paths[section, 1:size]
        return path, soma_distance

    def path(self, section):
        """Get a list of all sections up to the soma

        :param int section: the section to start with, included in the result if not the soma
        :returns: a numpy array containing the path starting with index 1,
                  and the size of the path
        """
        res = numpy.zeros_like(self.parents)
        idx = 1
        while section != -1:
            res[idx] = section
            section = self.parents[section]
            idx += 1
        res[0] = idx
        return res

    def path_and_distance_of(self, section, segment):
        """Calculate the path distance of a point located at `section` and
        `segment` indices from the soma.

        :param int section: section index
        :param int segment: segment index
        :returns: the distance
        """
        path, dist = self._section_lengths(section)
        if segment > 0:
            dist += self.segment_distances(section, segment - 1)
        return path, dist

    def distance_of(self, section, segment):
        """Calculate the path distance of a point located at `section` and
        `segment` indices from the soma.

        :param int section: section index
        :param int segment: segment index
        :returns: the distance
        :rtype: float
        """
        return self.path_and_distance_of(section, segment)[1]


class MorphologyDB(object):
    """Database wrapper to handle morphology mapping

    :param str db_path: directory that contains the morphologies as .h5
    :param list(str) mapping: a list of morphologies to associate with the indices
    """
    def __init__(self, db_path, mapping):
        self.db_path = Path(db_path)
        self._db = {}
        self._mapping = mapping

    def __getitem__(self, morpho):
        if not isinstance(morpho, str):
            morpho = self._mapping[morpho]
        path = str(self.db_path / (morpho + ".h5"))
        return Morphology(path)
        item = self._db.get(morpho)
        if not item:
            item = self._db[morpho] = Morphology(path)
        return item
