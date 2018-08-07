""" A lightweight morphology reader for functionalities required by fzer
"""

import h5py
import numpy
from lazy_property import LazyProperty
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
        self._morph_h5 = h5py.File(path, 'r')
        self._segment_distances = None

    @property
    def offsets(self):
        return self._morph_h5["/structure"][:, _H5V1_Fields.START_OFFSET]

    @property
    def cell_types(self):
        """the cell types of branch indices
        """
        return self._morph_h5["/structure"][:, _H5V1_Fields.TYPE]

    @property
    def parents(self):
        """the parents of branch indices
        """
        return self._morph_h5["/structure"][:, _H5V1_Fields.PARENT]

    def expand(self, b):
        """Expand an array to match offsets

        Single elements will be copied such that offsets loaded from H5 are
        obeyed, and multiplicity is equal to the difference in offsets.

        :param b: the array
        """
        assert self.xs is not None
        vals = numpy.zeros_like(self.xs, dtype=b.dtype)
        for i in range(len(self.offsets) - 1):
            vals[self.offsets[i]:self.offsets[i + 1]] = b[i]
        vals[self.offsets[-1]:-1] = b[-1]
        return vals

    @LazyProperty
    def __load_coordinates(self):
        """Load coordinates and calculate distances
        """
        self.xs = self._morph_h5["/points"][:, 0]
        self.ys = self._morph_h5["/points"][:, 1]
        self.zs = self._morph_h5["/points"][:, 2]
        self.ds = self._morph_h5["/points"][:, 3] / 0.5

        soma = self.expand(self.cell_types) == NEURON_SECTION_TYPE.soma
        n = numpy.count_nonzero(soma)
        soma_x = self.xs[soma].sum() / n
        soma_y = self.ys[soma].sum() / n
        soma_z = self.zs[soma].sum() / n

        self.__r = numpy.sqrt(numpy.power(self.xs[soma] - soma_x, 2) +
                              numpy.power(self.ys[soma] - soma_y, 2) +
                              numpy.power(self.zs[soma] - soma_z, 2)).max()

        self.xs[soma] = soma_x
        self.ys[soma] = soma_y
        self.zs[soma] = soma_z
        self.ds[soma] = self.__r

        self.__segments = numpy.sqrt(numpy.power(self.xs - numpy.roll(self.xs, -1), 2) +
                                     numpy.power(self.ys - numpy.roll(self.ys, -1), 2) +
                                     numpy.power(self.zs - numpy.roll(self.zs, -1), 2))

        # Fix soma segments and the last one
        self.__segments[soma] = 0
        self.__segments[-1] = 0

        return True

    @LazyProperty
    def branch_lengths(self):
        """Return the branch lengths
        """
        vals = numpy.zeros_like(self.cell_types, dtype=float)
        for i in range(self.offsets.size):
            vals[i] = self.segment_lengths(i)[:-1].sum()
        vals[self.cell_types == NEURON_SECTION_TYPE.soma] = 0
        return vals

    def segment_lengths(self, section):
        """Get the segment lengths of a section

        :param int section: index of the section to get the segment lengths
                            for
        """
        assert self.__load_coordinates
        if section + 1 < self.offsets.size:
            upper = self.offsets[section + 1]
        else:
            upper = self.__segments.size
        return self.__segments[self.offsets[section]:upper]

    @property
    def soma_radius(self):
        """Returns the radius of the soma
        """
        assert self.__load_coordinates
        return self.__r

    def __section_lengths(self, section):
        """Calculates distance from soma assuming complete sections

        :param int section: a partial section up to which the distance
                            should be calculated (i.e. this section is not
                            included in the length)
        """
        soma_distance = 0
        full_section = self.parents[section]
        while full_section != -1:
            parent = self.parents[full_section]
            if parent != -1:
                soma_distance += self.branch_lengths[full_section]
            full_section = parent
        return soma_distance

    def __partial_section_length(self, section, segment):
        """Return the partial sum of segment lengths in a section

        :param int section: the section index to integrate
        :param int segment: the first segment index that is not included in the sum
        """
        return self.segment_lengths(section)[:segment].sum()

    def path(self, section):
        """Get a list of all sections up to the soma

        :param int section: the section to start with, included in the result
        """
        res = [section]
        parent = self.parents[section]
        while parent != -1:
            res.append(parent)
            parent = self.parents[parent]
        return res

    def distance_of(self, section, segment):
        """Calculate the path distance of a point located at `section` and
        `segment` indices from the soma.

        :param int section: section index
        :param int segment: segment index
        :returns: the distance
        :rtype: float
        """
        return self.__section_lengths(section) + self.__partial_section_length(section, segment)

    def __section_type_index(self, section_type):
        return numpy.flatnonzero(self.cell_types == section_type)

    @LazyProperty
    def first_axon_section(self):
        """First axon section of the neuron
        """
        return int(self.__section_type_index(NEURON_SECTION_TYPE.axon)[0])


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
        if isinstance(morpho, int):
            morpho = self._mapping[morpho]
        item = self._db.get(morpho)
        if not item:
            path = str(self.db_path / (morpho + ".h5"))
            item = self._db[morpho] = Morphology(path)
        return item
