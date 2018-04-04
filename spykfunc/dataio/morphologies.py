""" A lightweight morphology reader for functionalities required by fzer
"""

from os import path as osp
import h5py
import numpy
from lazy_property import LazyProperty


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
    def __init__(self, path):
        self._morph_h5 = h5py.File(path, 'r')

    @property
    def offsets(self):
        return self._morph_h5["/structure"][:, _H5V1_Fields.START_OFFSET]

    @property
    def cell_types(self):
        return self._morph_h5["/structure"][:, _H5V1_Fields.TYPE]

    @property
    def parents(self):
        return self._morph_h5["/structure"][:, _H5V1_Fields.PARENT]

    def get_section_type_index(self, section_type):
        return numpy.flatnonzero(self.cell_types == section_type)

    @LazyProperty
    def first_axon_section(self):
        return int(self.get_section_type_index(NEURON_SECTION_TYPE.axon)[0])


class MorphologyDB(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self._db = {}

    def __getitem__(self, morpho_name):
        item = self._db.get(morpho_name)
        if not item:
            path = osp.join(self.db_path, morpho_name) + ".h5"
            item = self._db[morpho_name] = Morphology(path)
        return item
