""" A lightweight morphology reader for functionalities required by fzer
"""

import functools
import morphokit
from pathlib import Path


class MorphologyDB(object):
    """Database wrapper to handle morphology mapping

    :param str db_path: directory that contains the morphologies as .h5
    """
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._db = {}

    def __getitem__(self, morpho: str):
        item = self._db.get(morpho)
        if not item:
            path = str(self.db_path / (morpho + ".h5"))
            item = self._db[morpho] = morphokit.Morphology(path)
        return item

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_db']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._db = dict()

    @functools.lru_cache(None)
    def first_axon_section(self, morpho: str):
        types = self[morpho].section_types
        return types.index(int(morphokit.SectionType.axon)) + 1

    @functools.lru_cache(None)
    def soma_radius(self, morpho: str):
        soma = self[morpho].soma
        return soma.max_distance

    @functools.lru_cache(None)
    def distance_to_soma(self, morpho: str, section: int, segment: int):
        sec = self[morpho].section(section - 1)
        return sec.distance_to_soma(segment)

    @functools.lru_cache(None)
    def ancestors(self, morpho: str, section: int):
        sec = self[morpho].section(section - 1)
        return list(s.id + 1 for s in sec.iter(morphokit.upstream))
