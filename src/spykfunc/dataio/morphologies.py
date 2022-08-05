""" A lightweight morphology reader for functionalities required by fzer
"""

import functools
from pathlib import Path
from typing import Optional

import morphokit


_MAXIMUM_LRU_SIZE = 10_000


class MorphologyDB:
    """Database wrapper to handle morphology mapping

    Requires the `db_path` to be the path to the cell morphologies, whereas
    `spine_db_path` is optional and should point to a spine morphology storage directory.
    """

    def __init__(self, db_path: str, spine_db_path: Optional[str] = None):
        self.db_path = Path(db_path)
        if spine_db_path:
            self.spine_db_path = Path(spine_db_path)
        else:
            self.spine_db_path = None
        self._db = {}

    @property
    def spine_morphology_path(self) -> Optional[Path]:
        return self.spine_db_path

    def __getitem__(self, morpho: str):
        item = self._db.get(morpho)
        if not item:
            path = str(self.db_path / (morpho + ".h5"))
            item = self._db[morpho] = morphokit.Morphology(path)
        return item

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["_db"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._db = {}

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def first_axon_section(self, morpho: str):
        """Return distances for a synapse on the first axon segment.

        To avoid closeness to the soma, shift the position by 0.5 μm or the
        length of the first segment, whichever is less.

        Returns a tuple with
        * the index of the first axon section
        * the offset of the center of the first segment on said section
        * the fractional offset of this point
        * the distance from the soma of this point
        """
        types = self[morpho].section_types
        section_index = types.index(int(morphokit.SectionType.axon))
        section = self[morpho].section(section_index)
        section_length = section.pathlength(len(section.points) - 1)
        section_distance = min(0.5, section.pathlength(1))
        return (
            section_index + 1,  # MorphoK does not include the soma!
            section_distance,
            section_distance / section_length,
            section.distance_to_soma() + section_distance,
        )

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def soma_radius(self, morpho: str):
        soma = self[morpho].soma
        return soma.max_distance

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def distance_to_soma(self, morpho: str, section: int, segment: int):
        sec = self[morpho].section(section - 1)
        return sec.distance_to_soma(segment)

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def ancestors(self, morpho: str, section: int):
        sec = self[morpho].section(section - 1)
        return list(s.id + 1 for s in sec.iter(morphokit.upstream))
