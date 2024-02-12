"""Morphology wrappers."""

import functools
from pathlib import Path
from typing import Optional

import morphio
import numpy as np


_MAXIMUM_LRU_SIZE = 10_000


class MorphologyDB:
    """Database wrapper to handle morphology mapping.

    Requires the `db_path` to be the path to the cell morphologies, whereas
    `spine_db_path` is optional and should point to a spine morphology storage directory.
    """

    def __init__(self, db_path: str, spine_db_path: Optional[str] = None):
        """Create a new morphology database.

        Args:
            db_path: path to the neuron morphology storage
            spine_db_path: path to the spine morphology storage
        """
        if spine_db_path:
            self.spine_db_path = Path(spine_db_path)
        else:
            self.spine_db_path = None

        self._db_path = Path(db_path)
        self._storage = morphio.Collection(str(self._db_path))
        self._db = {}

    @property
    def spine_morphology_path(self) -> Optional[Path]:
        """The storage path for spine morphologies."""
        return self.spine_db_path

    def __getitem__(self, morpho: str):
        """Read the morphology `morpho` or retrieve a cached value."""
        item = self._db.get(morpho)
        if not item:
            item = self._db[morpho] = self._storage.load(morpho)
        return item

    def __getstate__(self):
        """Returns the state for pickling, without any cached morphologies."""
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["_db"]
        del state["_storage"]
        return state

    def __setstate__(self, state):
        """Sets the state after pickling, and creates an empty morphology cache."""
        self.__dict__.update(state)
        self._db = {}
        self._storage = morphio.Collection(str(self._db_path))

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
        section_index = list(types).index(int(morphio.SectionType.axon))
        section = self[morpho].section(section_index)
        section_id = section.id + 1  # Our convention includes the soma as 0
        section_length = self.pathlengths(morpho, section_id)[-1]
        section_distance = min(0.5, self.pathlengths(morpho, section_id)[1])
        return (
            section_index + 1,  # MorphoK does not include the soma!
            section_distance,
            section_distance / section_length,
            self.distance_to_soma(morpho, section_id) + section_distance,
        )

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def soma_radius(self, morpho: str):
        """Cached soma radius for `morpho`."""
        soma = self[morpho].soma
        return soma.max_distance

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def pathlengths(self, morpho: str, section: int):
        """The cumulative distance along the section to its start."""
        s = self[morpho].section(section - 1)

        def _diff(idx):
            cs = s.points[:, idx]
            cs = cs - np.roll(cs, 1)
            cs[0] = 0.0
            return cs

        return np.cumsum(np.sqrt(_diff(0) ** 2 + _diff(1) ** 2 + _diff(2) ** 2))

    def distance_to_soma(self, morpho: str, section: int, segment: int = 0):
        """The cumulative distance along the morphology to the soma."""
        distance = self.pathlengths(morpho, section)[segment]
        if not self[morpho].section(section - 1).is_root:
            parent = self[morpho].section(section - 1).parent.id + 1
            distance += self.distance_to_soma(morpho, parent, -1)
        return distance

    @functools.lru_cache(_MAXIMUM_LRU_SIZE)
    def ancestors(self, morpho: str, section: int):
        """Cached parents for `section` of `morpho`."""
        sec = self[morpho].section(section - 1)
        return list(s.id + 1 for s in sec.iter(morphio.IterType.upstream))
