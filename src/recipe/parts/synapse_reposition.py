"""
"""
import fnmatch
import itertools

from typing import Iterable, Iterator, Tuple

from ..property import MTypeValidator, Property, PropertyGroup


class Shift(Property):
    _name = "shift"

    _attributes = {"fromMType": "*", "toMType": "*", "type": "*"}

    def __call__(
        self, src_mtypes: Iterable[str], dst_mtypes: Iterable[str]
    ) -> Iterator[Tuple[str, str, str, str]]:
        for src, dst in itertools.product(
            fnmatch.filter(src_mtypes, self.fromMType),
            fnmatch.filter(dst_mtypes, self.toMType),
        ):
            yield (src, dst, self.type)


class SynapseShifts(PropertyGroup, MTypeValidator):
    _name = "SynapsesReposition"
    _kind = Shift

    _mtype_coverage = False  # MTypeValidator will only test the rules
