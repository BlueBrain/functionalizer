"""
"""
import fnmatch
import itertools
import logging
import numpy as np

from typing import Iterable, Iterator, List, Tuple

from ..property import Property, PropertyGroup


# dendrite mapping here is for historical purposes only, when we
# distinguished only between soma and !soma.
BRANCH_TYPES = {
    "*": [0, 1, 2, 3],
    "soma": [0],
    "axon": [1],
    "dendrite": [2, 3],
    "basal": [2],
    "apical": [3],
}


logger = logging.getLogger(__name__)


class TouchRule(Property):
    """Class representing a Touch rule"""

    _name = "touchRule"

    _attributes = {
        "fromMType": "*",
        "fromBranchType": "*",
        "fromLayer": "*",
        "toMType": "*",
        "toBranchType": "*",
        "toLayer": "*",
    }

    _alias = {"type": "toBranchType"}

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if self.fromLayer != "*":
            raise ValueError("fromLayer is deprecated and needs to be '*'")
        if self.toLayer != "*":
            raise ValueError("toLayer is deprecated and needs to be '*'")

    def __call__(
        self, src_mtypes: Iterable[str], dst_mtypes: Iterable[str]
    ) -> Iterator[Tuple[str, str, str, str]]:
        for src, dst in itertools.product(
            fnmatch.filter(src_mtypes, self.fromMType),
            fnmatch.filter(dst_mtypes, self.toMType),
        ):
            yield (src, self.fromBranchType, dst, self.toBranchType)


class TouchRules(PropertyGroup):
    _kind = TouchRule

    def validate(self, src_mtypes: List[str], dst_mtypes: List[str]) -> bool:
        def _check(mtypes, covered, kind):
            uncovered = set(mtypes) - covered
            if len(uncovered):
                logger.warning(
                    f"The following {kind} MTypes are not covered: {', '.join(uncovered)}"
                )
                return False
            return True

        covered_src = set()
        covered_dst = set()
        valid = True
        for r in self:
            values = list(r(src_mtypes, dst_mtypes))
            if len(values) == 0:
                logger.warning(f"The following rule does not match anything: {r}")
                valid = False
            for src, _, dst, _ in values:
                covered_src.add(src)
                covered_dst.add(dst)
        return all(
            [
                valid,
                _check(src_mtypes, covered_src, "source"),
                _check(dst_mtypes, covered_dst, "target"),
            ]
        )

    def to_matrix(self, src_mtypes: List[str], dst_mtypes: List[str]) -> np.array:
        """Construct a touch rule matrix

        Args:
            src_mtypes: The morphology types associated with the source population
            dst_mtypes: The morphology types associated with the target population
        Returns:
            A multidimensional matrix, containing a one (1) for every
            connection allowed. The dimensions correspond to the numeical
            indices of morphology types of source and destination, as well
            as the rule type.
        """
        src_mtype_rev = {name: i for i, name in enumerate(src_mtypes)}
        dst_mtype_rev = {name: i for i, name in enumerate(dst_mtypes)}

        ntypes = max(len(v) for v in BRANCH_TYPES.values())

        matrix = np.zeros(
            shape=(len(src_mtype_rev), len(dst_mtype_rev), ntypes, ntypes),
            dtype="uint8",
        )

        for r in self:
            for src_mt, src_bt, dst_mt, dst_bt in r(src_mtypes, dst_mtypes):
                for i in BRANCH_TYPES[src_bt]:
                    for j in BRANCH_TYPES[dst_bt]:
                        matrix[
                            src_mtype_rev[src_mt],
                            dst_mtype_rev[dst_mt],
                            i,
                            j
                        ] = 1

        return matrix
