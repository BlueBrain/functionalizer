"""
"""
import fnmatch
import itertools
import logging

from typing import Dict, Iterable, Iterator, List, Tuple

from ..property import Property, PropertyGroup


logger = logging.getLogger(__name__)


class MTypeRule(Property):
    """Class defining parameters to determine reduction and cutting probabilities."""

    _name = "mTypeRule"

    _attributes = {
        "fromMType": "*",
        "toMType": "*",
        "probability": float,
        "active_fraction": float,
        "bouton_reduction_factor": float,
        "cv_syns_connection": float,
        "mean_syns_connection": float,
        "stdev_syns_connection": float,
        "p_A": float,
        "pMu_A": float,
    }

    _alias = {"from": "fromMType", "to": "toMType"}

    def __call__(
        self, src_mtypes: Iterable[str], dst_mtypes: Iterable[str]
    ) -> Iterator[Tuple[str, str, "MTypeRule"]]:
        for src, dst in itertools.product(
            fnmatch.filter(src_mtypes, self.fromMType),
            fnmatch.filter(dst_mtypes, self.toMType),
        ):
            yield (src, dst, self)

    def overrides(self, other: "MTypeRule") -> bool:
        if "*" not in self.fromMType and "*" in other.fromMType:
            return True
        elif "*" not in self.toMType and "*" in other.toMType:
            return True
        else:
            return False

    def validate(self) -> bool:
        # Rule according to validation in ConnectivityPathway::getReduceAndCutParameters
        allowed_parameters = [
            {"mean_syns_connection", "stdev_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "mean_syns_connection"},
            {"bouton_reduction_factor", "cv_syns_connection", "probability"},
            {"bouton_reduction_factor", "pMu_A", "p_A"},
        ]

        set_parameters = set(
            attr for attr in self._float_fields if getattr(self, attr) is not None
        )
        if set_parameters not in allowed_parameters:
            logger.warning(f"The following rule does not conform: {self}")
            return False
        return True


class ConnectionRules(PropertyGroup):
    _kind = MTypeRule

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
            if not r.validate():
                valid = False
            values = list(r(src_mtypes, dst_mtypes))
            if len(values) == 0:
                logger.warning(f"The following rule does not match anything: {r}")
                valid = False
            for src, dst, _ in values:
                covered_src.add(src)
                covered_dst.add(dst)
        return all(
            [
                valid,
                _check(src_mtypes, covered_src, "source"),
                _check(dst_mtypes, covered_dst, "target"),
            ]
        )

    def to_matrix(
        self, src_mtypes: List[str], dst_mtypes: List[str]
    ) -> Dict[int, MTypeRule]:
        """Construct a connection rule matrix

        Args:
            src_mtypes: The morphology types associated with the source population
            dst_mtypes: The morphology types associated with the target population
        Returns:
            A flattenend matrix, in form of a dictionary, where the keys
            are the numerical equivalent of the source and target mtypes:
            ``source << 16 | target``.  The actual values are the rules
            themselves.
        """
        src_mtypes_rev = {mtype: i for i, mtype in enumerate(src_mtypes)}
        dst_mtypes_rev = {mtype: i for i, mtype in enumerate(dst_mtypes)}
        concrete_rules = {}
        for rule in self:
            for src, dst, _ in rule(src_mtypes, dst_mtypes):
                key = (src_mtypes_rev[src] << 16) + dst_mtypes_rev[dst]
                if key in concrete_rules:
                    if rule.overrides(concrete_rules[key]):
                        concrete_rules[key] = rule
                else:
                    concrete_rules[key] = rule
        return concrete_rules
