"""
"""
import fnmatch
import functools
import itertools
import logging
import operator

from collections import defaultdict
from typing import Dict, Iterable, Iterator, List, Tuple

from ..property import Property, PropertyGroup


logger = logging.getLogger(__name__)


class ConnectionRule(Property):
    """Class defining parameters to determine reduction and cutting probabilities."""

    _name = "rule"
    _alias = "mTypeRule"

    _attributes = {
        "fromEType": "*",
        "fromMType": "*",
        "fromRegion": "*",
        "fromSClass": "*",
        "toEType": "*",
        "toMType": "*",
        "toRegion": "*",
        "toSClass": "*",
        "probability": float,
        "active_fraction": float,
        "bouton_reduction_factor": float,
        "cv_syns_connection": float,
        "mean_syns_connection": float,
        "stdev_syns_connection": float,
        "p_A": float,
        "pMu_A": float,
    }

    _attribute_alias = {"from": "fromMType", "to": "toMType"}

    columns = [x + y for (x, y) in itertools.product(
        ("to", "from"),
        ("EType", "MType", "Region")
    )]

    def __call__(
        self, values: Dict[str, List[str]]
    ) -> Iterator[Tuple[Dict[str, str], "ConnectionRule"]]:
        expansions = [
            [(name, v) for v in fnmatch.filter(values[name], getattr(self, name))]
            for name in self.columns
            if name in values
        ]
        for cols in itertools.product(*expansions):
            yield (dict(cols), self)

    def overrides(self, other: "ConnectionRule") -> bool:
        """Returns true if the `other` rule supersedes the current one,
        false otherwise.

        The more specialized, i.e., the less `columns` contain the global
        match ``*``, the higher the precedence of a rule.
        I.e. ``<rule fromMType="L*" …/>`` will override ``<rule fromMType="*" …/>``.
        """
        mine = sum(getattr(self, col) == "*" for col in self.columns)
        theirs = sum(getattr(other, col) == "*" for col in self.columns)
        return mine < theirs

    def validate(self) -> bool:
        # Rule according to validation in ConnectivityPathway::getReduceAndCutParameters
        allowed_parameters = [
            {"mean_syns_connection", "stdev_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "mean_syns_connection"},
            {"bouton_reduction_factor", "cv_syns_connection", "probability"},
            {"bouton_reduction_factor", "pMu_A", "p_A"},
        ]
        possible_fields = functools.reduce(operator.or_, allowed_parameters)

        set_parameters = set(
            attr for attr in possible_fields if getattr(self, attr) is not None
        )
        if set_parameters not in allowed_parameters:
            logger.warning(f"The following rule does not conform: {self} ({set_parameters})")
            return False
        return True


class ConnectionRules(PropertyGroup):
    _kind = ConnectionRule

    def validate(
        self, values: Dict[str, List[str]]
    ) -> bool:
        def _check(name, covered):
            uncovered = set(values[name]) - covered
            if len(uncovered):
                logger.warning(
                    f"The following {name} are not covered: {', '.join(uncovered)}"
                )
                return False
            return True

        covered = defaultdict(set)
        names = self.requires
        valid = True
        for r in self:
            if not r.validate():
                valid = False
            expansion = list(r(values))
            if len(expansion) == 0:
                logger.warning(f"The following rule does not match anything: {r}")
                valid = False
            for keys, _ in expansion:
                for name, key in keys.items():
                    covered[name].add(key)
        return valid and all(_check(name, covered[name]) for name in covered)

    @property
    def requires(self) -> List[str]:
        """Returns a list of attributes whose values are required for
        properly selecting rules, i.e., excluding any attributes that match
        everything.
        """
        cols = []
        for col in ConnectionRule.columns:
            values = set(getattr(e, col, None) for e in self)
            if values != set("*"):
                cols.append(col)
        return cols

    def to_matrix(
        self, values: Dict[str, List[str]]
    ) -> Dict[int, ConnectionRule]:
        """Construct a flattened connection rule matrix

        Requires being passed a full set of allowed parameter values passed
        as argument `values`.
        """
        concrete = {}
        reverted = {}
        required = self.requires
        for name, allowed in values.items():
            reverted[name] = {v: i for i, v in enumerate(allowed)}
        for rule in self:
            for attrs, _ in rule(values):
                key = 0
                factor = 1
                for name in required:
                    key += reverted[name][attrs[name]] * factor
                    factor *= len(reverted[name])
                if key in concrete and concrete[key].overrides(rule):
                    pass
                else:
                    concrete[key] = rule
        return concrete
