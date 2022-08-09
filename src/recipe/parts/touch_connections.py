"""Parametrization of synapse connectivity."""
import functools
import logging
import operator

from typing import Dict, List

from .common import NODE_FIELDS
from ..property import PathwayProperty, PathwayPropertyGroup


logger = logging.getLogger(__name__)


class ConnectionRule(PathwayProperty):
    """Class defining parameters to determine reduction and cutting probabilities."""

    _name = "rule"
    _alias = "mTypeRule"

    _attributes = NODE_FIELDS | {
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

    def overrides(self, other: "ConnectionRule") -> bool:
        """Returns true if the current rule supersedes the `other` one, false otherwise.

        The more specialized, i.e., the less `columns` contain the global
        match ``*``, the higher the precedence of a rule.
        I.e. ``<rule fromMType="L*" …/>`` will override ``<rule fromMType="*" …/>``.
        """
        mine = sum(getattr(self, col) == "*" for col in self.columns())
        theirs = sum(getattr(other, col) == "*" for col in self.columns())
        return mine <= theirs

    def validate(self, _: Dict[str, List[str]] = None) -> bool:
        """Verifies that every rule uses only allowed attribute combinations."""
        # Rule according to validation in ConnectivityPathway::getReduceAndCutParameters
        allowed_parameters = [
            {"mean_syns_connection", "stdev_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "active_fraction"},
            {"bouton_reduction_factor", "cv_syns_connection", "mean_syns_connection"},
            {"bouton_reduction_factor", "cv_syns_connection", "probability"},
            {"bouton_reduction_factor", "pMu_A", "p_A"},
        ]
        possible_fields = functools.reduce(operator.or_, allowed_parameters)

        set_parameters = set(attr for attr in possible_fields if getattr(self, attr) is not None)
        if set_parameters not in allowed_parameters:
            logger.warning("The following rule does not conform: %s (%s)", self, set_parameters)
            return False
        return True


class ConnectionRules(PathwayPropertyGroup):
    """Group of `ConnectionRule`."""

    _kind = ConnectionRule
