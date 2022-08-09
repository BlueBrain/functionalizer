"""Specification of synaptic properties and classification."""
import logging

from typing import Dict, List

from .common import NODE_FIELDS
from ..property import PathwayProperty, PathwayPropertyGroup, Property, PropertyGroup

logger = logging.getLogger(__name__)


class SynapseRule(PathwayProperty):
    """Class representing a Synapse property."""

    _name = "synapse"

    _attributes = NODE_FIELDS | {
        "type": str,
        "neuralTransmitterReleaseDelay": 0.1,
        "axonalConductionVelocity": 300.0,
    }

    def __init__(self, *args, **kwargs):
        """Creates a new synapse rule.

        Enforces that the classification starts with either ``E`` or ``I``, for exitatory
        or inhibitory synapses, respectively.
        """
        super().__init__(*args, **kwargs)

        if not self.type or self.type[0] not in "EI":
            raise ValueError("Synapse type needs to start with either 'E' or 'I'")


class SpynapseRules(PathwayPropertyGroup):
    """A group of `SynapseRule`."""

    _kind = SynapseRule
    _name = "SynapsesProperties"


class SynapseClass(Property):
    """Stores the synaptic properties for a synapse class codified by the property `id`."""

    _name = "class"

    _attributes = {
        "id": str,
        "gsyn": float,
        "gsynSD": float,
        "dtc": float,
        "dtcSD": float,
        "u": float,
        "uSD": float,
        "d": float,
        "dSD": float,
        "f": float,
        "fSD": float,
        "nrrp": 0.0,
        "gsynSRSF": float,
        "uHillCoefficient": float,
    }

    _attribute_alias = {
        "gsynVar": "gsynSD",
        "dtcVar": "dtcSD",
        "uVar": "uSD",
        "dVar": "dSD",
        "fVar": "fSD",
        "ase": "nrrp",
    }


class SynapseClasses(PropertyGroup):
    """Container for synaptic properties per class."""

    _kind = SynapseClass
    _name = "SynapsesClassification"

    @classmethod
    def load(cls, xml, strict: bool = True):
        """Extract the synapse properties from `xml`.

        Enforces that the attributes ``gsynSRSF`` and ``uHillCoefficient`` are set for
        either none or all of the properties.
        """
        data = super(SynapseClasses, cls).load(xml, strict)
        for attr in ("gsynSRSF", "uHillCoefficient"):
            values = sum(getattr(d, attr, None) is not None for d in data)
            if values == 0:  # no values, remove attribute
                for d in data:
                    del d._local_attributes[attr]
            elif values != len(data):
                raise ValueError(
                    f"Attribute {attr} needs to be set/unset" f" for all {cls._name} simultaneously"
                )
        return data


class RuleMismatch(Exception):
    """Exception to handle mismatches between rules."""

    def __init__(self, duplicated, mismatch):
        """Create a new exception.

        Args:
            duplicated: rules that occur more than once
            mismatch: rules that have an identifier that is not matched by other rules
        """
        self.duplicated = duplicated
        self.mismatch = mismatch

        msg = ""
        if self.duplicated:
            msg += "rules with duplicated identifiers:\n  "
            msg += "\n  ".join(map(str, self.duplicated))
        if self.mismatch:
            if msg:
                msg += "\n"
            msg += "rules with missing counterpart:\n  "
            msg += "\n  ".join(map(str, self.mismatch))

        super().__init__(msg)

    def __reduce__(self):
        """Allows to pickle exceptions."""
        return (RuleMismatch, self.duplicated, self.mismatch)


class SynapseProperties:
    """Container to provide access to the synapse classification and properties.

    See also the classification rules :class:`~SynapseRules` and classification properties
    :class:`~SynapseClasses`.
    """

    rules: SpynapseRules
    classes: SynapseClasses

    def __init__(self, rules, classes):
        """Create a new container."""
        self.classes = classes
        self.rules = rules

    def __str__(self):
        """An XML like representation of the properties."""
        return f"{self.rules}\n{self.classes}"

    def validate(self, _: Dict[str, List[str]] = None) -> bool:
        """Validate synapse properties.

        As rule mismatch is already handled at loading time, this function always returns
        `True`.
        """
        return True

    @classmethod
    def load(cls, xml, strict: bool = True):
        """Extract synapes classification and properties from `xml`.

        Will check if any of the classification rules and property rules are unmatched or
        duplicated.
        """
        rules = SpynapseRules.load(xml, strict)
        classes = SynapseClasses.load(xml, strict)

        duplicates = list(cls._duplicated(classes))
        unmatched = list(cls._unmatched(rules, classes))
        if duplicates or unmatched:
            raise RuleMismatch(duplicates, unmatched)
        return cls(rules, classes)

    @staticmethod
    def _duplicated(rules):
        """Yields rules that are present more than once."""
        seen = set()
        for rule in rules:
            if rule.id in seen:
                yield rule
            seen.add(rule.id)

    @staticmethod
    def _unmatched(rules, classes):
        """Yields rules that do not match up."""
        types = set(r.type for r in rules)
        ids = set(c.id for c in classes)
        for r in rules:
            if r.type not in ids:
                yield r
        for c in classes:
            if c.id not in types:
                yield c
