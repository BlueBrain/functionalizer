"""
"""
import fnmatch
import itertools
import logging
import numpy as np

from collections import Counter
from typing import Iterable, Iterator, List, Tuple

from ..property import Property, PropertyGroup

logger = logging.getLogger(__name__)


class SynapseRule(Property):
    """Class representing a Synapse property"""

    _name = "synapse"

    _attributes = {
        "fromSClass": "*",
        "toSClass": "*",
        "fromMType": "*",
        "toMType": "*",
        "fromEType": "*",
        "toEType": "*",
        "type": str,
        "neuralTransmitterReleaseDelay": 0.1,
        "axonalConductionVelocity": 300.0,
    }

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

        if not self.type or self.type[0] not in "EI":
            raise ValueError(f"Synapse type needs to start with either 'E' or 'I'")


class SpynapseRules(PropertyGroup):
    _kind = SynapseRule
    _name = "SynapsesProperties"


class SynapseClass(Property):
    """Class representing a Synapse Classification"""
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
    _kind = SynapseClass
    _name = "SynapsesClassification"

    @classmethod
    def load(cls, xml):
        data = super(SynapseClasses, cls).load(xml)
        for attr in ("gsynSRSF", "uHillCoefficient"):
            values = sum(getattr(d, attr, None) is not None for d in data)
            if values == 0:  # no values, remove attribute
                for d in data:
                    del d._local_attributes[attr]
            elif values != len(data):
                raise ValueError(f"Attribute {attr} needs to be set/unset"
                                 f" for all {cls._name} simultaneously")
        return data


class RuleMismatch(Exception):
    def __init__(self, duplicated, mismatch):
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
        return (RuleMismatch, self.duplicated, self.mismatch)


class SynapseProperties:
    rules: SpynapseRules
    classes: SynapseClasses

    def __init__(self, rules, classes):
        self.classes = classes
        self.rules = rules

    def __str__(self):
        return f"{self.rules}\n{self.classes}"

    @classmethod
    def load(cls, xml):
        rules = SpynapseRules.load(xml)
        classes = SynapseClasses.load(xml)

        duplicates = list(cls._duplicated(classes))
        unmatched = list(cls._unmatched(rules, classes))
        if duplicates or unmatched:
            raise RuleMismatch(duplicates, unmatched)
        return cls(rules, classes)

    @staticmethod
    def _duplicated(rules):
        """Yields rules that are present more than once
        """
        seen = set()
        for rule in rules:
            if rule.id in seen:
                yield rule
            seen.add(rule.id)

    @staticmethod
    def _unmatched(rules, classes):
        """Yields rules that do not match up
        """
        types = set(r.type for r in rules)
        ids = set(c.id for c in classes)
        for r in rules:
            if r.type not in ids:
                yield r
        for c in classes:
            if c.id not in types:
                yield c
