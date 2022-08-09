"""Settings relating to the structure of the connectome."""
import logging
from typing import Dict, List

from ..property import Property, PropertyGroup, singleton

logger = logging.getLogger(__name__)


@singleton
class InterBoutonInterval(Property):
    """Settings for the touch redistribution of TouchDetector."""

    _attributes = {"minDistance": float, "maxDistance": float, "regionGap": float}


class StructuralSpineLength(Property):
    """Spine length used for overlap detection radius in TouchDetector."""

    _name = "rule"
    _alias = "StructuralType"

    _attributes = {
        "mType": str,
        "spineLength": float,
    }

    _attribute_alias = {"id": "mType"}


class StructuralSpineLengths(PropertyGroup):
    """Spine length specification.

    To be used as a *maximum* spine length when constructing the connectome via
    TouchDetector.
    """

    _alias = "NeuronTypes"
    _kind = StructuralSpineLength

    def validate(self, values: Dict[str, List[str]]) -> bool:
        """Checks that all morphology types have well defined spine lengths."""
        mtypes = set(values["fromMType"]) | set(values["toMType"])
        covered = {}
        valid = True
        for rule in self:
            if (length := covered.get(rule.mType)) and length != rule.spineLength:
                msg = f"{rule.mType}; {length} vs {rule.spineLength}"
                logger.warning("Conflicting spine lengths defined for: %s", msg)
                valid = False
            covered[rule.mType] = rule.spineLength
        uncovered = mtypes - set(covered)
        if uncovered:
            logger.warning("No spine lengths defined for: %s", ", ".join(uncovered))
            valid = False
        return valid
