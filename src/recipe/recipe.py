"""
Top-Level Interface for Recipe Handling
---------------------------------------
"""
import copy
import logging
import textwrap
from io import StringIO
from lxml import etree
from typing import Dict, List, Optional, TextIO, Tuple

from .property import NotFound
from .parts.bouton_density import InitialBoutonDistance
from .parts.gap_junction_properties import GapJunctionProperties
from .parts.seeds import Seeds
from .parts.spine_lengths import SpineLengths
from .parts.structure import InterBoutonInterval, StructuralSpineLengths
from .parts.synapse_properties import SynapseProperties
from .parts.synapse_reposition import SynapseShifts
from .parts.touch_connections import ConnectionRules
from .parts.touch_reduction import TouchReduction
from .parts.touch_rules import TouchRules

logger = logging.getLogger(__name__)


class Recipe(object):
    """Recipe information used for functionalizing circuits

    Instances of this class can be used to parse the XML description of our
    brain circuits, passed in via the parameter `filename`.
    All parts of the recipe present are extracted and can be accessed via
    attributes, where optional parts may be set to ``None``.

    Args:
        filename: path to an XML file to extract the recipe from
        strict: will raise when additional attributes are encountered, on
                by default
    """

    bouton_interval: InterBoutonInterval
    """The interval parametrization specifying how touches should be
    distributed where cell morphologies overlap.  See also
    :class:`~recipe.parts.structure.InterBoutonInterval`.
    """

    bouton_distances: Optional[InitialBoutonDistance] = None
    """Optional definition of the bouton distances from the soma.
    See also :class:`~recipe.parts.bouton_density.InitialBoutonDistance`.
    """

    connection_rules: Optional[ConnectionRules] = None
    """Optional parameters for the connectivity reduction.
    See also :class:`~recipe.parts.touch_connections.ConnectionRules`.
    """

    gap_junction_properties: Optional[GapJunctionProperties] = None
    """Optional definition of attributes for gap junctions.
    See also :class:`~recipe.parts.gap_junction_properties.GapJunctionProperties`.
    """

    touch_reduction: Optional[TouchReduction] = None
    """Optional parameter for a simple trimming of touches with a survival
    probability.
    See also :class:`~recipe.parts.touch_reduction.TouchReduction`.
    """

    touch_rules: TouchRules
    """Detailed definition of allowed synapses. All touches not matching
    the definitions here will be removed.
    See also :class:`~recipe.parts.touch_rules.TouchRules`.
    """

    seeds: Seeds
    """Seeds to use when generating random numbers during the touch
    reduction stage.
    See also :class:`~recipe.parts.seeds.Seeds`.
    """

    spine_lengths: SpineLengths
    """Defines percentiles for the length of spines of synapses, i.e., the
    distance between the surface positions of touches.
    See also :class:`~recipe.parts.spine_lengths.SpineLengths`.
    """

    structural_spine_lengths: StructuralSpineLengths
    """Defines the maximum length of spines used by TouchDetector.
    See also :class:`~recipe.parts.structure.StructuralSpineLengths`.
    """

    synapse_properties: SynapseProperties
    """Classifies synapses and assigns parameters for the physical
    properties of the synapses.
    See also :class:`~recipe.parts.synapse_properties.SynapseProperties`.
    """

    synapse_reposition: SynapseShifts
    """Definition of re-assignment of somatic synapses to the first axon
    segment.
    See also :class:`~recipe.parts.synapse_reposition.SynapseShifts`.
    """

    def __init__(self, filename: str, strict: bool = True):
        xml = self._load_from_xml(filename)
        for name, kind in self.__annotations__.items():
            try:
                if not hasattr(kind, "load"):
                    kind, = [cls for cls in kind.__args__ if cls != type(None)]
                setattr(self, name, kind.load(xml, strict=strict))
            except NotFound as e:
                logger.warning("missing recipe part: %s", e)

    def validate(
        self, values: Dict[str, List[str]]
    ) -> bool:
        """Validate basic functionality of the recipe.

        Checks provided here-in test for basic coverage, and provides a
        weak assertion that the recipe should be functional.

        The parameter `values` should be a dictionary containing all
        allowed values in the form ``fromMType``
        """
        def _inner():
            for name in self.__annotations__:
                attr = getattr(self, name, None)
                if attr and not attr.validate(values):
                    yield False
                yield True
        return all(_inner())

    def dump(self, fd: TextIO, connectivity_fd: Optional[TextIO] = None):
        """Write the recipe to the provided file descriptors.

        If `connectivity_fd` is given, the connection rules of the recipe
        are written to it, otherwise dump the whole recipe to `fd`.
        """
        contents = []
        header = ""
        for name in self.__annotations__:
            if not (hasattr(self, name) and getattr(self, name)):
                continue
            text = str(getattr(self, name))
            if len(text) == 0:
                continue
            if name == "connection_rules" and connectivity_fd:
                connectivity_fd.write(text)
                contents.append("&connectivity;")
                header = _CONNECTIVITY_DECLARATION.format(
                    getattr(connectivity_fd, "name", "UNKNOWN")
                )
            else:
                contents.append(text)
        inner = textwrap.indent("\n".join(contents), "  ")
        if inner:
            inner = "\n" + inner
        fd.write(_RECIPE_SKELETON.format(header, inner))

    def __str__(self) -> str:
        """Converts the recipe into XML.
        """
        output = StringIO()
        self.dump(output)
        return output.getvalue()

    @staticmethod
    def _load_from_xml(recipe_file):
        try:
            # Parse the given XML file:
            parser = etree.XMLParser(remove_comments=True)
            tree = etree.parse(recipe_file, parser)
        except (etree.XMLSyntaxError, etree.ParserError) as err:
            logger.warning(f"could not parse xml of recipe '{recipe_file}'")
            raise err
        except IOError as err:
            logger.warning(f"error while reading xml: {err}")
            raise err
        else:
            return tree.getroot()


_RECIPE_SKELETON = """\
<?xml version="1.0"?>{}
<blueColumn>{}
</blueColumn>
"""

_CONNECTIVITY_DECLARATION = """
<!DOCTYPE blueColumn [
  <!ENTITY connectivity SYSTEM "{}">
]>\
"""
