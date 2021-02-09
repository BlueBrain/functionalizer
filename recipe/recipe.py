"""Basic definitions for recipe parsing
"""
import copy
import logging
import textwrap
from lxml import etree
from typing import Dict, List, Optional, Tuple

from .property import NotFound
from .parts.bouton_density import InitialBoutonDistance
from .parts.gap_junction_properties import GapJunctionProperties
from .parts.seeds import Seeds
from .parts.spine_lengths import SpineLengths
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

    Currently, the following parts of the recipe are supported:

    * ``bouton_distances``: Optional definition of the bouton distances
      from the soma.  See also
      :class:`~recipe.parts.bouton_density.InitialBoutonDistance`.

    * ``connection_rules``: Optional parameters for the connectivity
      reduction.  See also
      :class:`~recipe.parts.touch_connections.ConnectionRules`.

    * ``gap_junction_properties``: Optional definition of attributes for
      gap junctions.  See also
      :class:`~recipe.parts.gap_junction_properties.GapJunctionProperties`.

    * ``touch_reduction``: Optional parameter for a simple trimming of
      touches with a survival probability.  See also
      :class:`~recipe.parts.touch_connections.ConnectionRules`.

    * ``touch_rules``: Detailed definition of allowed synapses. All touches
      not matching the definitions here will be removed.  See also
      :class:`~recipe.parts.touch_rules.TouchRules`.

    * ``seeds``: Seeds to use when generating random numbers during the
      touch reduction stage.  See also :class:`~recipe.parts.seeds.Seeds`.

    * ``spine_lengths``: Defines percentiles for the length of spines of
      synapses, i.e., the distance between the surface positions of
      touches.  See also :class:`~recipe.parts.spine_lengths.SpineLengths`.

    * ``synapse_properties``: Classifies synapses and assigns parameters
      for the physical properties of the synapses.  See also
      :class:`~recipe.parts.synapse_properties.SynapseProperties`.

    * ``synapse_reposition``: Definition of re-assignment of somatic
      synapses to the first axon segment.  See also
      :class:`~recipe.parts.synapse_reposition.SynapseShifts`.

    Args:
        filename: path to an XML file to extract the recipe from
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
    See also :class:`~recipe.parts.touch_connections.ConnectionRules`.
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

    def __init__(self, filename: str):
        xml = self._load_from_xml(filename)
        for name, kind in self.__annotations__.items():
            try:
                if not hasattr(kind, "load"):
                    kind, = [cls for cls in kind.__args__ if cls != type(None)]
                setattr(self, name, kind.load(xml))
            except NotFound as e:
                logger.warning("missing recipe part: %s", e)

    def validate(self, src_mtypes: List[str], dst_mtypes: List[str], **kwargs) -> bool:
        """Validate basic functionality of the recipe.

        Checks provided here-in test for basic coverage, and provides a
        weak assertion that the recipe should be functional.

        Args:
            src_mtypes: List of the `mtypes` of the source node population
            dst_mtypes: List of the `mtypes` of the target node population
        """
        def _inner():
            for name in self.__annotations__:
                attr = getattr(self, name, None)
                if attr and not attr.validate(**kwargs):
                    yield False
                yield True
        return all(_inner())

    def dumps(self, split_connectivity: bool = True) -> str:
        contents = []
        for name in self.__annotations__:
            attr = getattr(self, name, None)
            if attr:
                text = str(attr)
                if len(text) > 0:
                    contents.append(text)
        inner = textwrap.indent("\n".join(contents), "  ")
        if inner:
            inner = "\n" + inner
        return RECIPE_SHELL.format(inner)

    def __str__(self) -> str:
        """Converts the recipe into XML.
        """
        return self.dumps(False)

    @staticmethod
    def _load_from_xml(recipe_file):
        try:
            # Parse the given XML file:
            parser = etree.XMLParser(recover=True, remove_comments=True)
            tree = etree.parse(recipe_file, parser)
        except (etree.XMLSyntaxError, etree.ParserError) as err:
            logger.warning(f"could not parse xml of recipe '{recipe_file}'")
            raise err
        except IOError as err:
            logger.warning(f"error while reading xml: {err}")
            raise err
        else:
            return tree.getroot()


RECIPE_SHELL="""\
<?xml version="1.0"?>
<blueColumn>{}
</blueColumn>
"""
