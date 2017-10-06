# *******************************************************************************************************
# This file represents the Synapse properties and recipe, which is laded from the XML recipe file
# 2017 Fernando Pereira
# *******************************************************************************************************
from __future__ import print_function, absolute_import

from os import path
from xml.etree import ElementTree as ET
from xml.parsers.expat import ExpatError
import pprint
from .definitions import CellClass
from .utils import get_logger

logger = get_logger(__name__)


# -------------------------------------------------------------------------------------------------------------
class ConnectType:
    """Enum class for Connect Types"""
# -------------------------------------------------------------------------------------------------------------
    InvalidConnect = 0
    MTypeConnect = 1  # <mTypeRule>
    LayerConnect = 2  # <layerRule>
    ClassConnect = 3  # <sClassRule>
    MaxConnectTypes = 4

    __rule_names = {
        "mTypeRule": MTypeConnect,
        "layerRule": LayerConnect,
        "sClassRule": ClassConnect
    }
    __names = {val: name for name, val in __rule_names.items()}

    @classmethod
    def from_type_name(cls, name):
        return cls.__rule_names.get(name, cls.InvalidConnect)

    @classmethod
    def to_str(cls, index):
        return cls.__names[index]


# -------------------------------------------------------------------------------------------------------------
class ConnectivityPathRule(object):
    """Connectivity Pathway rule"""
# -------------------------------------------------------------------------------------------------------------
    connect_type = None
    source = None
    destination = None

    probability = None
    active_fraction = None
    bouton_reduction_factor = None
    cv_syns_connection = None
    mean_syns_connection = None
    stdev_syns_connection = None

    # Possible field, currently not used by functionalizer
    distance_bin = None
    probability_bin = None
    reciprocal_bin = None
    reduction_min_prob = None
    reduction_max_prob = None
    multi_apposition_slope = None
    multi_apposition_offset = None

    _float_fields = ["probability", "mean_syns_connection", "stdev_syns_connection",
                     "active_fraction", "bouton_reduction_factor", "cv_syns_connection"]

    # ------
    def __init__(self, rule_type, rule_dict, rule_children=None):
        # type: (str, dict, list) -> None

        self.connect_type = ConnectType.from_type_name(rule_type)

        # Convert names
        self.source = rule_dict.pop("from")
        self.destination = rule_dict.pop("to")

        for prop_name, prop_val in rule_dict.items():
            if prop_name in self.__class__.__dict__:
                if prop_name in ConnectivityPathRule._float_fields:
                    setattr(self, prop_name, float(prop_val))
                else:
                    setattr(self, prop_name, prop_val)

        # Convert IDS
        if self.connect_type == ConnectType.LayerConnect:
            self.source = int(self.source) - 1
            self.destination = int(self.destination) - 1

        if rule_children:
            # simple probability
            self.distance_bin = []
            self.probability_bin = []
            self.reciprocal_bin = []
            for bin in rule_children:  # type:dict
                self.distance_bin.append(bin.get("distance", 0))
                self.probability_bin.append(bin.get("probability", 0))
                self.reciprocal_bin.append(bin.get("reciprocal", 0))

        if not self.is_valid():
            logger.error("Wrong number of params in Connection Rule: " + str(self))

    # ---
    def is_valid(self):
        # Rule according to validation in ConnectivityPathway::getReduceAndCutParameters
        # Count number of rule params, must be 3
        n_set_params = sum(var is not None for var in (self.probability, self.mean_syns_connection,
                                                       self.stdev_syns_connection, self.active_fraction,
                                                       self.bouton_reduction_factor, self.cv_syns_connection))
        return n_set_params == 3

    # ---
    def __repr__(self):
        return '<%s from="%s" to="%s">' % (ConnectType.to_str(self.connect_type), self.source, self.destination)


# -------------------------------------------------------------------------------------------------------------
class _GenericPropHolder(object):
    """
    A Generic Property holder whose subclasses shall define a
    _supported_attrs class attribute with the list of supported attributes
    """
# -------------------------------------------------------------------------------------------------------------
    # We keep the index in which entries are declared
    _supported_attrs = ("_i",)

    def __init__(self, **rules):
        for name, value in rules.items():
            # Note: "_i" must be checked for since subclasses override _supported_attrs
            if name not in self._supported_attrs and name != "_i":
                logger.warning("Attribute %s not expected for recipe class %s",
                               name, type(self).__name__)
                continue

            if value == "*":
                # * the match-all, is represented as None
                setattr(self, name, None)
                continue

            # Attempt conversion to real types
            try: value = int(value)
            except ValueError:
                try: value = float(value)
                except ValueError: pass

            setattr(self, name, value)

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        attrs = " ".join('{0}="{1}"'.format(attr_name, getattr(self, attr_name))
                        for attr_name in self._supported_attrs)
        return '<{cls_name} {attrs}>'.format(cls_name=type(self).__name__, attrs=attrs)


# -------------------------------------------------------------------------------------------------------------
class SynapseBoutonDistances(_GenericPropHolder):
    """Info/filter for Synapses Bouton Distance"""
    # -------------------------------------------------------------------------------------------------------------
    _supported_attrs = {'inhibitorySynapsesDistance', 'excitatorySynapsesDistance'}
    inhibitorySynapsesDistance = 5.0
    excitatorySynapsesDistance = 25.0

    def is_distance_valid(self, cell_class, soma_axon_distance):
        if cell_class == CellClass.CLASS_INH:
            return soma_axon_distance >= self.inhibitorySynapsesDistance
        if cell_class == CellClass.CLASS_EXC:
            return soma_axon_distance >= self.excitatorySynapsesDistance


# -------------------------------------------------------------------------------------------------------------
class TouchRule(_GenericPropHolder):
    """Class representing a Touch rule"""
# -------------------------------------------------------------------------------------------------------------
    _supported_attrs = {'fromLayer', 'toLayer', 'fromMType', 'toMType', 'type'}
    fromLayer = None
    toLayer = None
    fromMType = None
    toMType = None
    type = ""


# -------------------------------------------------------------------------------------------------------------
class SynapsesProperty(_GenericPropHolder):
    """Class representing a Synapse property"""
# -------------------------------------------------------------------------------------------------------------
    fromSClass = None  # None -> no filter (equiv to *wildcard)
    toSClass = None
    fromMType = None
    toMType = None
    fromEType = None
    toEType = None
    type = ""
    neuralTransmitterReleaseDelay = 0.1
    axonalConductionVelocity = 0.00333  # TODO: or 300?
    _supported_attrs = [k for k in locals().keys()
                        if not k.startswith("_")]


# -------------------------------------------------------------------------------------------------------------
class SynapsesClassification(_GenericPropHolder):
    """Class representing a Synapse Classification"""
# -------------------------------------------------------------------------------------------------------------
    id = ""
    gsyn = .0
    gsynVar = .0
    nsyn =.0
    nsynVar = .0
    dtc = .0
    dtcVar = .0
    u = .0
    uVar = .0
    d = .0
    dVar = .0
    f = .0
    fVar = .0
    ase = .0
    _supported_attrs = [k for k in locals().keys()
                        if not k.startswith("_")]


# -------------------------------------------------------------------------------------------------------------
class Recipe(object):
    """Class holding Recipe information"""
# -------------------------------------------------------------------------------------------------------------
    # Defaults
    neuralTransmitterReleaseDelay_default = 0.1
    defaultAxonalConductionVelocity_default = 300.0

    def __init__(self, recipe_file=None):
        self.touch_rules = []
        self.conn_rules = []
        self.synapses_distance = None
        self.synapse_properties = []
        self.synapse_classification = []
        self.recipe_seeds = [None] * 3

        if recipe_file:
            self.load_from_xml(recipe_file)

    # ------
    def load_from_xml(self, recipe_file):
        try:
            # Parse the given XML file:
            tree = ET.parse(recipe_file)
        except ExpatError as e:
            logger.warning("[XML] Error (line %d): %d", e.lineno, e.code)
            logger.warning("[XML] Offset: %d", e.offset)
            raise e
        except IOError as e:
            logger.warning("[XML] I/O Error %d: %s", e.errno, e.strerror)
            raise e
        else:
            recipe_xml = tree.getroot()

        self.load_probabilities(recipe_xml.find("ConnectionRules"))
        self.load_bouton_distance(recipe_xml.find("InitialBoutonDistance"))
        self.load_recipe_group_into_list_convert(recipe_xml.find("TouchRules"),
                                                 self.touch_rules, TouchRule)
        self.load_recipe_group_into_list_convert(recipe_xml.find("SynapsesProperties"),
                                                 self.synapse_properties, SynapsesProperty)
        self.load_recipe_group_into_list_convert(recipe_xml.find("SynapsesClassification"),
                                                 self.synapse_classification, SynapsesClassification)

    # -------
    def load_probabilities(self, connections):  # type: (list)->None
        for conn_rule in connections:
            # Create ConnectivityPath from NodeInfo-like object, compatible with xml.Element
            if not isinstance(conn_rule, ConnectivityPathRule):
                children = [child.attrib for child in conn_rule]
                conn_rule = ConnectivityPathRule(conn_rule.tag, conn_rule.attrib.copy(), children)
            self.conn_rules.append(conn_rule)

    # -------
    def load_bouton_distance(self, initial_bouton_distance_info):
        expected_fields = ("defaultInhSynapsesDistance", "defaultExcSynapsesDistance")
        if hasattr(initial_bouton_distance_info, "items"):
            infos = {key: val for key, val in initial_bouton_distance_info.items() if key in expected_fields}
            self.synapses_distance = SynapseBoutonDistances(**infos)
        else:
            self.synapses_distance = SynapseBoutonDistances()

    # -------
    @classmethod
    def load_recipe_group_into_list_convert(cls, items, dest_lst, item_cls):
        # Some fields are referred to by their index. We pick it here
        for i, item in enumerate(items):
            dest_lst.append(cls._check_convert(item, item_cls, i))

    # ------
    @staticmethod
    def _check_convert(item, cls, i=None):
        """Checks if the given item is already of type cls
           Otherwise attempts to convert by passing the dict as keyword arguments
        """
        if not isinstance(item, cls):
            if hasattr(item, "attrib"):
                item = item.attrib
            item = cls(**item)
        # Store index as _i
        if i is not None:
            item._i = i
        return item

    # -------
    def __str__(self):
        return "<\n\"Touch rules\":\n" + pprint.pformat(self.touch_rules) + \
               "\n\"Conn rules\":\n" + pprint.pformat(self.conn_rules) + ">\n"

    # -------
    class NodeInfo:
        """The Node interface
        """
        _sub_nodes = []  # The list of sub nodes
        tag = ""  # Type of connection (mTypeRule,...)
        attrib = {}  # A dict with the attributes of the connection #type:dict

        def __iter__(self, *args):
            """Gets an iterator for children nodes, required for simple probability"""
            return iter(self._sub_nodes)


####################
# TESTING
####################
def test_load_xml(path):
    r = Recipe(recipe_path)
    print(str(r))


def test_syn_distances():
    from xml.etree.ElementTree import Element
    el = Element("InitialBoutonDistance", {"blaaaa": 1, "defaultInhSynapsesDistance": 0.25})
    rep = Recipe()
    rep.load_bouton_distance(el)
    assert rep.synapses_distance.inhibitorySynapsesDistance == 0.25
    rep.load_bouton_distance({"blaaaa": 1, "defaultInhSynapsesDistance": 0.26})
    assert rep.synapses_distance.inhibitorySynapsesDistance == 0.26


if __name__ == "__main__":
    BASE_DIR = "/home/leite/dev/TestData/circuitBuilding_1000neurons"
    recipe_path = path.join(BASE_DIR, "recipe/builderRecipeAllPathways.xml")
    test_load_xml(recipe_path)
    test_syn_distances()
