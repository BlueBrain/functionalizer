# *******************************************************************************************************
# This file represents the Synapse properties and recipe, which is laded from the XML recipe file
# 2017 Fernando Pereira
# *******************************************************************************************************
from __future__ import print_function, absolute_import

from lxml import etree
from six import iteritems
from .utils import get_logger

logger = get_logger(__name__)


class ConfigurationError(Exception):
    """Exception signaling an unrecoverable inconsistency in the recipe
    """
    pass


# A singleton to mark required fields
_REQUIRED_ = object()


class GenericProperty(object):
    """
    A Generic Property holder whose subclasses shall define a
    _supported_attrs class attribute with the list of supported attributes
    Additionally, they can define _map_attrs to set alias
    Class attributes set to None are not required.
    """

    # We keep the index in which entries are declared
    _supported_attrs = ("_i",)
    # Attributes that may change their defaults globally in the recipe
    _supported_defaults = []

    _warn_missing_attrs = ()
    # allow to rename attributes
    _map_attrs = dict()
    # allow recipe to be optional
    _required = True

    def __init__(self, **rules):
        all_attrs = set(self._supported_attrs) | set(self._map_attrs.keys())
        changed_attrs = set()

        for name, value in rules.items():
            # Note: "_i" must be checked for since subclasses override _supported_attrs
            if name not in all_attrs and name != "_i":
                logger.warning("Attribute %s not expected for recipe class %s",
                               name, type(self).__name__)
                continue

            # name exists, but might be an alias. Get original
            att_name = self._map_attrs.get(name, name)
            changed_attrs.add(att_name)
            if att_name != name:
                logger.debug("[Alias] Attribute %s read from field %s", att_name, name)

            if value == "*":
                # * the match-all, is represented as None
                setattr(self, att_name, None)
            else:
                # Attempt conversion to real types
                value = self._convert_type(value)
                setattr(self, att_name, value)

        # Look for required fields which were not set
        for att_name in self._supported_attrs:
            if not att_name.startswith("_") and att_name not in changed_attrs:
                field_desc = "%s: Field %s" % (self.__class__.__name__, att_name)
                if att_name in self._warn_missing_attrs:
                    logger.warning(field_desc + " was not specified. Proceeding with default value.")
                elif getattr(self.__class__, att_name) is _REQUIRED_:
                    raise ConfigurationError("%s required but not specified" % field_desc)

    @staticmethod
    def _convert_type(value):
        try:
            # Will raise if not int
            # Comparison False if is a true float, so no change
            if value == str(int(value)):
                return int(value)
        except ValueError:
            # check str has a float
            try:
                return float(value)
            except ValueError:
                pass
        return value

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        unmap = {v: k for k, v in iteritems(self._map_attrs)}
        all_attrs = set(self._supported_attrs) | set(self._map_attrs.keys())
        attrs = " ".join('{0}="{1}"'.format(unmap.get(n, n), getattr(self, self._map_attrs.get(n, n)))
                         for n in all_attrs if n != '_i')
        return '<{cls_name} {attrs}>'.format(cls_name=type(self).__name__, attrs=attrs)

    @classmethod
    def load(cls, xml):
        """Load a list of properties defined by the class
        """
        e = xml.find(getattr(cls, "_name", cls.__name__))
        for k, v in e.attrib.items():
            if hasattr(cls, k) and k in getattr(cls, "_supported_defaults", []):
                setattr(cls, k, cls._convert_type(v))
            else:
                raise ValueError(f"Default value for attribute {k} in {cls.__name__} not supported")
        return list(Recipe.load_group(e, cls, cls._required))

    @classmethod
    def load_one(cls, xml):
        """Load a list of properties defined by the class
        """
        e = xml.find(getattr(cls, "_name", cls.__name__))
        if hasattr(e, "items"):
            infos = {k: v for k, v in e.items()}
            return cls(**infos)
        return cls()


class Recipe(object):
    """Class holding Recipe information"""

    def __init__(self, recipe_file=None):
        self.xml = None

        if recipe_file:
            self.load_from_xml(recipe_file)

    def load_from_xml(self, recipe_file):
        try:
            # Parse the given XML file:
            parser = etree.XMLParser(recover=True, remove_comments=True)
            tree = etree.parse(recipe_file, parser)
        except (etree.XMLSyntaxError, etree.ParserError) as e:
            logger.warning("[XML] Could not parse recipe %s", recipe_file)
            raise e
        except IOError as e:
            logger.warning("[XML] I/O Error %d: %s", e.errno, e.strerror)
            raise e
        else:
            self.xml = tree.getroot()

    @classmethod
    def load_group(cls, items, item_cls, required=True):
        """Convert and yield contents of an XML list

        :param items: a list of XML elements
        :param item_cls: target class to convert the XML elements to
        :param required: allow the list of XML elements to be optional
        """
        if items is None:
            if not required:
                logger.warn("skipping conversion of %s items (not required)", item_cls.__name__)
                return
            raise AttributeError(f"Cannot find required recipe component for {item_cls.__name__}")
        # Some fields are referred to by their index. We pick it here
        for i, item in enumerate(items):
            yield cls._check_convert(item, item_cls, i)

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

    class NodeInfo:
        """The Node interface
        """
        _sub_nodes = []  # The list of sub nodes
        tag = ""  # Type of connection (mTypeRule,...)
        attrib = {}  # A dict with the attributes of the connection #type:dict

        def __iter__(self, *args):
            """Gets an iterator for children nodes, required for simple probability"""
            return iter(self._sub_nodes)
