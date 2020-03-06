# *******************************************************************************************************
# This file represents the Synapse properties and recipe, which is laded from the XML recipe file
# 2017 Fernando Pereira
# *******************************************************************************************************
from __future__ import print_function, absolute_import

from collections import namedtuple
from enum import Enum
from lxml import etree
from typing import Union

import copy

from .utils import get_logger

logger = get_logger(__name__)


class ConfigurationError(Exception):
    """Exception signaling an unrecoverable inconsistency in the recipe
    """
    pass


class Attribute:
    """Attributes to a property

    Accepts keyword arguments only apart from the name of the attribute.

    Args:
        name: The identifier in the configuration
        kind: The Python type of the attribute. Will be assumed from
              `default` if not given.
        default: Optional default value
        group_default: If group level attributes may change the default
        required: If the attribute needs to be set
        warn: Print a warning if the attribute is not set
    """
    def __init__(self,
                 name: str,
                 *,
                 alias: str = None,
                 kind: type = None,
                 default: Union[int, float, str] = None,
                 group_default: bool = False,
                 required: bool = True,
                 warn: bool = False):
        self._alias = alias
        if kind is None:
            if default is None:
                raise TypeError("__init__() missing required keyword argument "
                                "'kind' or 'default'")
            self._kind = type(default)
        elif default is not None and type(default) != kind:
            raise TypeError("__init__() has mismatch of types for required keyword "
                            "argument 'kind' or 'default'")
        elif default is None and kind is None:
            raise TypeError("__init__() has missing type for required keyword "
                            "argument 'kind' or 'default'")
        else:
            self._kind = kind
        self._name = name
        self._default = default
        self._group_default = group_default

        self._required = required
        self._warn = warn

    def convert(self, value=None):
        if value is None:
            if self.required and self._default is not None:
                return self._default
            raise ValueError(
                f"{self.name} requires a value"
            )
        return self._kind(value)

    def default(self, value):
        if not self._group_default:
            raise TypeError(f"cannot change default for '{self.name}'")
        if type(value) == int and type(value) != self._kind:
            raise TypeError(f"wrong type to change default for '{self.name}'")
        self._default = self._kind(value)

    @property
    def alias(self):
        return self._alias

    @property
    def initialized(self):
        return self._value is not None

    @property
    def name(self):
        return self._name

    @property
    def required(self):
        return self._required

    @property
    def warn(self):
        return self._warn


class PropertyMeta(type):
    def __new__(cls, name, bases, dct):
        attributes = dct.pop("attributes")
        dct.setdefault("required", True)
        dct.setdefault("singleton", False)

        attributes.insert(0, Attribute(name="_i", kind=int, required=False))

        def allowed(k):
            return k.startswith("_") or k in ("group_name", "load", "required", "singleton")
        public = [k for k in dct.keys() if not allowed(k)]

        assert \
            len(public) == 0, \
            "The only class attribute allowed are 'attributes', 'load'"

        x = super().__new__(cls, name, bases, dct)
        x._attributes = attributes

        return x

    def load(self, xml):
        """Load a list of properties defined by the class
        """
        # Setting the group's default value for an attribute will not work
        # when loading more than one group, thus restore previous state
        # after conversion.
        attributes = copy.deepcopy(self._attributes)
        try:
            if hasattr(self, "group_name"):
                e = xml.find(self.group_name)
                if e is None:
                    raise ValueError(f"Could not find '{self.group_name}'")
                attrs = {a.name: a for a in self._attributes}
                for k, v in e.attrib.items():
                    attrs[k].default(v)
                items = e
            else:
                items = xml.findall(self.__name__)
            data = [self(_i=i, **(getattr(item, "attrib", item)))
                    for i, item in enumerate(items)]
        finally:
            self._attributes = attributes

        if not self.singleton:
            return data

        if len(data) > 1:
            raise ValueError(f"{self.__name__} should only be present once")
        elif len(data) == 1:
            return data[0]
        elif not self.required:
            return self()
        raise ValueError(f"{self.__name__} could not be found")


class GenericProperty(metaclass=PropertyMeta):
    """Generic class embodying properties of settings

    Settings should inherit from this class, and define the following class
    attributes:

    - `attributes`: a list of attributes the setting can have
    - `group_name`: name under which to search for settings
    - `required`: if the settings group is optional (defaults to `True`)
    - `singleton`: the setting should be defined only once (defaults to
      `False`)

    If `group_name` is given, the attributes of the element with the same
    name will set as the default values for the encapsulated properties.
    All child items of the element will be converted into the property
    class.

    Otherwise, all items matching the property class name will be converted.

    If `required` is given and no item is matched, a `ValueError` will be
    raised.
    """

    attributes = []

    def __init__(self, **items):
        for attr in self._attributes:
            item = items.pop(attr.name, None)
            if item is None and attr.alias:
                item = items.pop(attr.alias, None)
                if item is not None:
                    logger.debug(
                        "[Alias] Attribute %s read from field %s",
                        attr.name,
                        attr.alias
                    )

            if item is None and not attr.required:
                continue
            elif item is None and attr.warn:
                logger.warning(
                    "%s: Field %s was not specified. Proceeding with default value.",
                    type(self).__name__,
                    attr.name
                )

            try:
                setattr(self, attr.name, attr.convert(item))
            except Exception as e:
                raise ConfigurationError(
                    f"{type(self).__name__}: Field {attr.name} required but not specified"
                )

        for name in items.keys():
            logger.warning("Attribute %s not expected for recipe class %s",
                           name, type(self).__name__)

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        attrs = []
        for attr in self._attributes:
            if attr.name == "_i":
                continue
            if hasattr(self, attr.name):
                attrs.append(f"{attr.name}=\"{getattr(self, attr.name)}\"")
        return '<{} {}>'.format(type(self).__name__, " ".join(attrs))


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
