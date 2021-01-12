"""Basic components to build XML recipes
"""
import re

from copy import deepcopy
from typing import Any, Callable, Dict, Optional, Union
from textwrap import indent


SURROUNDING_ZEROS = re.compile(r"^0*([1-9][0-9]*|0(?=\.))(?:|.0+|(.[0-9]*[1-9]+)0*)$")


def _sanitize(value):
    if isinstance(value, str):
        return SURROUNDING_ZEROS.sub(r"\1\2", value)
    return value


class NotFound(Exception):
    def __init__(self, cls):
        super().__init__(f"cannot find {cls.__name__}")


class Property:
    """
    """
    _name: Union[str, None] = None

    _alias: Dict[str, str] = {}
    _attributes: Dict[str, Any] = {}

    _implicit = False

    def __init__(self, **kwargs):
        defaults = kwargs.pop("defaults", {})
        self._i = kwargs.pop("index", None)
        self._imlicit = self._implicit
        self._local_attributes = deepcopy(self._attributes)
        self._values: Dict[str, Any] = {}
        for key, value in kwargs.items():
            setattr(self, key, value)
        for key, value in defaults.items():
            if key not in self._local_attributes:
                raise TypeError(
                    f"cannot set a default for non-existant attribute {key} in {type(self)}"
                )
            self._local_attributes[key] = self._convert(key, value)
        for key, value in self._alias.items():
            if not hasattr(self, value):
                raise TypeError(
                    f"cannot alias non-existant {value} to {key} in {type(self)}"
                )

    def __delattr__(self, attr):
        resolved_name = self._alias.get(attr, attr)
        if resolved_name in self._values:
            del self._values[resolved_name]

    def __getattr__(self, attr):
        resolved_name = self._alias.get(attr, attr)
        if attr.startswith("_"):
            return self.__dict__[attr]
        elif resolved_name in self._local_attributes:
            default = self._local_attributes[resolved_name]
            if isinstance(default, type):
                default = None
            return self._values.get(resolved_name, default)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr, value):
        resolved_name = self._alias.get(attr, attr)
        if attr.startswith("_"):
            super().__setattr__(attr, value)
        elif resolved_name in self._local_attributes:
            self._values[resolved_name] = self._convert(resolved_name, value)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __getstate__(self):
        return (self._i, self._implicit, self._local_attributes, self._values)

    def __setstate__(self, state):
        self._i, self._implicit, self._local_attributes, self._values = state

    def __str__(self):
        def vals():
            yield ""
            for key, value in self._values.items():
                yield f'{key}="{value}"'
        attrs = " ".join(vals())
        print(self._imlicit, attrs)
        if self._implicit and len(attrs) == 0:
            return ""
        tag = self._name or type(self).__name__
        return f"<{tag}{attrs} />"

    def _convert(self, attr, value):
        kind = self._local_attributes[attr]
        if not isinstance(kind, type):
            kind = type(kind)
        new_value = kind(value)
        round_trip = type(value)(new_value)
        if _sanitize(round_trip) != _sanitize(value):
            raise TypeError(
                "conversion mismatch: {}={}({}) results in {}"
                .format(attr, kind.__name__, value, new_value)
            )
        return new_value

    def validate(self, **kwargs) -> bool:
        return True


class PropertyGroup(list):
    _kind: Property
    _name: Optional[str] = None
    _defaults: Dict[str, Any] = {}

    def __init__(self, *args, **kwargs):
        self._defaults = kwargs.pop("defaults", {})
        super().__init__(*args, **kwargs)

    def __str__(self):
        def vals():
            yield ""
            for key, value in self._defaults.items():
                yield f'{key}="{value}"'
        attrs = " ".join(vals())
        inner = "\n".join(str(e) for e in self)
        if inner:
            inner = "\n" + indent(inner, "  ") + "\n"
        tag = self._name or type(self).__name__
        return f"<{tag}{attrs}>{inner}</{tag}>"

    @classmethod
    def load(cls, xml):
        """Load a list of properties defined by the class
        """
        tag = cls._name or cls.__name__
        root = xml.findall(tag)
        if len(root) == 0:
            raise NotFound(cls)
        if len(root) > 1:
            raise ValueError(f"{cls._name} needs to be defind exactly once")
        defaults = root[0].attrib
        items = root[0].findall(cls._kind._name)
        data = [cls._kind(index=i, defaults=defaults, **item.attrib)
                for i, item in enumerate(items)]
        return cls(data, defaults=defaults)


def singleton(fct: Union[Callable, None] = None, implicit: bool = False) -> Callable:
    """Decorate a class to allow initializing with an XML element

    Will search for the classes name in the provided element, and load

    Args:
        fct:      the callable object to decorate
        implicit: will create the decorated element with default parameters
                  if it is not found
    """
    def _deco(cls):
        def _loader(*args, **kwargs):
            tag = cls._name or cls.__name__
            if (len(args) > 0) == (len(kwargs) > 0):
                raise TypeError("check call signature")
            elif args:
                if len(args) > 1:
                    raise TypeError("check call signature")
                items = args[0].findall(tag)
                if len(items) == 0:
                    if implicit:
                        obj = cls()
                        obj._implicit = True
                        return obj
                    else:
                        raise NotFound(cls)
                elif len(items) > 1:
                    raise ValueError(f"singleton expected for {cls}")
                return cls(**items[0].attrib)
            return cls(**kwargs)
        cls.load = _loader
        return cls
    if fct:
        return _deco(fct)
    return _deco
