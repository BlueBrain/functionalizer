"""Basic components to build XML recipes
"""
import logging
import re

from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Union, Set
from textwrap import indent


SURROUNDING_ZEROS = re.compile(r"^0*([1-9][0-9]*|0(?=\.))(?:|.0+|(.[0-9]*[1-9]+)0*)$")


logger = logging.getLogger(__name__)


def _sanitize(value):
    if isinstance(value, str):
        return SURROUNDING_ZEROS.sub(r"\1\2", value)
    return value


class NotFound(Exception):
    def __init__(self, cls):
        super().__init__(f"cannot find {cls.__name__}")


class Property:
    """Generic property class that should be inherited from.
    """
    _name: Union[str, None] = None

    _attributes: Dict[str, Any] = {}
    _attribute_alias: Dict[str, str] = {}

    _implicit = False

    def __init__(self, strict: bool = True, **kwargs):
        defaults = kwargs.pop("defaults", {})
        self._i = kwargs.pop("index", None)
        self._imlicit = self._implicit
        self._local_attributes = deepcopy(self._attributes)
        self._values: Dict[str, Any] = {}
        for key, value in kwargs.items():
            try:
                setattr(self, key, value)
            except AttributeError:
                if strict:
                    raise
        for key, value in defaults.items():
            if key not in self._local_attributes:
                raise TypeError(
                    f"cannot set a default for non-existant attribute {key} in {type(self)}"
                )
            self._local_attributes[key] = self._convert(key, value)
        for key, value in self._attribute_alias.items():
            if not hasattr(self, value):
                raise TypeError(
                    f"cannot alias non-existant {value} to {key} in {type(self)}"
                )

    def __delattr__(self, attr):
        resolved_name = self._attribute_alias.get(attr, attr)
        if resolved_name in self._values:
            del self._values[resolved_name]

    def __getattr__(self, attr):
        resolved_name = self._attribute_alias.get(attr, attr)
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
        resolved_name = self._attribute_alias.get(attr, attr)
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

    def __repr__(self):
        return str(self)

    def __str__(self):
        def vals():
            yield ""
            for key, value in self._values.items():
                yield f'{key}="{value}"'
        attrs = " ".join(vals())
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

    def validate(self, *args, **kwargs) -> bool:
        return True


class PropertyGroup(list):
    """A container to group settings.
    """
    _kind: Property
    _name: Optional[str] = None
    _alias: Optional[str] = None
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
    def load(cls, xml, strict: bool = True):
        """Load a list of properties defined by the class
        """
        tag = cls._name or cls.__name__
        root = xml.findall(tag)
        if len(root) == 0:
            if cls._alias:
                root = xml.findall(cls._alias)
            if len(root) == 0:
                raise NotFound(cls)
        if len(root) > 1:
            raise ValueError(f"{cls._name} needs to be defind exactly once")

        valid_defaults = set(cls._kind._attributes) | set(cls._kind._attribute_alias)
        defaults = {}
        for k, v in root[0].attrib.items():
            if k in valid_defaults:
                defaults[k] = v
            elif strict:
                raise AttributeError(f"'{tag}' object cannot override attribute '{k}'")

        allowed = set([cls._kind._name])
        if hasattr(cls._kind, "_alias"):
            allowed.add(cls._kind._alias)
        items = [i for i in root[0].iter() if i.tag in allowed]
        data = [cls._kind(strict=strict, index=i, defaults=defaults, **item.attrib)
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
            strict = kwargs.pop("strict", True)
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
                return cls(strict=strict, **items[0].attrib)
            return cls(strict=strict, **kwargs)
        cls.load = _loader
        return cls
    if fct:
        return _deco(fct)
    return _deco


class MTypeValidator:
    """Mixin to validate rules depending on morpholgy types.

    Will test both that all rules will match some morphology types, and
    that all morphology types passed to ``validate`` will be covered by at
    least one rule.
    If the inheriting class has an attribute ``_mtype_coverage`` set to
    ``False``, the latter part of the validation will be skipped.
    """

    def validate(
        self, values: Dict[str, List[str]]
    ) -> bool:
        def _check(mtypes, covered, kind):
            uncovered = set(mtypes) - covered
            if len(uncovered):
                logger.warning(
                    f"The following {kind} MTypes are not covered: {', '.join(uncovered)}"
                )
                return False
            return True

        src_mtypes = values["fromMType"]
        dst_mtypes = values["toMType"]

        covered_src = set()
        covered_dst = set()
        valid = True
        for r in self:
            values = list(r(src_mtypes, dst_mtypes))
            if len(values) == 0:
                logger.warning(f"The following rule does not match anything: {r}")
                valid = False
            for src, dst, *_ in values:
                covered_src.add(src)
                covered_dst.add(dst)
        if not getattr(self, "_mtype_coverage", True):
            return valid
        return all(
            [
                valid,
                _check(src_mtypes, covered_src, "source"),
                _check(dst_mtypes, covered_dst, "target"),
            ]
        )
