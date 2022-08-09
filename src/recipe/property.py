"""Basic components to build XML recipes."""
import fnmatch
import itertools
import logging
import re

from collections import defaultdict
from copy import deepcopy
from typing import Any, Callable, Dict, Iterator, List, Optional, Union, Tuple
from textwrap import indent

import numpy as np


SURROUNDING_ZEROS = re.compile(r"^0*([1-9][0-9]*|0(?=\.))(?:|.0+|(.[0-9]*[1-9]+)0*)$")


logger = logging.getLogger(__name__)


def _sanitize(value):
    if isinstance(value, str):
        return SURROUNDING_ZEROS.sub(r"\1\2", value)
    return value


class NotFound(Exception):
    """Exception for missing recipe component."""

    def __init__(self, cls):
        """Creates exception for missing `cls`."""
        super().__init__(f"cannot find {cls.__name__}")


class Property:
    """Generic property class that should be inherited from."""

    _name: Union[str, None] = None

    _attributes: Dict[str, Any] = {}
    _attribute_alias: Dict[str, str] = {}

    _implicit = False

    def __init__(self, strict: bool = True, **kwargs):
        """Create a new property object.

        When passing `strict`, will raise an `AttributeError` if an attribute is present
        that is not defined in `_attributes` or `_attribute_alias`.
        """
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
                raise TypeError(f"cannot alias non-existant {value} to {key} in {type(self)}")

    def __delattr__(self, attr):
        """Deletes attribute `attr`, even if it is aliased."""
        resolved_name = self._attribute_alias.get(attr, attr)
        if resolved_name in self._values:
            del self._values[resolved_name]

    def __getattr__(self, attr):
        """Gets attribute `attr`, taking aliases and inheritance into account."""
        resolved_name = self._attribute_alias.get(attr, attr)
        if attr.startswith("_"):
            return self.__dict__[attr]
        if resolved_name in self._local_attributes:
            default = self._local_attributes[resolved_name]
            if isinstance(default, type):
                default = None
            return self._values.get(resolved_name, default)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr, value):
        """Sets attribute `attr`, taking aliases and data types into account."""
        resolved_name = self._attribute_alias.get(attr, attr)
        if attr.startswith("_"):
            super().__setattr__(attr, value)
        elif resolved_name in self._local_attributes:
            self._values[resolved_name] = self._convert(resolved_name, value)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __getstate__(self):
        """State needed for pickling."""
        return (self._i, self._implicit, self._local_attributes, self._values)

    def __setstate__(self, state):
        """Restoration after pickling."""
        self._i, self._implicit, self._local_attributes, self._values = state

    def __repr__(self):
        """More legible representation."""
        return str(self)

    def __str__(self):
        """An XML like representation of the rule."""

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
                f"conversion mismatch: {attr}={kind.__name__}({value}) results in {new_value}"
            )
        return new_value

    def validate(self, _: Dict[str, List[str]] = None) -> bool:
        """Validates attribute values."""
        return True


class PropertyGroup(list):
    """A container to group settings."""

    _kind: Property
    _name: Optional[str] = None
    _alias: Optional[str] = None
    _defaults: Dict[str, Any] = {}

    def __init__(self, *args, **kwargs):
        """Creates a new property container."""
        self._defaults = kwargs.pop("defaults", {})
        super().__init__(*args, **kwargs)

    def __str__(self):
        """An XML like representation of the group."""

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
        """Load a list of properties defined by the class."""
        if isinstance(xml, str):
            from io import StringIO
            from .recipe import Recipe  # pylint: disable=cyclic-import

            xml = Recipe._load_from_xml(StringIO(xml))
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
        data = [
            cls._kind(strict=strict, index=i, defaults=defaults, **item.attrib)
            for i, item in enumerate(items)
        ]
        return cls(data, defaults=defaults)


def singleton(fct: Union[Callable, None] = None, implicit: bool = False) -> Callable:
    """Decorate a class to allow initializing with an XML element.

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
            if args:
                if len(args) > 1:
                    raise TypeError("check call signature")
                items = args[0].findall(tag)
                if len(items) == 0:
                    if implicit:
                        obj = cls()
                        obj._implicit = True
                        return obj
                    raise NotFound(cls)
                if len(items) > 1:
                    raise ValueError(f"singleton expected for {cls}")
                return cls(strict=strict, **items[0].attrib)
            return cls(strict=strict, **kwargs)

        cls.load = _loader
        return cls

    if fct:
        return _deco(fct)
    return _deco


_DIRECTIONAL_ATTR = re.compile(r"^(from|to)([A-Z]\w+)$")


class PathwayProperty(Property):
    """Property that uses pattern matching to define connectivity."""

    @classmethod
    def columns(cls):
        """Column names for attributes used in the pathway.

        The attributes of the property that indicate a source / target invocation via the
        prefixes ``from`` and ``to``.

        >>> class rule(PathwayProperty):
        ...     _attributes = {'toMType': '*', 'fromEType': '*', 'something': str}
        >>> r = rule(toMType='B*', fromEType='*')
        >>> r.columns()
        ['fromEType', 'toMType']
        """
        return sorted(k for k in cls._attributes if _DIRECTIONAL_ATTR.match(k))

    def __call__(self, values: Dict[str, List[str]]) -> Iterator[Tuple[Dict[str, str], Any]]:
        """Expand the pathway for all possible attribute values.

        Expand the property given the possible matching attribute values in `values`.
        Returns an iterator over dictionaries with the attribute values matched and the
        property itself.

        >>> class rule(PathwayProperty):
        ...     _attributes = {'toMType': '*', 'fromEType': '*', 'something': str}
        >>> r = rule(toMType='B*', fromEType='*')
        >>> list(r({'toMType': ['Foo', 'Bar']}))
        [({'toMType': 'Bar'}, <rule toMType="B*" fromEType="*" />)]
        """
        expansions = [
            [(name, v) for v in fnmatch.filter(values[name], getattr(self, name))]
            for name in self.columns()
            if name in values
        ]
        for cols in itertools.product(*expansions):
            yield (dict(cols), self)


class PathwayPropertyGroup(PropertyGroup):
    """Property group for `PathwayProperty`."""

    def validate(self, values: Dict[str, List[str]]) -> bool:
        """Basic validation.

        Checks that all rules of the group are correct and all values passed are covered.

        >>> from recipe.parts.touch_connections import ConnectionRules
        >>> rules = ConnectionRules.load('''
        ... <blueColumn>
        ...   <ConnectionRules>
        ...     <mTypeRule fromMType="*" toMType="B*"
        ...       bouton_reduction_factor="0" pMu_A="1.0" p_A="1.0" />
        ...     <mTypeRule fromMType="*" toMType="Bar"
        ...       bouton_reduction_factor="0" pMu_A="1.0" p_A="1.0" />
        ...     <mTypeRule fromMType="*" toMType="Bar"
        ...       toRegion="Spam" bouton_reduction_factor="0" pMu_A="1.0" p_A="1.0" />
        ...   </ConnectionRules>
        ... </blueColumn>
        ... ''')
        >>> values = {
        ...     'toMType': ['Bar', 'Baz'],
        ...     'toRegion': ['Ham', 'Eggs', 'Spam']
        ... }
        >>> rules.validate(values)
        True
        >>> values = {
        ...     'toMType': ['Foo', 'Bar', 'Baz'],
        ...     'toRegion': ['Ham', 'Eggs', 'Spam']
        ... }
        >>> rules.validate(values)  # Foo is not covered by the rules!
        False
        """

        def _check(name, covered):
            uncovered = set(values[name]) - covered
            if len(uncovered):
                logger.warning("The following %s are not covered: %s", name, ", ".join(uncovered))
                return False
            return True

        covered = defaultdict(set)
        valid = True
        for r in self:
            if not r.validate():
                valid = False
            expansion = list(r(values))
            if len(expansion) == 0:
                logger.warning("The following rule does not match anything: %s", r)
                valid = False
            for keys, _ in expansion:
                for name, key in keys.items():
                    covered[name].add(key)
        return valid and all(_check(name, covered[name]) for name in covered)

    @property
    def required(self) -> List[str]:
        """The required attributes for this property group.

        Returns a list of attributes whose values are required for properly selecting
        rules, i.e., all attributes used by the enclosed properties, excluding any that
        are consistently set to ``"*"``.

        For example:

        >>> from recipe.parts.touch_connections import ConnectionRules
        >>> ConnectionRules.load('''
        ... <blueColumn>
        ...   <ConnectionRules>
        ...     <mTypeRule fromMType="*" toMType="B*" />
        ...     <mTypeRule fromMType="*" toMType="Bar" />
        ...     <mTypeRule fromMType="*" toMType="Bar" toRegion="Spam" />
        ...   </ConnectionRules>
        ... </blueColumn>
        ... ''').required
        ['toMType', 'toRegion']
        """
        cols = []
        for col in self._kind.columns():
            values = set(getattr(e, col, None) for e in self)
            if values != set("*"):
                cols.append(col)
        return cols

    def pathway_index(
        self,
        key: Dict[str, str],
        values: Dict[str, List[str]],
    ) -> int:
        """Create a pathway index from `key`.

        Generate an integer pathway identifier for the provided `key`, and reference
        `values` for all attributes given by :py:attr:`~required`.

        >>> from recipe.parts.touch_connections import ConnectionRules
        >>> rules = ConnectionRules.load('''
        ... <blueColumn>
        ...   <ConnectionRules>
        ...     <mTypeRule fromMType="*" toMType="B*" />
        ...     <mTypeRule fromMType="*" toMType="Bar" />
        ...     <mTypeRule fromMType="*" toMType="Bar" toRegion="Spam" />
        ...   </ConnectionRules>
        ... </blueColumn>
        ... ''')
        >>> values = {
        ...     'toMType': ['Bar', 'Baz'],
        ...     'toRegion': ['Ham', 'Eggs', 'Spam']
        ... }
        >>> rules.pathway_index(
        ...     key={'toMType': 'Bar', 'toRegion': 'Spam'},
        ...     values=values
        ... )
        4
        >>> rules.pathway_index(
        ...     key={'toMType': 'Baz', 'toRegion': 'Spam'},
        ...     values=values
        ... )
        5
        >>> rules.pathway_index(
        ...     key={'toMType': 'Baz', 'toRegion': 'Eggs'},
        ...     values=values
        ... )
        3
        """
        factor = 1
        result = 0
        for attr in self.required:
            result += values[attr].index(key[attr]) * factor
            factor *= len(values[attr])
        return result

    def pathway_values(
        self,
        key: int,
        values: Dict[str, List[str]],
    ) -> Dict[str, str]:
        """Resolve the attribute values from pathway index `key`.

        Generate a dictionaly of attributes and values that need to be set given an
        integer pathway `key` and reference `values` for all attributes given by
        :py:attr:`~required`.

        >>> from recipe.parts.touch_connections import ConnectionRules
        >>> rules = ConnectionRules.load('''
        ... <blueColumn>
        ...   <ConnectionRules>
        ...     <mTypeRule fromMType="*" toMType="B*" />
        ...     <mTypeRule fromMType="*" toMType="Bar" />
        ...     <mTypeRule fromMType="*" toMType="Bar" toRegion="Spam" />
        ...   </ConnectionRules>
        ... </blueColumn>
        ... ''')
        >>> values = {
        ...     'toMType': ['Bar', 'Baz'],
        ...     'toRegion': ['Ham', 'Eggs', 'Spam']
        ... }
        >>> rules.pathway_values(
        ...     key=4,
        ...     values=values
        ... )
        {'toMType': 'Bar', 'toRegion': 'Spam'}
        """
        result = {}
        for attr in self.required:
            idx = key % len(values[attr])
            key //= len(values[attr])
            result[attr] = values[attr][idx]
        return result

    def to_matrix(self, values: Dict[str, List[str]]) -> np.array:
        """Construct a flattened connection rule matrix.

        Requires being passed a full set of allowed parameter values passed
        as argument `values`.  Will raise a :class:`KeyError` if any
        required attributes listed by :py:attr:`~required` are not
        present.

        Returns a :class:`numpy.array` filled with indices into the
        :class:`PathwayPropertyGroup` for each possible pathway.
        Values greater than the last index of the
        :class:`~PathwayPropertyGroup` signify no match with any rule.

        >>> from recipe.parts.touch_connections import ConnectionRules
        >>> rules = ConnectionRules.load('''
        ... <blueColumn>
        ...   <ConnectionRules>
        ...     <mTypeRule fromMType="*" toMType="B*" />
        ...     <mTypeRule fromMType="*" toMType="Bar" />
        ...     <mTypeRule fromMType="*" toMType="Bar" toRegion="Spam" />
        ...   </ConnectionRules>
        ... </blueColumn>
        ... ''')
        >>> values = {
        ...     'toMType': ['Bar', 'Baz'],
        ...     'toRegion': ['Ham', 'Eggs', 'Spam']
        ... }
        >>> m = rules.to_matrix(values)
        >>> m
        array([1, 0, 1, 0, 2, 0], dtype=uint32)
        >>> # The last rule matches Bar/Spam
        >>> idx = m[rules.pathway_index({'toMType': 'Bar', 'toRegion': 'Spam'}, values)]
        >>> idx
        2
        >>> rules[idx]
        <rule fromMType="*" toMType="Bar" toRegion="Spam" />
        """
        required_attributes = self.required
        not_present = set(required_attributes) - set(values)
        if not_present:
            raise KeyError(f"Missing keys: {', '.join(not_present)}")
        reverted = {}
        for name, allowed in values.items():
            if name in required_attributes:
                reverted[name] = {v: i for i, v in enumerate(allowed)}
        concrete = np.full(
            fill_value=len(self),
            shape=[len(values[r]) for r in required_attributes],
            dtype="uint32",
        )
        default_key = [np.arange(len(values[name])) for name in required_attributes]
        for rule in self:
            key = default_key[:]
            for n, name in enumerate(required_attributes):
                attr = getattr(rule, name)
                if attr != "*":
                    filtered = fnmatch.filter(values[name], attr)
                    indices = [reverted[name][i] for i in filtered]
                    key[n] = indices
            indices = np.ix_(*key)
            if hasattr(rule, "overrides"):
                # Provide the opportunity to selectively override previous
                # rules
                active_rules = concrete[indices]
                for old_idx in np.unique(active_rules):
                    if old_idx >= len(self) or rule.overrides(self[old_idx]):
                        active_rules = np.where(active_rules == old_idx, rule._i, active_rules)
                concrete[indices] = active_rules
            else:
                concrete[indices] = rule._i
        return concrete.flatten(order="F")


class MTypeValidator:
    """Mixin to validate rules depending on morpholgy types.

    Will test both that all rules will match some morphology types, and
    that all morphology types passed to ``validate`` will be covered by at
    least one rule.
    If the inheriting class has an attribute ``_mtype_coverage`` set to
    ``False``, the latter part of the validation will be skipped.
    """

    def validate(self, values: Dict[str, List[str]]) -> bool:
        """Checks that all MType values are covered, and rules match MTypes."""

        def _check(mtypes, covered, kind):
            uncovered = set(mtypes) - covered
            if len(uncovered):
                logger.warning(
                    "The following %s MTypes are not covered: %s",
                    kind,
                    ", ".join(uncovered),
                )
                return False
            return True

        src_mtypes = values["fromMType"]
        dst_mtypes = values["toMType"]

        covered_src = set()
        covered_dst = set()
        valid = True
        for r in iter(self):
            values = list(r(src_mtypes, dst_mtypes))
            if len(values) == 0:
                logger.warning("The following rule does not match anything: %s", r)
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
