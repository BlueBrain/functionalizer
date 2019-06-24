from __future__ import print_function, absolute_import

"""
Query interface for Neuron dataframe / graph
"""

from abc import abstractmethod
from glob import glob
import hashlib
import importlib
import inspect
import os
from pathlib import Path
import sys

import sparkmanager as sm

from spykfunc.circuit import Circuit
from spykfunc.utils import get_logger
from spykfunc.utils.checkpointing import checkpoint_resume, CheckpointHandler

logger = get_logger(__name__)


def load(*dirnames: str) -> None:
    """Load plugins from a list of directories

    If no directories are given, load a default set of plugins.

    Args:
        dirnames: A list of directories to load plugins from.
    """
    if not dirnames:
        dirnames = [os.path.join(os.path.dirname(__file__), "implementations")]
    for dirname in dirnames:
        filenames = glob(f"{dirname}/*.py")
        for filename in filenames:
            modulename = os.path.realpath(filename[:-3])
            relative = min(
                [os.path.relpath(modulename, p) for p in sys.path],
                key=len
            )
            modulename = relative.replace(os.sep, ".")
            importlib.import_module(modulename)


# ---------------------------------------------------
# Dataset operations
# ---------------------------------------------------
class __DatasetOperationType(type):
    """Forced unification of classes

    The structure of the constructor and application function to circuits
    is pre-defined to be able to construct and apply filters automatically.

    Classes implementing this type are automatically added to a registry,
    with a trailing "Filter" stripped of their name.  Set the attribute
    `_visible` to `False` to exclude a filter from appearing in the list.
    """
    __filters = dict()

    def __init__(cls, name, bases, attrs) -> None:
        if 'apply' not in attrs:
            raise AttributeError(f'class {cls} does not implement "apply(circuit)"')
        try:
            spec = inspect.getfullargspec(attrs['apply'])
            if not (spec.varargs is None and
                    spec.varkw is None and
                    spec.defaults is None and
                    spec.args == ['self', 'circuit']):
                raise TypeError
        except TypeError:
            raise AttributeError(f'class {cls} does not implement "apply(circuit)" properly')
        spec = inspect.getfullargspec(cls.__init__)
        if not (spec.varargs is None and
                spec.varkw is None and
                spec.defaults is None and
                spec.args == ['self', 'recipe', 'morphos']):
            raise AttributeError(f'class {cls} does not implement "__init__(recipe, morphos)" properly')
        type.__init__(cls, name, bases, attrs)
        if attrs.get('_visible', True):
            cls.__filters[name.replace('Filter', '')] = cls

    @classmethod
    def initialize(cls, names, *args):
        """Create filters from a list of names.

        :param names: A list of filter class names to invoke
        :param args: Arguments to pass through to the filters
        :return: A list of filter instances
        """
        for fcls in cls.__filters.values():
            if hasattr(fcls, '_checkpoint_name'):
                delattr(fcls, '_checkpoint_name')
        key = hashlib.sha256()
        key.update(b'foobar3000')
        filters = []
        for name in names:
            fcls = cls.__filters.get(name, cls.__filters.get(name + 'Filter'))
            if fcls is None:
                raise ValueError(f"Cannot find filter '{name}'")
            key.update(fcls.__name__.encode())
            if hasattr(fcls, "_checkpoint_name"):
                raise ValueError(f"Cannot have more than one {fcls.__name__}")
            fcls._checkpoint_name = f"{fcls.__name__.replace('Filter', '').lower()}" \
                                    f"_{key.hexdigest()[:8]}"
            filters.append(fcls(*args))
        for i in range(len(filters) - 1, -1, -1):
            base = Path(checkpoint_resume.directory)
            parquet = filters[i]._checkpoint_name + '.parquet'
            table = filters[i]._checkpoint_name + '.ptable'
            fn = '_SUCCESS'
            if (base / parquet / fn).exists() or (base / table / fn).exists():
                classname = filters[i].__class__.__name__
                logger.info(f"Found checkpoint for {classname}")
                break
        for f in filters[:i]:
            classname = f.__class__.__name__
            logger.info(f"Removing {classname}")
        return filters[i:]

    @classmethod
    def modules(cls):
        """List registered subclasses
        """
        return sorted(cls.__filters.keys())


def _log_touch_count(df):
    """Print information for end users
    """
    logger.info("Surviving touches: %d", df.count())
    return df


class DatasetOperation(object, metaclass=__DatasetOperationType):
    """Force filters to have a unified interface
    """

    _checkpoint = False
    _checkpoint_buckets = None

    _visible = False

    def __init__(self, recipe, morphos):
        """Empty constructor supposed to be overriden
        """
        pass

    def __call__(self, circuit):
        classname = self.__class__.__name__
        logger.info(f"Applying {classname}")
        with sm.jobgroup(classname):
            if not self._checkpoint:
                circuit.df = self.apply(circuit)
            else:
                @checkpoint_resume(self._checkpoint_name,
                                   handlers=[
                                       CheckpointHandler.before_save(Circuit.only_touch_columns),
                                       CheckpointHandler.post_resume(_log_touch_count)],
                                   bucket_cols=self._checkpoint_buckets)
                def fun():
                    return self.apply(circuit)
                circuit.df = fun()
            return circuit

    @abstractmethod
    def apply(self, circuit):
        """Needs actual implementation of the operation.

        :param circuit: An instance of the `Circuit` class
        :return: A Spark dataframe representing the updated circuit
        """
        pass
