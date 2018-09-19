from __future__ import print_function, absolute_import

"""
Query interface for Neuron dataframe / graph
"""

from abc import abstractmethod
import hashlib
import inspect
from pathlib import Path

import sparkmanager as sm

from .circuit import Circuit
from .utils import get_logger
from .utils.checkpointing import checkpoint_resume, CheckpointHandler

logger = get_logger(__name__)


# ---------------------------------------------------
# Dataset operations
# ---------------------------------------------------
class __DatasetOperationType(type):
    """Forced unification of classes
    """
    __filters = dict()

    def __init__(cls, name, bases, attrs) -> None:
        if 'apply' not in attrs:
            raise AttributeError(f'class {cls} does not implement "apply(circuit)"')
        try:
            spec = inspect.getargspec(attrs['apply'])
            if not (spec.varargs is None and
                    spec.keywords is None and
                    spec.defaults is None and
                    spec.args == ['self', 'circuit']):
                raise TypeError
        except TypeError:
            raise AttributeError(f'class {cls} does not implement "apply(circuit)" properly')
        spec = inspect.getargspec(cls.__init__)
        if not (spec.varargs is None and
                spec.keywords is None and
                spec.defaults is None and
                spec.args == ['self', 'recipe', 'morphos', 'stats']):
            raise AttributeError(f'class {cls} does not implement "__init__(recipe, morphos, stats)" properly')
        type.__init__(cls, name, bases, attrs)
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


class DatasetOperation(object, metaclass=__DatasetOperationType):
    """Force filters to have a unified interface
    """

    _checkpoint = False
    _checkpoint_buckets = None

    def __init__(self, recipe, morphos, stats):
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
                                       CheckpointHandler.before_save(Circuit.only_touch_columns)],
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
