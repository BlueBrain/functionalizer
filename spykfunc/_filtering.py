from __future__ import print_function, absolute_import

"""
Query interface for Neuron dataframe / graph
"""

from abc import abstractmethod, abstractproperty, ABCMeta
from . import schema
import copy


# ---------------------------------------------------
# Dataset operations
# ---------------------------------------------------
class DataSetOperation(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def apply(self, df, *args, **kw):
        pass


class RddOperation(object):
    """Filters shall implement _execute to run on dataframes and F to execute on RDDs
    """
    __metaclass__ = ABCMeta

    @abstractproperty
    def F(self):
        """F is a property which shall return the corresponding touch row filter function.
            Such a function shall return True if the row obeys the filter, False otherwise"""
        pass


class Filter(DataSetOperation):
    def __init__(self, field1, value_or_field, comparator="=", flat_structure=False):
        self._field1 = field1
        self._comparator = comparator
        self._comparing_value_or_field = value_or_field
        self._flat_structure = flat_structure

    def __repr__(self):
        return ".where(" + str(self._field1) + self._comparator + str(self._comparing_value_or_field) + ")"

    def apply(self, df, *args, **kw):
        return df.where(str(self._field1) + self._comparator + str(self._comparing_value_or_field))


class Projection(DataSetOperation):
    def __init__(self, fields):
        self._fields = fields

    def __repr__(self):
        return ".select(%s)" % ",".join(self._fields)

    def apply(self, df, *args, **kw):
        return df.select(*[str(f) for f in self._fields])


class Grouping(DataSetOperation):
    def __init__(self, grouping_fields, aggregating_func_per_field):
        self._fields = grouping_fields
        self._aggregating_func_per_field = aggregating_func_per_field

    def __repr__(self):
        return ".groupBy(%s)" % ",".join(str(f) for f in self._fields) + ".agg(<funcs>)"

    def apply(self, df, *args, **kw):
        groups = df.groupBy(*[str(f) for f in self._fields])
        if self._aggregating_func_per_field:
            return groups.agg({str(field): func for field, func in self._aggregating_func_per_field.items()})
        else:
            return groups


class _Raw_Operation(DataSetOperation):
    def __init__(self, operation):
        self._raw_op_str = operation

    def __repr__(self):
        return self._raw_op_str

    def apply(self, df, *args, **kw):
        # Raw operations not done here
        pass


class DataSetQ(object):
    def __init__(self, cur_dataq, available_fields=None, flat_structure=False):
        self._cur_dataset = cur_dataq
        self._op = None
        self._fields_avail = set(available_fields or schema._default_graph_fields)
        self._flat_structure = flat_structure

    def filter(self, field1, value_or_field, comparator="="):
        """ Filters the dataset according by a value or another field \n
        :param field1: The field to select, refer to :py:mod:`spykfunc.schema` classes.
        :param value_or_field: A literal value or another field.
        :param comparator: The comparator sign (default: '=')
        :return: The new dataset (lazy evaluated)
        """
        # Ensure fields exist
        assert field1 in self._fields_avail, Exception(
            "Please use a field from the available set:" + str(self._fields_avail))
        if isinstance(value_or_field, schema.Field):
            assert value_or_field in self._fields_avail, Exception(
                "Please use a field from the available set:" + str(self._fields_avail))
        # Create filter
        self._op = Filter(field1, value_or_field, comparator, self._flat_structure)
        return DataSetQ(self)

    def select(self, *fields):
        """Selects a subset of the fields from the current Dataframe
        """
        self._op = Projection(fields)
        return DataSetQ(self, self._flat_fields(fields), flat_structure=True)

    def groupBy(self, *grouping_fields, **aggregating_func_per_field):
        """Performs basic aggregation by a field
        """
        self._op = Grouping(grouping_fields, aggregating_func_per_field)
        return DataSetQ(self, self._fields_avail)  # todo: fields are kept if aggregation is not performed

    # Terminating queries -> return a value, not another DataSetQ
    def count(self):
        """ Counts the number of records from the dataframe \n
        :return: The dataframe record count
        """
        self._op = _Raw_Operation(".count()")
        return self._dataframe.count()

    def get_stats(self):
        """ Collects the statistics of the current dataframe (df.describe())
        """
        self._op = _Raw_Operation(".describe().collect()")
        return self._dataframe.describe()

    def show(self, n=10):
        """ Shows a subset of the records, by default first 10 \n
        :param n: The number of records to show
        """
        self._op = _Raw_Operation(".show(%d)" % (n,))
        return self._dataframe.show(n)

    def __repr__(self):
        op_repr = repr(self._op) if self._op else ""

        if isinstance(self._cur_dataset, DataSetQ):
            return repr(self._cur_dataset) + op_repr

        return "[DATA]" + op_repr

    def apply(self):
        """ perform the operatin on the dataset from the previous level
        """
        return self._op.apply(self._dataframe)

    # Except in the top-level, where the dataset is the dataframe, we must execute the DatasetQ operation to obtain it
    @property
    def _dataframe(self):
        if isinstance(self._cur_dataset, DataSetQ):
            return self._cur_dataset.apply()
        return self._cur_dataset

    @staticmethod
    def _flat_fields(fields):
        new_fields = []
        for field in fields:
            newf = copy.copy(field)
            newf.graph_entity = None
            new_fields.append(newf)
        return new_fields


# ------------------------------------------------
# TESTS
# ------------------------------------------------
class MockProp:
    def __init__(self, obj, name):
        self.obj = obj
        self.name = name

    def __call__(self, *args, **kwargs):
        self.obj.name += "." + self.name + "(" + ",".join(args) + ",".join(
            "%s=%s" % (key, val) for key, val in kwargs.items()) + ")"
        return self.obj

    def __str__(self):
        return self.obj.name + "." + self.name


class Mock:
    def __init__(self, curname):
        self.name = curname

    def __getattr__(self, name):
        return MockProp(self, name)

    def __str__(self):
        return self.name


if __name__ == "__main__":
    m = Mock("<dataset>")
    baseData = DataSetQ(m)
    q = baseData.filter(schema.PostPostNeuronFields.morphology, 1).groupBy(schema.PostNeuronFields.name)
    print(repr(q))
    print(str(q.count()))
