"""A default filter plugin
"""
import fnmatch
import numpy as np
import sparkmanager as sm

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty, Recipe
from spykfunc.utils import get_logger

logger = get_logger(__name__)


class TouchRule(GenericProperty):
    """Class representing a Touch rule"""

    _supported_attrs = {'fromMType', 'toMType', 'type'}
    fromMType = None
    toMType = None
    type = ""

    @classmethod
    def load(cls, xml, mtypes, *, strict=False):
        """Construct touch rule matrix

        Args:
            xml: The raw recipe XML to extract the data from
            mtypes: The morphology types associated with the circuit
            strict: Raise a ValueError a morphology type is not covered by the rules
        Returns:
            A multidimensional matrix, containing a one (1) for every
            connection allowed. The dimensions correspond to the numeical
            indices of morphology types of source and destination, as well
            as the rule type.
        """
        mtype_rev = {name: i for i, name in enumerate(mtypes)}

        # dendrite mapping here is for historical purposes only, when we
        # distinguished only between soma and !soma.
        type_map = {
            '*': [0, 1, 2, 3],
            'soma': [0],
            'dendrite': [1, 2, 3],
            'basal': [2],
            'apical': [3]
        }

        touch_rule_matrix = np.zeros(
            shape=(len(mtype_rev), len(mtype_rev), 4),
            dtype="uint8"
        )

        covered_types = set()

        for rule in Recipe.load_group(xml.find("TouchRules"), cls):
            t1s = [slice(None)]
            t2s = [slice(None)]
            if rule.fromMType:
                t1s = [mtype_rev[m] for m in fnmatch.filter(mtypes, rule.fromMType)]
                covered_types.update(t1s)
            if rule.toMType:
                t2s = [mtype_rev[m] for m in fnmatch.filter(mtypes, rule.toMType)]
                covered_types.update(t2s)
            rs = type_map[rule.type] if rule.type else [slice(None)]

            for t1 in t1s:
                for t2 in t2s:
                    for r in rs:
                        touch_rule_matrix[t1, t2, r] = 1

        not_covered = set(mtype_rev.values()) - covered_types
        if not_covered:
            msg = "No touch rules are covering: %s", ", ".join(mtypes[i] for i in not_covered)
            if strict:
                raise ValueError(msg)
            else:
                logger.warning(msg)
                logger.warning("All corresponding touches will be trimmed!")

        return touch_rule_matrix


class TouchRulesFilter(DatasetOperation):
    """Filter touches based on recipe rules

    Defined in the recipe as `TouchRules`, restrict connections between
    mtypes and types (dendrite/soma).  Any touches not allowed are removed.

    This filter is deterministic.
    """

    _checkpoint = True

    def __init__(self, recipe, neurons, morphos):
        """Initilize the filter by parsing the recipe

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        self.rules = TouchRule.load(recipe.xml, neurons.mTypes)

    def apply(self, circuit):
        """ .apply() method (runner) of the filter
        """
        indices = list(self.rules.shape)
        for i in reversed(range(len(indices) - 1)):
            indices[i] *= indices[i + 1]

        rdd = sm.parallelize(((i,)
                              for i, v in enumerate(self.rules.flatten())
                              if v == 0), 200)
        rules = sm.createDataFrame(rdd, schema=["fail"])
        # For each neuron we require:
        # - preMType
        # - postMType
        # - postBranchType
        #
        # The first four fields are properties of the neurons, part of
        # neuronDF, while postBranchType is a property if the touch,
        # historically checked by the index of the target neuron
        # section (0->soma)
        touches = circuit.df
        if not hasattr(circuit.df, 'branch_type'):
            touches = touches.withColumn('branch_type',
                                         (touches.post_section > 0).cast('integer'))
        return touches.withColumn("fail",
                                  touches.src_mtype_i * indices[1] +
                                  touches.dst_mtype_i * indices[2] +
                                  touches.branch_type) \
                      .join(F.broadcast(rules), "fail", "left_anti") \
                      .drop("fail")
