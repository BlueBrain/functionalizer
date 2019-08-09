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
    def load(cls, xml, src_mtypes, dst_mtypes, *, strict=False):
        """Construct touch rule matrix

        Args:
            xml: The raw recipe XML to extract the data from
            src_mtypes: The morphology types associated with the source population
            dst_mtypes: The morphology types associated with the target population
            strict: Raise a ValueError a morphology type is not covered by the rules
        Returns:
            A multidimensional matrix, containing a one (1) for every
            connection allowed. The dimensions correspond to the numeical
            indices of morphology types of source and destination, as well
            as the rule type.
        """
        src_mtype_rev = {name: i for i, name in enumerate(src_mtypes)}
        dst_mtype_rev = {name: i for i, name in enumerate(dst_mtypes)}

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
            shape=(len(src_mtype_rev), len(dst_mtype_rev), 4),
            dtype="uint8"
        )

        src_covered = set()
        dst_covered = set()

        for rule in Recipe.load_group(xml.find("TouchRules"), cls):
            t1s = [slice(None)]
            t2s = [slice(None)]
            if rule.fromMType:
                t1s = [src_mtype_rev[m] for m in fnmatch.filter(src_mtypes, rule.fromMType)]
                src_covered.update(t1s)
                if len(t1s) == 0:
                    logger.warn(f"Touch rules can't match fromMType='{rule.fromMType}'")
            if rule.toMType:
                t2s = [dst_mtype_rev[m] for m in fnmatch.filter(dst_mtypes, rule.toMType)]
                dst_covered.update(t2s)
                if len(t2s) == 0:
                    logger.warn(f"Touch rules can't match toMType='{rule.toMType}'")
            rs = type_map[rule.type] if rule.type else [slice(None)]

            for t1 in t1s:
                for t2 in t2s:
                    for r in rs:
                        touch_rule_matrix[t1, t2, r] = 1

        not_covered = set()
        for  v in set(src_mtype_rev.values()) - src_covered:
            not_covered.add(src_mtypes[v])
        for  v in set(dst_mtype_rev.values()) - dst_covered:
            not_covered.add(dst_mtypes[v])
        if not_covered:
            msg = "No touch rules are covering: " + ", ".join(not_covered)
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

    def __init__(self, recipe, source, target, morphos):
        """Initilize the filter by parsing the recipe

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        self.rules = TouchRule.load(recipe.xml, source.mtypes, target.mtypes)

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
