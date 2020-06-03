"""A default filter plugin
"""
import fnmatch
import numpy as np
import sparkmanager as sm

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.recipe import Attribute, GenericProperty, Recipe
from spykfunc.utils import get_logger

from . import Seeds, add_random_column


logger = get_logger(__name__)


_KEY_TOUCH = 0x202


class TouchReduction(GenericProperty):
    """Class representing a Touch filter"""

    attributes = [
        Attribute(name="survival_rate", kind=float)
    ]

    singleton = True


class TouchReductionFilter(DatasetOperation):
    """Filter touches based on a simple probability

    Defined in the recipe as `TouchReduction`, restrict connections
    according to the `survival_rate` defined.
    """

    def __init__(self, recipe, source, target, morphos):
        """Initilize the filter by parsing the recipe

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        self.survival = TouchReduction.load(recipe.xml).survival_rate
        self.seed = Seeds.load(recipe.xml).synapseSeed
        logger.info("Using seed %d for trimming touches", self.seed)

    def apply(self, circuit):
        """Actually reduce the touches of the circuit
        """
        touches = add_random_column(
            circuit.df,
            "touch_rand",
            self.seed,
            _KEY_TOUCH,
            F.col("synapse_id")
        )

        return (
            touches
            .where(F.col("touch_rand") <= F.lit(self.survival))
            .drop("touch_rand")
        )


class TouchRule(GenericProperty):
    """Class representing a Touch rule"""

    attributes = [
        Attribute("fromMType", default="*"),
        Attribute("toMType", default="*"),
        Attribute("fromBranchType", default="*"),
        Attribute("toBranchType", alias="type", default="*"),
    ]

    group_name = "TouchRules"

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
            'axon': [1],
            'dendrite': [2, 3],
            'basal': [2],
            'apical': [3]
        }

        touch_rule_matrix = np.zeros(
            shape=(len(src_mtype_rev), len(dst_mtype_rev), 4, 4),
            dtype="uint8"
        )

        src_covered = set()
        dst_covered = set()

        for rule in GenericProperty.load.__func__(cls, xml):
            t1s = [slice(None)]
            t2s = [slice(None)]
            if rule.fromMType:
                t1s = [src_mtype_rev[m] for m in fnmatch.filter(src_mtypes, rule.fromMType)]
                src_covered.update(t1s)
                if len(t1s) == 0:
                    logger.warning(f"Touch rules can't match fromMType='{rule.fromMType}'")
            if rule.toMType:
                t2s = [dst_mtype_rev[m] for m in fnmatch.filter(dst_mtypes, rule.toMType)]
                dst_covered.update(t2s)
                if len(t2s) == 0:
                    logger.warning(f"Touch rules can't match toMType='{rule.toMType}'")

            r1s = type_map[rule.fromBranchType] if rule.fromBranchType else [slice(None)]
            r2s = type_map[rule.toBranchType] if rule.toBranchType else [slice(None)]

            for t1 in t1s:
                for t2 in t2s:
                    for r1 in r1s:
                        for r2 in r2s:
                            touch_rule_matrix[t1, t2, r1, r2] = 1

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
        # - preBranchType
        # - postBranchType
        #
        # The first four fields are properties of the neurons, part of
        # neuronDF, while postBranchType is a property if the touch,
        # historically checked by the index of the target neuron
        # section (0->soma)
        added = []
        touches = circuit.df
        if not hasattr(circuit.df, 'pre_branch_type'):
            logger.warning(f"Guessing pre-branch type for touch rules!")
            touches = (
                touches
                .withColumn('pre_branch_type',
                            (touches.pre_section > 0).cast('integer') * 2)
            )
            added.append("pre_branch_type")
        if not hasattr(circuit.df, 'post_branch_type'):
            if 'branch_type' in touches.columns:
                touches = (
                    touches
                    .withColumnRenamed('branch_type', 'post_branch_type')
                )
            else:
                logger.warning(f"Guessing post-branch type for touch rules!")
                touches = (
                    touches
                    .withColumn('post_branch_type',
                                (touches.post_section > 0).cast('integer') * 2)
                )
            added.append("post_branch_type")
        return touches.withColumn("fail",
                                  touches.src_mtype_i * indices[1] +
                                  touches.dst_mtype_i * indices[2] +
                                  touches.pre_branch_type * indices[3] +
                                  touches.post_branch_type) \
                      .join(F.broadcast(rules), "fail", "left_anti") \
                      .drop("fail", *added)
