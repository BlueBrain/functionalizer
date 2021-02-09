"""A default filter plugin
"""
import fnmatch
import numpy as np
import sparkmanager as sm

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

from . import add_random_column


logger = get_logger(__name__)


_KEY_TOUCH = 0x202


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
        self.survival = recipe.touch_reduction.survival_rate
        self.seed = recipe.seeds.synapseSeed
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
        self.rules = recipe.touch_rules.to_matrix(source.mtype_values, target.mtype_values)

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
