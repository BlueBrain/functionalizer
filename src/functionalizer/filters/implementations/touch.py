"""Filters reducing touches."""

import numpy as np
import pandas as pd
import sparkmanager as sm
from pyspark.sql import functions as F

from functionalizer.filters import DatasetOperation
from functionalizer.utils import get_logger

from . import add_random_column

logger = get_logger(__name__)


_KEY_TOUCH = 0x202


class TouchReductionFilter(DatasetOperation):
    """Filter touches based on a simple probability.

    Defined in the recipe as `TouchReduction`, restrict connections
    according to the `survival_rate` defined.
    """

    def __init__(self, recipe, source, target):
        """Initilize the filter by parsing the recipe.

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        super().__init__(recipe, source, target)
        self.survival = recipe.get("touch_reduction.survival_rate")
        self.seed = recipe.get("seed")
        logger.info("Using seed %d for trimming touches", self.seed)

    def apply(self, circuit):
        """Actually reduce the touches of the circuit."""
        touches = add_random_column(circuit.df, "touch_rand", self.seed, _KEY_TOUCH)

        return touches.where(F.col("touch_rand") <= F.lit(self.survival)).drop("touch_rand")


class TouchRulesFilter(DatasetOperation):
    """Filter touches based on recipe rules.

    Defined in the recipe as `TouchRules`, restrict connections between
    mtypes and types (dendrite/soma).  Any touches not allowed are removed.

    This filter is deterministic.
    """

    _checkpoint = True

    def __init__(self, recipe, source, target):
        """Initilize the filter by parsing the recipe.

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        super().__init__(recipe, source, target)
        self.columns, self.rules = recipe.as_matrix("touch_rules")
        self.unset_value = len(recipe.get("touch_rules"))

    def apply(self, circuit):
        """Filter the circuit edges according to the touch rules."""
        fail_column = F.lit(0)
        for col, factor in zip(self.columns, self.rules.shape):
            fail_column *= factor
            fail_column += F.col(col)
        rules = sm.createDataFrame(
            pd.DataFrame({"fail": np.nonzero(self.rules.flatten() == self.unset_value)[0]})
        )
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
        touches = circuit.df
        if not hasattr(circuit.df, "efferent_section_type") or not hasattr(
            circuit.df, "afferent_section_type"
        ):
            raise RuntimeError("TouchRules need [ae]fferent_section_type")
        return (
            touches.withColumn("fail", fail_column)
            .join(F.broadcast(rules), "fail", "left_anti")
            .drop("fail")
        )
