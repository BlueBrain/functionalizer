"""Filters reducing touches."""
from pyspark.sql import functions as F

import sparkmanager as sm

from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

from . import add_random_column


logger = get_logger(__name__)


_KEY_TOUCH = 0x202


class TouchReductionFilter(DatasetOperation):
    """Filter touches based on a simple probability.

    Defined in the recipe as `TouchReduction`, restrict connections
    according to the `survival_rate` defined.
    """

    def __init__(self, recipe, source, target, morphos):
        """Initilize the filter by parsing the recipe.

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        super().__init__(recipe, source, target, morphos)
        self.survival = recipe.touch_reduction.survival_rate
        self.seed = recipe.seeds.synapseSeed
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

    def __init__(self, recipe, source, target, morphos):
        """Initilize the filter by parsing the recipe.

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        super().__init__(recipe, source, target, morphos)
        self.rules = recipe.touch_rules.to_matrix(source.mtype_values, target.mtype_values)

    def apply(self, circuit):
        """Filter the circuit edges according to the touch rules."""
        indices = list(self.rules.shape)
        for i in reversed(range(len(indices) - 1)):
            indices[i] *= indices[i + 1]

        rdd = sm.parallelize(((i,) for i, v in enumerate(self.rules.flatten()) if v == 0), 200)
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
        if not hasattr(circuit.df, "efferent_section_type") or not hasattr(
            circuit.df, "afferent_section_type"
        ):
            raise RuntimeError("TouchRules need [ae]fferent_section_type")
        return (
            touches.withColumn(
                "fail",
                touches.src_mtype_i * indices[1]
                + touches.dst_mtype_i * indices[2]
                + touches.efferent_section_type * indices[3]
                + touches.afferent_section_type,
            )
            .join(F.broadcast(rules), "fail", "left_anti")
            .drop("fail", *added)
        )
