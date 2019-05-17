"""A default filter plugin
"""
import sparkmanager as sm
from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation


class TouchRulesFilter(DatasetOperation):
    """Filter touches based on recipe rules

    Defined in the recipe as `TouchRules`, restrict connections between
    mtypes, types (dendrite/soma), and layers.  Any touches not allowed are
    removed.

    This filter is deterministic.
    """

    _checkpoint = True

    def apply(self, circuit):
        """ .apply() method (runner) of the filter
        """
        indices = list(circuit.touch_rules.shape)
        for i in reversed(range(len(indices) - 1)):
            indices[i] *= indices[i + 1]

        rdd = sm.parallelize(((i,)
                              for i, v in enumerate(circuit.touch_rules.flatten())
                              if v == 0), 200)
        rules = sm.createDataFrame(rdd, schema=["fail"])
        # For each neuron we require:
        # - preLayer
        # - preMType
        # - postLayer
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
                                  touches.src_layer_i * indices[1] +
                                  touches.dst_layer_i * indices[2] +
                                  touches.src_mtype_i * indices[3] +
                                  touches.dst_mtype_i * indices[4] +
                                  touches.branch_type) \
                      .join(F.broadcast(rules), "fail", "left_anti") \
                      .drop("fail")
