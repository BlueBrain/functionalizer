"""A default filter plugin
"""
import fnmatch
import numpy as np
import sparkmanager as sm

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty


class TouchRule(GenericProperty):
    """Class representing a Touch rule"""

    _supported_attrs = {'fromLayer', 'toLayer', 'fromMType', 'toMType', 'type'}
    fromLayer = None
    toLayer = None
    fromMType = None
    toMType = None
    type = ""


class TouchRulesFilter(DatasetOperation):
    """Filter touches based on recipe rules

    Defined in the recipe as `TouchRules`, restrict connections between
    mtypes, types (dendrite/soma), and layers.  Any touches not allowed are
    removed.

    This filter is deterministic.
    """

    _checkpoint = True

    def __init__(self, recipe, morphos, stats):
        """Initilize the filter by parsing the recipe

        The rules stored in the recipe are loaded in their abstract form,
        concretization will happen with the acctual circuit.
        """
        self.rules = list(
            recipe.load_group(
                recipe.xml.find("TouchRules"),
                TouchRule
            )
        )

    def concretize_rules(self, circuit):
        """Construct the conctrete form of the touch rules

        Use information of the circuit to construct a rule matrix out of
        the recipe information stored within the filter.
        """
        layers = circuit.layers
        layers_rev = {layer: i for i, layer in enumerate(layers)}

        mtype = circuit.morphology_types
        mtype_rev = {name: i for i, name in enumerate(mtype)}

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
            shape=(len(layers_rev), len(layers_rev),
                   len(mtype_rev), len(mtype_rev), 4),
            dtype="uint8"
        )

        for rule in self.rules:
            l1 = layers_rev[rule.fromLayer] if rule.fromLayer else slice(None)
            l2 = layers_rev[rule.toLayer] if rule.toLayer else slice(None)
            t1s = [slice(None)]
            t2s = [slice(None)]
            if rule.fromMType:
                t1s = [mtype_rev[m] for m in fnmatch.filter(mtype, rule.fromMType)]
            if rule.toMType:
                t2s = [mtype_rev[m] for m in fnmatch.filter(mtype, rule.toMType)]
            rs = type_map[rule.type] if rule.type else [slice(None)]

            for t1 in t1s:
                for t2 in t2s:
                    for r in rs:
                        touch_rule_matrix[l1, l2, t1, t2, r] = 1
        return touch_rule_matrix

    def apply(self, circuit):
        """ .apply() method (runner) of the filter
        """
        touch_rules = self.concretize_rules(circuit)

        indices = list(touch_rules.shape)
        for i in reversed(range(len(indices) - 1)):
            indices[i] *= indices[i + 1]

        rdd = sm.parallelize(((i,)
                              for i, v in enumerate(touch_rules.flatten())
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
