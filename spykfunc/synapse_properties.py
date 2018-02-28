"""
Additional "Synapse property fields
"""
from __future__ import absolute_import
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext

# -----
def compute_additional_h5_fields(circuit, syn_class_matrix, syn_props_df):
    neurons = circuit.neurons
    touches = circuit.touches

    def prefixed(pre):
        tmp = neurons
        for col in tmp.schema.names:
            tmp = tmp.withColumnRenamed(col, pre if col == "id" else "{}_{}".format(pre, col))
        return tmp

    touches = touches.alias("t") \
        .join(prefixed("src"), "src") \
        .join(prefixed("dst"), "dst")

    syn_class_dims = syn_class_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]

    # Compute the index for the matrix as in a flat array
    touches = touches.withColumn("syn_prop_index",
                                 touches.src_morphology_i * index_length[1] +
                                 touches.src_electrophysiology * index_length[2] +
                                 touches.src_syn_class_index * index_length[3] +
                                 touches.dst_morphology_i * index_length[4] +
                                 touches.dst_electrophysiology * index_length[5] +
                                 touches.dst_syn_class_index)

    # Get the rule index from the numpy matrix
    # According to benchmarks in pyspark, applying to_syn_prop_i (py) with 1000 N (2M touches) split by three cores
    # takes 5 more seconds. That means that in average there's 5min overhead per 20k neurons per core
    to_syn_prop_i = get_synapse_property_udf(syn_class_matrix)
    touches = touches.withColumn("syn_prop_i", to_syn_prop_i(touches.syn_prop_index))

    touches = touches.drop("syn_prop_index")
    syn_props_df = syn_props_df.select(F.struct("*").alias("prop"))

    # Join with Syn Prop Class
    touches = touches.join(syn_props_df, touches.syn_prop_i == syn_props_df.prop._prop_i)

    # 0: Connecting gid: presynaptic for nrn.h5, postsynaptic for nrn_efferent.h5
    # 1: Axonal delay: computed using the distance of the presynaptic axon to the post synaptic terminal (milliseconds)
    # 2: postSection ID (int)
    # 3: postSegment ID (int)
    # 4: The post distance (in microns) of the synapse from the begining of the post segment 3D point, or \-1 for soma
    # 5: preSection ID (int)
    # 6: preSegment ID (int)
    # 7: The pre distance (in microns) of the synapse from the begining of the pre segment  3D point (float)
    # 8: g_synX is the conductance of the synapse (nanosiemens) (float)
    # 9: u_syn is the u parameter in the TM model (0-1) (float)
    # 10: d_syn is the time constant of depression (milliseconds) (int)
    # 11: f_syn is the time constant of facilitation (milliseconds) (int)
    # 12: DTC - Decay Time Constant (milliseconds) (float)
    # 13: synapseType, the synapse type Inhibitory < 100 or Excitatory >= 100 (specific value depends recipe)
    # 14: The morphology type of the pre neuron.  Index corresponds with circuit.mvd2
    # 15-16: BranchOrder of the dendrite, BranchOrder of the axon (int,int)
    # 17: ASE Absolute Synaptic Efficacy (Millivolts) (int)
    # 18: Branch Type from the post neuron(0 for soma,

    # Compute #1: delaySomaDistance
    touches = touches.withColumn("axional_delay",
        (
            touches.prop.neuralTransmitterReleaseDelay +
            touches.distance_soma / touches.prop.axonalConductionVelocity
        ).cast(T.FloatType())
    )

    # Compute #8-12: g, u, d, f, dtc
    touches = touches.selectExpr(
        "*",
        "gauss_rand(0) * prop.gsynVar + prop.gsyn as gsyn",
        "gauss_rand(0) * prop.uVar + prop.u as u",
        "gauss_rand(0) * prop.dVar + prop.d as d",
        "gauss_rand(0) * prop.fVar + prop.f as f",
        "gauss_rand(0) * prop.dtcVar + prop.dtc as dtc"
    )

    # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
    t = touches.withColumn("synapseType",
                           (F.when(touches.prop.type.substr(0, 1) == F.lit('E'), 100)
                            .otherwise(0)
                            ) + touches.prop._class_i)

    # Select fields
    return t.select(
        # Exported touch gids are 1-base, not 0
        (t.src + 1).alias("pre_gid"),
        (t.dst + 1).alias("post_gid"),
        t.axional_delay,
        t.post_section.alias("post_section"),
        t.post_segment.alias("post_segment"),
        t.post_offset.alias("post_offset"),
        t.pre_section.alias("pre_section"),
        t.pre_segment.alias("pre_segment"),
        t.pre_offset.alias("pre_offset"),
        "gsyn", "u", "d", "f", "dtc",
        t.synapseType.alias("synapseType"),
        t.src_morphology_i.alias("morphology"),
        F.lit(0).alias("branch_order_dend"),  # TBD
        t.branch_order.alias("branch_order_axon"),
        t.prop.ase.alias("ase"),
        F.lit(0).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
    )
    
    
# *********************************************************
# Synapse classification UDF
# *********************************************************
def get_synapse_property_udf(syn_class_matrix, sc=None):
    if sc is None:
        sc = SparkContext.getOrCreate()

    # We need the matrix in all nodes, flattened
    syn_class_matrix_flat = sc.broadcast(syn_class_matrix.flatten())

    def syn_prop_udf(syn_prop_index):
        # Leaves are still tuple size2
        return syn_class_matrix_flat.value[syn_prop_index].tolist()

    return F.udf(syn_prop_udf, T.ShortType())
