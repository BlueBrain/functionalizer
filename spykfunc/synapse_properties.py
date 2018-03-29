"""
Additional "Synapse property fields
"""
from __future__ import absolute_import
from pyspark.sql import functions as F
from pyspark.sql import types as T
import sparkmanager as sm

from .schema import SYNAPSE_CLASS_MAP_SCHEMA as schema


def compute_additional_h5_fields(circuit, syn_class_matrix, syn_props_df):
    syn_class_dims = syn_class_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]

    # Compute the index for the matrix as in a flat array
    touches = circuit.withColumn("syn_prop_index",
                                 circuit.src_morphology_i * index_length[1] +
                                 circuit.src_electrophysiology * index_length[2] +
                                 circuit.src_syn_class_index * index_length[3] +
                                 circuit.dst_morphology_i * index_length[4] +
                                 circuit.dst_electrophysiology * index_length[5] +
                                 circuit.dst_syn_class_index)

    # Convert the numpy matrix into a dataframe and join to get the right
    # property index
    rdd = sm.parallelize(enumerate(int(n) for n in syn_class_matrix.flatten()), 200)
    syn_class = sm.createDataFrame(rdd, schema).cache()
    touches = touches.join(F.broadcast(syn_class), "syn_prop_index").drop("syn_prop_index")

    # Join with Syn Prop Class
    syn_props_df = syn_props_df.alias("synprop")  # Synprops is globally cached and broadcasted
    touches = touches.join(syn_props_df, touches.syn_prop_i == syn_props_df._prop_i)

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
    touches = touches.withColumn(
        "axional_delay",
        F.expr("synprop.neuralTransmitterReleaseDelay + distance_soma  / synprop.axonalConductionVelocity")
        .cast(T.FloatType())
    )

    # Compute #8-12: g, u, d, f, dtc
    touches = touches.selectExpr(
        "*",
        "gauss_rand(0) * synprop.gsynSD + synprop.gsyn as rand_gsyn",
        "gauss_rand(0) * synprop.uSD + synprop.u as rand_u",
        "gauss_rand(0) * synprop.dSD + synprop.d as rand_d",
        "gauss_rand(0) * synprop.fSD + synprop.f as rand_f",
        "gauss_rand(0) * synprop.dtcSD + synprop.dtc as rand_dtc"
    )

    # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
    t = touches.withColumn("synapseType",
                           (F.when(F.col("synprop.type").substr(0, 1) == F.lit('E'), 100)
                            .otherwise(0)) +
                           F.col("synprop._class_i"))

    # Select fields
    return t.select(
        # Exported touch gids are 1-base, not 0
        t.src.alias("pre_gid"),
        t.dst.alias("post_gid"),
        t.axional_delay,
        t.post_section.alias("post_section"),
        t.post_segment.alias("post_segment"),
        t.post_offset.alias("post_offset"),
        t.pre_section.alias("pre_section"),
        t.pre_segment.alias("pre_segment"),
        t.pre_offset.alias("pre_offset"),
        t.rand_gsyn.alias("gsyn"),
        t.rand_u.alias("u"),
        t.rand_d.alias("d"),
        t.rand_f.alias("f"),
        t.rand_dtc.alias("dtc"),
        t.synapseType.alias("synapseType"),
        t.src_morphology_i.alias("morphology"),
        F.lit(0).alias("branch_order_dend"),  # TBD
        t.branch_order.alias("branch_order_axon"),
        t.ase.alias("ase"),
        F.lit(0).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
    )
