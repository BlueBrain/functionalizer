"""
Additional "Synapse property fields
Shall replace compute_additional_h5_fields in data_export
"""
from __future__ import absolute_import
from pyspark.sql import functions as F
from pyspark.sql import types as T
from . import filter_udfs
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# -----
def compute_additional_h5_fields(neuronG, syn_class_matrix, syn_props_df):
    touches = neuronG.find("(n1)-[t]->(n2)")
    syn_class_dims = syn_class_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]

    # Compute the index for the matrix as in a flat array
    touches = touches.withColumn("syn_prop_index",
                                 touches.n1.morphology_i * index_length[1] +
                                 touches.n1.electrophysiology * index_length[2] +
                                 touches.n1.syn_class_index * index_length[3] +
                                 touches.n2.morphology_i * index_length[4] +
                                 touches.n2.electrophysiology * index_length[5] +
                                 touches.n2.syn_class_index)

    to_syn_prop_i = filter_udfs.get_synapse_property_udf(syn_class_matrix)
    touches = touches.withColumn("syn_prop_i", to_syn_prop_i(touches.syn_prop_index))

    # According to benchmarks in pyspark, applying to_syn_prop_i (py) with 1000 N (2M touches) split by three cores takes 5 more seconds
    # That means that in average there's 5min overhead per 20k neurons per core

    touches = touches.drop("syn_prop_index")
    syn_props_df = syn_props_df.select(F.struct("*").alias("prop"))

    # Join with Syn Prop Class
    touches = touches.join(syn_props_df, touches.syn_prop_i == syn_props_df.prop._prop_i)

    # 0: Connecting gid: presynaptic for nrn.h5, postsynaptic for nrn_efferent.h5
    # 1: Axonal delay: computed using the distance of the presynaptic axon to the post synaptic terminal (milliseconds) (float)
    # 2: postSection ID (int)
    # 3: postSegment ID (int)
    # 4: The post distance (in microns) of the synapse from the begining of the post segment 3D point, or \-1 for soma connections  (float)
    # 5: preSection ID (int)
    # 6: preSegment ID (int)
    # 7: The pre distance (in microns) of the synapse from the begining of the pre segment  3D point (float)
    # 8: g_synX is the conductance of the synapse (nanosiemens) (float)
    # 9: u_syn is the u parameter in the TM model (0-1) (float)
    # 10: d_syn is the time constant of depression (milliseconds) (int)
    # 11: f_syn is the time constant of facilitation (milliseconds) (int)
    # 12: DTC - Decay Time Constant (milliseconds) (float)
    # 13: synapseType, the synapse type Inhibitory < 100 or Excitatory >= 100 (specific value corresponds to generating recipe)
    # 14: The morphology type of the pre neuron.  Index corresponds with circuit.mvd2
    # 15-16: BranchOrder of the dendrite, BranchOrder of the axon (int,int)
    # 17: ASE Absolute Synaptic Efficacy (Millivolts) (int)
    # 18: Branch Type from the post neuron(0 for soma,

    # Compute #1: delaySomaDistance
    touches = touches.withColumn("axional_delay", (
            touches.prop.neuralTransmitterReleaseDelay +
            touches.t.distance_soma / touches.prop.axonalConductionVelocity
        ).cast(T.FloatType())
    )

    # Compute #8-12
    # We ruse a Java UDFs (gauss_rand) which requires using spark.sql
    touches.createOrReplaceTempView("cur_touches")
    touches = spark.sql(
        "select *,"
        " gauss_rand(0) * prop.gsynVar + prop.gsyn as gsyn, "  # g
        " gauss_rand(0) * prop.uVar + prop.u as u,"     # u
        " gauss_rand(0) * prop.dVar + prop.d as d,"     # d
        " gauss_rand(0) * prop.fVar + prop.f as f,"     # f
        " gauss_rand(0) * prop.dtcVar + prop.dtc as dtc"  # dtc
        " from cur_touches")

    # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
    t = touches.withColumn("synapseType",
                           (F.when(touches.prop.type.substr(0, 1) == F.lit('E'), 100)
                            .otherwise(0)
                            ) + touches.prop._class_i)

    # Select fields
    return t.select(
        # Exported touch gids are 1-base, not 0
        (t.t.src + 1).alias("pre_gid"),
        (t.t.dst + 1).alias("post_gid"),
        t.axional_delay,
        t.t.post_section.alias("post_section"),
        t.t.post_segment.alias("post_segment"),
        t.t.post_offset.alias("post_offset"),
        t.t.pre_section.alias("pre_section"),
        t.t.pre_segment.alias("pre_segment"),
        t.t.pre_offset.alias("pre_offset"),
        "gsyn", "u", "d", "f", "dtc",
        t.synapseType.alias("synapseType"),
        t.n1.morphology_i.alias("morphology"),
        F.lit(0).alias("branch_order_dend"),  # TBD
        t.t.branch_order.alias("branch_order_axon"),
        t.prop.ase.alias("ase"),
        F.lit(0).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
    )
