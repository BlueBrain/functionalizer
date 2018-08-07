"""
Additional "Synapse property fields
"""
from __future__ import absolute_import
import pandas
import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .schema import touches_with_pathway, SYNAPSE_CLASS_MAP_SCHEMA as schema
from .utils.spark import cache_broadcast_single_part
from .random import RNGThreefry, gamma, poisson, truncated_normal


def compute_additional_h5_fields(circuit, reduced, syn_class_matrix, syn_props_df, seed):
    """Compute randomized properties of touches based on their classification.

    :param circuit: the full synapse connection circuit
    :param reduced: a reduced connection count with only one pair of src, dst each
    :param syn_class_matrix: a matrix associating connection properties with a class
    :param syn_props_df: a dataframe containing the properties of connection classes
    :param seed: the seed to use for the random numbers underlying the properties
    """
    syn_class_dims = syn_class_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]

    # Compute the index for the matrix as in a flat array
    connections = reduced.withColumn("syn_prop_index",
                                     reduced.src_mtype_i * index_length[1] +
                                     reduced.src_etype_i * index_length[2] +
                                     reduced.src_syn_class_i * index_length[3] +
                                     reduced.dst_mtype_i * index_length[4] +
                                     reduced.dst_etype_i * index_length[5] +
                                     reduced.dst_syn_class_i)

    # Convert the numpy matrix into a dataframe and join to get the right
    # property index
    nparts = 100
    rdd = sm.parallelize(enumerate(int(n) for n in syn_class_matrix.flatten()), nparts)
    syn_class = cache_broadcast_single_part(sm.createDataFrame(rdd, schema), parallelism=nparts)
    connections = connections.join(syn_class, "syn_prop_index").drop("syn_prop_index")

    # Join with Syn Prop Class
    syn_props_df = syn_props_df.alias("synprop")  # Synprops is globally cached and broadcasted
    connections = connections.join(F.broadcast(syn_props_df), connections.syn_prop_i == syn_props_df._prop_i)

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
    # 17: NRRP - Number of Readily Releasable Pool vesicles
    # 18: Branch Type from the post neuron(0 for soma,

    # Compute #8-12: g, u, d, f, dtc
    #
    # IDs for random functions need to be unique, hence the use of offset
    connections = _add_connection_properties(connections, seed)

    touches = circuit.alias("c").join(connections.alias("conn"),
                                      [F.col("c.src") == F.col("conn.src"), F.col("c.dst") == F.col("conn.dst")])

    # Compute #1: delaySomaDistance
    touches = touches.withColumn(
        "axonal_delay",
        F.expr("conn.neuralTransmitterReleaseDelay + distance_soma * conn.axonalConductionVelocity")
        .cast(T.FloatType())
    )

    # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
    t = touches.withColumn(
        "synapseType",
        (F.when(F.col("conn.type").substr(0, 1) == F.lit('E'), 100).otherwise(0) +
         F.col("conn._class_i")
         ).cast(T.ShortType())
    )

    # Select fields
    return t.select(
        F.col("c.src").alias("pre_gid"),
        F.col("c.dst").alias("post_gid"),
        t.axonal_delay,
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
        F.col("c.src_mtype_i").alias("morphology"),
        F.lit(0).alias("branch_order_dend"),  # TBD
        t.branch_order.alias("branch_order_axon"),
        t.rand_nrrp.alias("nrrp"),
        F.lit(0).alias("branch_type"),  # TBD (0 soma, 1 axon, 2 basel dendrite, 3 apical dendrite)
    )


def patch_ChC_SPAA_cells(circuit, morphology_db, pathways_to_patch):
    """Patches a circuit, fixing the touch post-segment of ChC and SPAA cells to axon
    """
    # isChCpre = neuronMap.getMTypeFromIndex(preMType).find( "ChC" )
    # isSP_AApre = neuronMap.getMTypeFromIndex(preMType).find("SP_AA")

    get_axon_section_id = _create_axon_section_udf(morphology_db)

    circuit = touches_with_pathway(circuit).join(pathways_to_patch, "pathway_i", "left_outer")

    patched_circuit = (
        circuit.withColumn(
            "new_post_section",
            F.when(circuit.reposition,
                   get_axon_section_id(circuit.dst_morphology_i))
             .otherwise(circuit.post_section)
        )
        .withColumn("new_post_segment",
                    F.when(circuit.reposition, 0).otherwise(circuit.post_segment))
        .withColumn("new_post_offset",
                    F.when(circuit.reposition, 0.5).otherwise(circuit.post_offset))
        .drop("post_section", "post_segment", "post_offset", "pathway_i", "reposition")
        .withColumnRenamed("new_post_section", "post_section")
        .withColumnRenamed("new_post_segment", "post_segment")
        .withColumnRenamed("new_post_offset", "post_offset")
    )

    return patched_circuit


def _add_connection_properties(connections, seed):
    """Add connection properties drawn from random distributions

    :param connections: A Spark dataframe holding synapse connections
    :return: The input dataframe with additonal property columns
    """
    def __generate(key, fct):
        @F.pandas_udf('float')
        def _udf(*args):
            rng = RNGThreefry().seed(seed)
            args = [a.values for a in args]
            return pandas.Series(fct(rng.derivate(key), *args))
        return _udf

    add = [
        ("gsyn", __generate(0x1001, gamma)),
        ("d", __generate(0x1002, gamma)),
        ("f", __generate(0x1003, gamma)),
        ("u", __generate(0x1004, truncated_normal)),
        ("dtc", __generate(0x1005, truncated_normal)),
        ("nrrp", __generate(0x1006, poisson))
    ]

    connections = connections.sortWithinPartitions("src")
    for n, f in add:
        args = [F.col("src"), F.col("dst"), F.col(n)]
        if n != "nrrp":
            args.append(F.col(n + "SD"))
        connections = connections.withColumn("rand_" + n, f(*args))
    return connections


def _create_axon_section_udf(morphology_db):
    """ Creates a UDF for a given morphologyDB that looks up
        the first axon section in a morphology given its name

    :param morphology_db: The morphology db
    :return: The first section index in the morphology
    """

    @F.udf(returnType=T.IntegerType())
    def get_axon_section_id(morpho):
        return morphology_db[morpho].first_axon_section

    return get_axon_section_id
