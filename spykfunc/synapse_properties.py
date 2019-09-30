"""
Additional "Synapse property fields
"""
from __future__ import absolute_import
import numpy
import pandas
import sparkmanager as sm
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .schema import touches_with_pathway, SYNAPSE_CLASS_MAP_SCHEMA as schema
from .utils.spark import cache_broadcast_single_part
from .random import RNGThreefry, gamma, poisson, truncated_normal


def compute_additional_h5_fields(circuit, reduced, classification_matrix, properties_df, seed):
    """Compute randomized properties of touches based on their classification.

    :param circuit: the full synapse connection circuit
    :param reduced: a reduced connection count with only one pair of src, dst each
    :param classification_matrix: a matrix associating connection properties with a class
    :param properties_df: a dataframe containing the properties of connection classes
    :param seed: the seed to use for the random numbers underlying the properties
    """
    syn_class_dims = classification_matrix.shape  # tuple of len 6

    index_length = list(syn_class_dims)
    for i in reversed(range(len(index_length) - 1)):
        index_length[i] *= index_length[i + 1]


    # Compute the index for the matrix as in a flat array
    connections = reduced.withColumn("classification_index",
                                     reduced.src_mtype_i * index_length[1] +
                                     reduced.src_etype_i * index_length[2] +
                                     reduced.src_syn_class_i * index_length[3] +
                                     reduced.dst_mtype_i * index_length[4] +
                                     reduced.dst_etype_i * index_length[5] +
                                     reduced.dst_syn_class_i)

    # Convert the numpy matrix into a dataframe and join to get the right
    # property index
    nparts = 100
    rdd = sm.parallelize(enumerate(int(n) for n in classification_matrix.flatten()), nparts)
    syn_class = cache_broadcast_single_part(sm.createDataFrame(rdd, schema), parallelism=nparts)
    connections = connections.join(syn_class, "classification_index").drop("classification_index")

    # Join with Syn Prop Class
    properties_df = properties_df.alias("synprop")  # Synprops is globally cached and broadcasted
    connections = connections.join(
        F.broadcast(properties_df),
        connections.classification_i == properties_df._class_i
    )

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

    # Identical columns should be dropped later
    overlap = set(connections.columns) & set(circuit.columns)
    for col in overlap:
        connections = connections.withColumnRenamed(col, f"_{col}")

    touches = (
        circuit
        .alias("c")
        .join(connections.alias("conn"),
              [F.col("c.src") == F.col("_src"),
               F.col("c.dst") == F.col("_dst")])
        .drop(*[f"_{col}" for col in overlap])
    )

    # Compute #1: delaySomaDistance
    touches = touches.withColumn(
        "axonal_delay",
        F.expr("conn.neuralTransmitterReleaseDelay + distance_soma / conn.axonalConductionVelocity")
        .cast(T.FloatType())
    )

    # Compute #13: synapseType:  Inhibitory < 100 or  Excitatory >= 100
    t = touches.withColumn(
        "synapseType",
        (F.when(F.col("conn.type").substr(0, 1) == F.lit('E'), 100).otherwise(0) +
         F.col("conn._prop_i")
         ).cast(T.ShortType())
    )

    # Required for SONATA support
    if not hasattr(t, "synapse_type_id"):
        t = t.withColumn(
            "synapse_type_id",
            F.lit(0)
        )

    return t


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
             get_axon_section_id(circuit.reposition,
                                 circuit.post_section,
                                 circuit.dst_morphology)
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

    The input parameters for the properties will be removed, the output
    columns may retain the same name, though.

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
        args = [F.col("synapse_id"), F.col(n)]
        temp = [n]
        if n != "nrrp":
            args.append(F.col(n + "SD"))
            temp.append(n + "SD")
        connections = (
            connections
            .withColumn(f"rand_{n}", f(*args))
            .drop(*temp)
            .withColumnRenamed(f"rand_{n}", n)
        )
    return connections


def _create_axon_section_udf(morphology_db):
    """ Creates a UDF for a given morphologyDB that looks up
        the first axon section in a morphology given its name

    :param morphology_db: the morphology db
    :return: a UDF to shift the post section of some morphology types
    """
    @F.pandas_udf('integer')
    def shift_axon_section_id(reposition, defaults, morphos):
        """Shift the axon sections for morphologies flagged.

        :param reposition: flag to indicate if a shift is needed
        :param defaults: post section to use when no shift is required
        :param morphos: morphology types to get the axon section for
        """
        sections = numpy.array(defaults.values, copy=True)
        for i, (shift, morpho) in enumerate(zip(reposition, morphos)):
            if shift:
                sections[i] = morphology_db.first_axon_section(morpho)
        return pandas.Series(data=sections)
    return shift_axon_section_id
