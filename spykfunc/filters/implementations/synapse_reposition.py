import fnmatch
import itertools

import numpy
import pandas

import sparkmanager as sm
from pyspark.sql import functions as F

from spykfunc import schema
from spykfunc.filters import DatasetOperation
from spykfunc.recipe import Attribute, GenericProperty


class SynapsesReposition(GenericProperty):
    """Class representing rules to shift synapse positions"""

    attributes = [
        Attribute("fromMtype", default="*", kind=str),
        Attribute("toMtype", default="*", kind=str),
        Attribute("type", default="*", kind=str)
    ]


class SynapseReposition(DatasetOperation):
    """Reposition synapses

    Shifts the post-section of synapses for ChC and SpAA cells to the soma
    according to the `SynapsesReposition` rules of the recipe.
    """

    _required = False

    def __init__(self, recipe, source, target, morphos):
        reposition = SynapsesReposition.load(recipe.xml)
        self.reposition = self.convert_reposition(source, target, reposition)

    def apply(self, circuit):
        """Actually reposition the synapses
        """
        get_axon_section_id = _create_axon_section_udf(circuit.morphologies)

        circuit = (
            schema
            .touches_with_pathway(circuit.df)
            .join(self.reposition, "pathway_i", "left_outer")
        )

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

    @staticmethod
    def convert_reposition(source, target, reposition):
        """Loader for pathways that need synapses to be repositioned
        """
        src_mtype = source.mtypes
        src_mtype_rev = {name: i for i, name in enumerate(src_mtype)}
        dst_mtype = target.mtypes
        dst_mtype_rev = {name: i for i, name in enumerate(dst_mtype)}

        paths = []
        for shift in reposition:
            src = src_mtype_rev.values()
            dst = dst_mtype_rev.values()
            if shift.type != 'AIS':
                continue
            if shift.fromMType:
                src = [src_mtype_rev[m] for m in fnmatch.filter(src_mtype, shift.fromMType)]
            if shift.toMType:
                dst = [dst_mtype_rev[m] for m in fnmatch.filter(dst_mtype, shift.toMType)]
            paths.extend(itertools.product(src, dst))
        pathways = sm.createDataFrame([((s << 16) | d, True) for s, d in paths], schema.SYNAPSE_REPOSITION_SCHEMA)
        return F.broadcast(pathways)


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
