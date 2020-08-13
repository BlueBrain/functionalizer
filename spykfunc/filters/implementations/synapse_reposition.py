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
        Attribute("fromMType", default="*", kind=str),
        Attribute("toMType", default="*", kind=str),
        Attribute("type", default="*", kind=str)
    ]

    group_name = "SynapsesReposition"


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
        axon_shift = _create_axon_section_udf(circuit.morphologies)

        patched = (
            schema
            .touches_with_pathway(circuit.df)
            .join(self.reposition, "pathway_i", "left_outer")
            .mapInPandas(axon_shift, circuit.df.schema)
            .drop("pathway_i", "reposition")
        )

        return patched

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
    :return: a function than can be used by ``mapInPandas`` to shift synapses
    """
    def _shift_to_axon_section(dfs):
        """Shifts synapses to the first axon section
        """
        for df in dfs:
            set_section_fraction = "post_section_fraction" in df.columns
            set_soma_distance = "distance_soma" in df.columns
            for i in numpy.nonzero(df.reposition.values)[0]:
                morpho = df.dst_morphology.iloc[i]
                (idx, dist, frac, soma) = morphology_db.first_axon_section(morpho)
                df.post_branch_type = 1  # Axon. Soma is 0, dendrites are higher
                df.post_offset.iloc[i] = dist
                df.post_section.iloc[i] = idx
                df.post_segment.iloc[i] = 0  # First segment on the axon
                if set_section_fraction:
                    df.post_section_fraction.iloc[i] = frac
                if set_soma_distance:
                    df.distance_soma.iloc[i] = soma
            yield df
    return _shift_to_axon_section
