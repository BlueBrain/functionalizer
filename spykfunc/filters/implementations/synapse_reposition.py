import fnmatch
import itertools

import sparkmanager as sm
from pyspark.sql import functions as F

from spykfunc import schema
from spykfunc.filters import DatasetOperation
from spykfunc.recipe import GenericProperty


class SynapsesReposition(GenericProperty):
    """Class representing rules to shift synapse positions"""

    _supported_attrs = {'fromMType', 'toMType', 'type'}
    fromMType = None
    toMType = None
    type = ""


class SynapseReposition(DatasetOperation):
    """Reposition synapses

    Shifts the post-section of synapses for ChC and SpAA cells to the soma
    according to the `SynapsesReposition` rules of the recipe.
    """

    def __init__(self, recipe, source, target, morphos):
        reposition = list(
            recipe.load_group(
                recipe.xml.find("SynapsesReposition"),
                SynapsesReposition,
                required=False
            )
        )

        self.reposition = self.convert_reposition(source, target, reposition)

    def apply(self, circuit):
        """Actually reposition the synapses
        """
        from spykfunc.synapse_properties import patch_ChC_SPAA_cells
        return patch_ChC_SPAA_cells(circuit.df,
                                    circuit.morphologies,
                                    self.reposition)

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
