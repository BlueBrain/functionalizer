"""
Assign spine morphologies to synapses for all neurons in a circuit
"""

from pathlib import Path

import numpy as np
import pandas as pd

from pyspark.sql import functions as F

from spykfunc.filters import DatasetOperation


class SpineMorphologies(DatasetOperation):
    """Map synapses to the spine that is the closest in length

    Following `this paper <https://pubmed.ncbi.nlm.nih.gov/28721455/>`_ for the recipe:

    * 90% of assumed excitatory synapses are on spines.
    * Only 6% of spines have more than one synapse.
    * ~70% of inhibitory synapses on dendritic shafts.

    We need to support the above cases, but for now the following simplification is apply:

    1. All excitatory synapses onto PYR cells will form on spines, all excitatory synapses
       onto inhib cells will form on shafts, with the exception of Spiny Stellate cells.
       Those will be on spines.
    2. All inhibitory synapses form on the shaft for all cell types.
    3. Some synapses share the same spine instance. (Note: As of writing, there is
       no spine instance in circuit)

    To follow this assignment rules, one uses the following fields defined in the sonata
    technical description

    1. Map synapse to the spine that is the closest in length: `spine_length` for chemical
       connections.
    2. All excitatory synapses onto PYR cells will form on spines, all excitatory synapses
       onto inhibitory cells will form on shafts. All inhibitory synapses form on the
       shaft for all cell types.
       Requires the edge field `syn_type_id`, which is 0 for inhibitory synapses, 100 for
       excitatory ones.
       Requires the node field `morph_class`, used to classify the morphology.
    """

    _reductive = False
    _required = False

    # This filter adds three columns:
    _columns = [
        (None, "spine_morphology"),
        (None, "spine_psd_id"),
        (None, "spine_sharing_id"),
    ]

    def __init__(self, recipe, source, target, morphos):
        super().__init__(recipe, source, target, morphos)
        self._morphologies, self._filter = _create_spine_morphology_udf(
            morphos.spine_morphology_path
        )

    def apply(self, circuit):
        """Return a circuit dataframe with spine morphologies assigned"""
        # Ideally, adding this to the current schema would be sufficient. For some reason
        # though, Spark only accepts adding empty columns and populating those.
        # schema = (
        #     circuit.df.schema
        #     .add("spine_morphology", T.IntegerType(), False)
        #     .add("spine_psd_id", T.IntegerType(), False)
        #     .add("spine_sharing_id", T.IntegerType(), False)
        # )
        df = (
            circuit.df.withColumn("spine_morphology", F.lit(0))
            .withColumn("spine_psd_id", F.lit(0))
            .withColumn("spine_sharing_id", F.lit(0))
        )
        df = df.mapInPandas(self._filter, df.schema).withColumn(
            "spine_morphology",
            F.col("spine_morphology").alias(
                "spine_morphology",
                metadata={"enumeration_values": list(self._morphologies)},
            ),
        )
        return df


def _get_spine_lengths(filename):
    """Get all spine lengths from a single spine morphology file"""
    import morphio

    spine_morph = morphio.DendriticSpine(filename)
    distances = np.zeros(len(spine_morph.post_synaptic_density), dtype=float)
    base_pos = spine_morph.points[0]
    for index, psd in enumerate(spine_morph.post_synaptic_density):
        section = spine_morph.sections[psd.section_id]
        start_segment_pos = section.points[psd.segment_id]
        end_segment_pos = section.points[psd.segment_id + 1]
        segment_length = end_segment_pos - start_segment_pos
        psd_pos = start_segment_pos + (segment_length * psd.offset)
        distances[index] = np.linalg.norm(psd_pos - base_pos)
    return distances


def _read_spine_morphology_attributes(spine_morpho_path: Path):
    """Reads all spine morphologies from the input path and returns a dataframe with spine
    morphology properties
    """
    files = sorted(spine_morpho_path.glob("*.h5"))
    ids = np.ndarray((0,), dtype=int)
    lengths = np.ndarray((0,), dtype=float)
    morphologies = np.ndarray((0,), dtype=int)
    names = np.array([""] + [f.stem for f in files])

    for n, path in enumerate(files):
        spines = _get_spine_lengths(path)
        size = len(spines)

        ids = np.append(ids, np.arange(size))
        # morphology names are offset by 1 to account for no assignment (pos 0: "")
        morphologies = np.append(morphologies, np.repeat(n + 1, size))
        lengths = np.append(lengths, spines)

    return names, pd.DataFrame(
        {"spine_length": lengths, "spine_morphology": morphologies, "spine_psd_id": ids}
    )


def _create_spine_morphology_udf(spine_morpho_path: Path):
    """Return a generating function that can be used to assign spine morphologies"""
    names, spine_data = _read_spine_morphology_attributes(spine_morpho_path)

    def __generate(dfs):
        for df in dfs:
            mcount = len(spine_data["spine_length"])
            scount = len(df["spine_length"])

            morphology_spine_lengths = np.array(spine_data["spine_length"])
            morphology_spine_names = np.array(spine_data["spine_morphology"])
            morphology_spine_psd_ids = np.array(spine_data["spine_psd_id"])

            sonata_spine_lengths = np.array(df["spine_length"])

            morphology_spine_lengths.shape = (1, mcount)
            sonata_spine_lengths.shape = (scount, 1)

            mspines = np.repeat(morphology_spine_lengths, scount, axis=0)
            sspines = np.repeat(sonata_spine_lengths, mcount, axis=1)

            # Subtract the two matrices and form the argmin over every row
            morpho_idxs = np.argmin(np.abs(mspines - sspines), axis=1).flatten()
            default_idxs = (df["syn_type_id"] == 0) | (df["src_morph_class"] == "INT")

            final_morphologies = morphology_spine_names[morpho_idxs]
            final_morphologies[default_idxs] = 0
            final_psd_ids = morphology_spine_psd_ids[morpho_idxs]
            final_psd_ids[default_idxs] = -1
            df["spine_morphology"] = final_morphologies
            df["spine_psd_id"] = final_psd_ids
            df["spine_sharing_id"] = np.full(final_psd_ids.shape, -1)

            yield df

    return names, __generate
