"""A plugin to filter by spine length
"""
from operator import attrgetter

import pandas as pd
from pyspark.sql import functions as F

import sparkmanager as sm

from spykfunc.filters import DatasetOperation
from spykfunc.utils import get_logger

from . import add_bin_column, add_random_column


logger = get_logger(__name__)


_KEY_SPINE = 0x200


class SpineLengthFilter(DatasetOperation):
    """Filter synapses by spine length"""

    _required = False

    def __init__(self, recipe, source, target, morphos):
        super().__init__(recipe, source, target, morphos)
        self.seed = recipe.seeds.synapseSeed
        logger.info("Using seed %d for spine length adjustment", self.seed)

        self.binnings = sorted(recipe.spine_lengths, key=attrgetter("length"))

    def apply(self, circuit):
        # Augment circuit with both a random value (used for survival) and
        # assign a bin based on spine length
        touches = add_bin_column(
            circuit.df, "spine_bin", [b.length for b in self.binnings], "spine_length"
        )
        touches = add_random_column(
            touches,
            "spine_rand",
            self.seed,
            _KEY_SPINE,
        )

        # Extract the desired PDF for spine lengths
        wants_cdf = [b.fraction for b in self.binnings]
        wants_pdf = [a - b for a, b in zip(wants_cdf[1:], wants_cdf)]
        want = pd.DataFrame(
            {
                "max_length": [b.length for b in self.binnings[1:]],
                "want": wants_pdf,
            }
        )

        # Gather the real distribution of spine lenghts
        have = touches.groupby(F.col("spine_bin")).count().toPandas()
        have.set_index("spine_bin", inplace=True)
        have.sort_index(inplace=True)
        have.columns = ["have"]
        # Calculate the survival rate
        have = have.join(want, how="inner")
        have["survival_rate"] = have.want / have.have
        have.survival_rate /= have.survival_rate.max()
        have.survival_rate.fillna(value=0.0, inplace=True)
        have["surviving"] = have.survival_rate * have.have

        logger.info(
            "Adjusting spine lengths, using the following data\n%s",
            have.to_string(index=False),
        )

        have["spine_bin"] = have.index
        rates = sm.createDataFrame(have[["spine_bin", "survival_rate"]])
        return (
            touches.join(F.broadcast(rates), "spine_bin")
            .where(F.col("spine_rand") <= F.col("survival_rate"))
            .drop("spine_bin", "spine_rand", "survival_rate")
        )
