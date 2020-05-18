# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
import glob
import logging
import os
import time
import sparkmanager as sm

from . import filters
from . import schema
from . import utils
from .filters import DatasetOperation
from .circuit import Circuit
from .recipe import Recipe
from .data_loader import NeuronData
from .definitions import CheckpointPhases, SortBy
from .utils.checkpointing import checkpoint_resume
from .utils.filesystem import adjust_for_spark, autosense_hdfs

__all__ = ["Functionalizer", "CheckpointPhases"]

logger = utils.get_logger(__name__)
_MB = 1024 ** 2


class _SpykfuncOptions:
    output_dir = "spykfunc_output"
    properties = None
    name = "Functionalizer"
    cache_dir = None
    filters = None
    checkpoint_dir = None
    debug = False
    strict = False

    def __init__(self, options_dict):
        filename = options_dict.get("configuration", None)
        for name, option in options_dict.items():
            # Update only relevat, non-None entries
            if option is not None and hasattr(self, name):
                setattr(self, name, option)
        if self.checkpoint_dir is None:
            local_p = os.path.join(self.output_dir, "_checkpoints")
            hdfs_p = "/_spykfunc_{date}/checkpoints"
            self.checkpoint_dir = autosense_hdfs(local_p, hdfs_p)
        if self.cache_dir is None:
            self.cache_dir = os.path.join(self.output_dir, "_circuits")
        if self.properties is None:
            self.properties = utils.Configuration(
                outdir=self.output_dir,
                filename=filename,
                overrides=options_dict.get("overrides"),
            )
        if self.filters is None:
            raise AttributeError("Need to have filters specified!")


class Functionalizer(object):
    """ Functionalizer Session class
    """

    circuit = None
    """:property: ciruit containing neuron and touch data"""

    recipe = None
    """:property: The parsed recipe"""

    # ==========
    def __init__(self, **options):
        # Create config
        self._config = _SpykfuncOptions(options)
        checkpoint_resume.directory = self._config.checkpoint_dir

        if self._config.debug:
            filters.enable_debug()

        # Create Spark session with the static config
        report_file = os.path.join(self._config.output_dir, "report.json")
        sm.create(
            self._config.name, self._config.properties("spark"), report=report_file
        )

        # Configuring Spark runtime
        sm.setLogLevel("WARN")
        sm.setCheckpointDir(
            adjust_for_spark(os.path.join(self._config.checkpoint_dir, "tmp"))
        )
        sm._jsc.hadoopConfiguration().setInt("parquet.block.size", 64 * _MB)

    # -------------------------------------------------------------------------
    # Data loading and Init
    # -------------------------------------------------------------------------
    @sm.assign_to_jobgroup
    def init_data(self, recipe_file, source, target, morpho_dir, parquet=None, sonata=None):
        """ Initializes all data for a Functionalizer session, reading MVDs, morphologies, recipe,
        and making all conversions

        :param recipe_file: The recipe file (XML)
        :param source: The source population path and name
        :param target: The target population path and name
        :param morpho_dir: The dir containing all required morphologies
        :param parquet: A list of touch files. A single globbing expression can be specified as well"
        :param sonata: The edge population path and name
        """
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        os.path.isdir(self._config.output_dir) or os.makedirs(self._config.output_dir)

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Load Neurons data
        n_from = NeuronData(*source, self._config.cache_dir)
        n_from.load_neurons()

        if source == target:
            n_to = n_from
        else:
            n_to = NeuronData(*target, self._config.cache_dir)
            n_to.load_neurons()

        # 'Load' touches
        if parquet:
            if isinstance(parquet, str):
                parquet = glob.glob(parquet)
            touches = (
                NeuronData.load_touch_parquet(*parquet)
                .withColumnRenamed("pre_neuron_id", "src")
                .withColumnRenamed("post_neuron_id", "dst")
            )
        elif sonata:
            touches = (
                NeuronData.load_touch_sonata(*sonata)
                .withColumnRenamed("pre_neuron_id", "src")
                .withColumnRenamed("post_neuron_id", "dst")
            )
        else:
            raise RuntimeError("Need to have touches")

        self.circuit = Circuit(n_from, n_to, touches, self.recipe, morpho_dir)

        # Grow suffle partitions with size of touches DF
        # Min: 100 reducers
        # NOTE: According to some tests we need to cap the amount of reducers to 4000 per node
        # NOTE: Some problems during shuffle happen with many partitions if shuffle compression is enabled!
        touch_partitions = touches.rdd.getNumPartitions()
        shuffle_partitions = ((touch_partitions - 1) // 100 + 1) * 100
        if touch_partitions == 0:
            raise ValueError("No partitions found in touch data")
        elif touch_partitions <= 100:
            shuffle_partitions = 100

        logger.info(
            "Processing %d touch partitions (shuffle counts: %d)",
            touch_partitions,
            shuffle_partitions,
        )
        sm.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

        return self

    @property
    def output_directory(self):
        """:property: the directory to save results in
        """
        return self._config.output_dir

    @property
    def touches(self):
        """:property: The current touch set without additional neuron data as Dataframe.
        """
        return self.circuit.touches

    # -------------------------------------------------------------------------
    # Main entry point of Filter Execution
    # -------------------------------------------------------------------------
    def process_filters(self, filters=None, overwrite=False):
        """Runs all functionalizer filters in order, according to the classic functionalizer:
           (1.1) Soma-axon distance
           (1.2) Touch rules (s2f only)
           (2.1) Reduce (s2f only)
           (2.2) Cut (s2f only)
        """
        self._ensure_data_loaded()

        checkpoint_resume.overwrite = overwrite

        filters = DatasetOperation.initialize(
            filters or self._config.filters,
            self.recipe,
            self.circuit.source,
            self.circuit.target,
            self.circuit.morphologies,
        )

        if self._config.strict:
            utils.Enforcer().check()

        logger.info("Starting Filtering...")
        for f in filters:
            self.circuit = f(self.circuit)
        return self.circuit

    # -------------------------------------------------------------------------
    # Exporting results
    # -------------------------------------------------------------------------
    @sm.assign_to_jobgroup
    def export_results(
            self,
            output_path=None,
            overwrite=False,
            order: SortBy = SortBy.POST,
            filename: str = "circuit.parquet"):
        """ Exports the current touches to storage, appending the synapse property fields

        :param output_path: Changes the default export directory
        :param order: How to sort the output
        """
        def get_fields(df):
            # Transitional SYN2 spec fields
            for field, alias, cast in schema.OUTPUT_COLUMN_MAPPING:
                if hasattr(df, field):
                    logger.info("Writing field %s", alias)
                    if cast:
                        yield getattr(df, field).cast(cast).alias(alias)
                    else:
                        yield getattr(df, field).alias(alias)

        df = (
            self.circuit.touches
            .withColumnRenamed("src", "pre_gid")
            .withColumnRenamed("dst", "post_gid")
        )

        output_path = os.path.realpath(os.path.join(self.output_directory, filename))
        logger.info("Exporting touches...")
        df_output = df.select(*get_fields(df)) \
                      .sort(*(order.value))
        df_output.write.parquet(
            adjust_for_spark(output_path, local=True),
            mode="overwrite"
        )
        logger.info("Data export complete")

    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------
    def _ensure_data_loaded(self):
        """ Ensures required data is available"""
        if self.recipe is None or self.circuit is None:
            raise RuntimeError("No touches available. Please load data first.")
