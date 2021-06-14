# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
import logging
import os
import re
import time
import pyarrow.parquet as pq
import sparkmanager as sm

from pyspark.sql import functions as F

from recipe import Recipe

from . import filters
from . import schema
from . import utils
from .filters import DatasetOperation
from .circuit import Circuit
from .data_loader import NeuronData, EdgeData
from .definitions import CheckpointPhases, SortBy
from .utils.checkpointing import checkpoint_resume
from .utils.filesystem import adjust_for_spark, autosense_hdfs
from .version import version as spykfunc_version
from .schema import METADATA_PATTERN, METADATA_PATTERN_RE

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
    dry_run = False

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
            filters.enable_debug(self._config.output_dir)

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
    def init_data(self, recipe_file, source, source_nodeset, target, target_nodeset,
                        morphologies, edges=None):
        """Initialize all data required

        Will load the necessary cell collections from `source` and `target`
        parameters, and construct the underlying brain :class:`.Circuit`.
        The `recipe_file` will only be fully processed once filters are
        instantiated.

        Arguments
        ---------
        recipe_file
            A scientific prescription to be used by the filters on the
            circuit
        source
            The source population path and name
        target
            The target population path and name
        morphologies
            The storage path to all required morphologies
        edges
            A list of files containing edges
        """
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        os.path.isdir(self._config.output_dir) or os.makedirs(self._config.output_dir)

        logger.debug("Starting data loading...")
        if source and target:
            n_from = NeuronData(source, source_nodeset, self._config.cache_dir)
            if source == target and source_nodeset == target_nodeset:
                n_to = n_from
            else:
                n_to = NeuronData(target, target_nodeset, self._config.cache_dir)
        else:
            n_from = None
            n_to = None

        touches = EdgeData(*edges)

        self.circuit = Circuit(n_from, n_to, touches, morphologies)

        if recipe_file:
            self.recipe = Recipe(recipe_file, self._config.strict)
            if self._config.strict and not self.recipe.validate({
                "fromMType": n_from.mtype_values,
                "fromEType": n_from.etype_values,
                "fromSClass": n_from.sclass_values,
                "toMType": n_to.mtype_values,
                "toEType": n_to.etype_values,
                "toSClass": n_to.sclass_values,
            }):
                raise RuntimeError("Recipe validation failed")

        return self

    def _configure_shuffle_partitions(self, touches):
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
        """Filter the circuit

        Uses either the specified filters or a default set, based on the
        parameters passed to the :class:`.Functionalizer` constructor.

        Any filter that writes a checkpoint will be skipped if the sequence
        of data and filters leading up to said checkpoint did not change.
        Use the `overwrite` argument to change this behavior.

        Arguments
        ---------
        filters
            A list of filter names to be run.  Any `Filter` suffix should
            be omitted.
        overwrite
            Allows to overwrite checkpoints
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

        if self._config.strict or self._config.dry_run:
            utils.Enforcer().check()
        if self._config.dry_run:
            return

        self._configure_shuffle_partitions(self.circuit.touches)

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
        """Writes the touches of the circuit to disk

        Arguments
        ---------
        output_path
            Allows to change the default output directory
        overwrite
            Write over previous results
        order
            The sorting of the touches
        filename
            Allows to change the default output name
        """
        def get_fields(df):
            # Transitional SYN2 spec fields
            for col in df.columns:
                if col in schema.OUTPUT_MAPPING:
                    alias, cast = schema.OUTPUT_MAPPING[col]
                    logger.info("Writing field %s", alias)
                    if cast:
                        yield getattr(df, col).cast(cast).alias(alias)
                    else:
                        yield getattr(df, col).alias(alias)
                else:
                    logger.info("Writing field %s", col)
                    yield col

        df = (
            self.circuit.touches
            .withColumnRenamed("src", "source_node_id")
            .withColumnRenamed("dst", "target_node_id")
        )

        required = set(["population_name", "population_size"])
        if not required.issubset(df.schema["source_node_id"].metadata):
            logger.info("Augmenting metadata for field source_node_id")
            df = df.withColumn("source_node_id", F.col("source_node_id").alias("source_node_id", metadata={
                "population_name": self.circuit.source.population,
                "population_size": len(self.circuit.source)
            }))
        if not required.issubset(df.schema["target_node_id"].metadata):
            logger.info("Augmenting metadata for field target_node_id")
            df = df.withColumn("target_node_id", F.col("target_node_id").alias("target_node_id", metadata={
                "population_name": self.circuit.target.population,
                "population_size": len(self.circuit.target)
            }))

        output_path = os.path.realpath(os.path.join(self.output_directory, filename))
        logger.info("Exporting touches...")
        df_output = df.select(*get_fields(df)) \
                      .sort(*(order.value))
        df_output.write.parquet(
            adjust_for_spark(output_path, local=True),
            mode="overwrite"
        )
        logger.info("Data written to disk")
        self._add_metadata(output_path)
        logger.info("Metadata added to data")
        logger.info("Data export complete")

    def _add_metadata(self, path):
        schema = pq.ParquetDataset(path).schema.to_arrow_schema()
        metadata = {k.decode(): v.decode() for k, v in schema.metadata.items()}
        metadata.update(self.circuit.metadata)
        this_run = 1
        for key in metadata:
            if m := METADATA_PATTERN_RE.match(key):
                # "version" numbers may contain a dot, thus: string → float → int
                this_run = max(this_run, int(float(m.group(1))) + 1)
        metadata.update({
            METADATA_PATTERN.format(this_run, "version"): spykfunc_version,
            METADATA_PATTERN.format(this_run, "filters"): ",".join(self._config.filters),
            METADATA_PATTERN.format(this_run, "recipe"): str(self.recipe or ""),
        })
        if self.circuit.source and self.circuit.target:
            metadata.update({
                "source_population_name": self.circuit.source.population,
                "source_population_size": str(len(self.circuit.source)),
                "target_population_name": self.circuit.target.population,
                "target_population_size": str(len(self.circuit.target)),
            })
        new_schema = schema.with_metadata(metadata)
        pq.write_metadata(new_schema, os.path.join(path, "_metadata"))


    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------
    def _ensure_data_loaded(self):
        """Ensures required data is available
        """
        if self.circuit is None:
            raise RuntimeError("No touches available. Please load data first.")
        if self._config.filters and self.recipe is None:
            raise RuntimeError("No recipe available.")
