"""An implementation of Functionalizer in Apache Spark."""

import hashlib
import os
from pathlib import Path
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from fz_td_recipe import Recipe
import sparkmanager as sm

from . import utils
from .filters import DatasetOperation
from .circuit import Circuit
from .io import NodeData, EdgeData, shift_branch_type
from .definitions import CheckpointPhases, SortBy
from .utils.checkpointing import checkpoint_resume
from .utils.filesystem import adjust_for_spark, autosense_hdfs
from .version import version as spykfunc_version
from .schema import METADATA_PATTERN, METADATA_PATTERN_RE, OUTPUT_MAPPING

__all__ = ["Functionalizer", "CheckpointPhases"]

logger = utils.get_logger(__name__)
_MB = 1024**2


class RecipeNotPresent(Exception):
    """Indicates that no Recipe has been set up."""


class _MockRecipe:
    def __getattr__(self, attr):
        raise RecipeNotPresent("Need to provide a recipe")


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


class Functionalizer:
    """Functionalizer session class."""

    circuit = None
    """:property: ciruit containing neuron and touch data"""

    recipe = _MockRecipe()
    """:property: The parsed recipe"""

    # ==========
    def __init__(self, **options):
        """Create a new Functionalizer instance."""
        # Create config
        self._config = _SpykfuncOptions(options)
        self._recipe_file = None
        checkpoint_resume.directory = self._config.checkpoint_dir

        if self._config.debug:
            from . import filters

            filters.enable_debug(self._config.output_dir)

        # Create Spark session with the static config
        report_file = os.path.join(self._config.output_dir, "report.json")
        sm.create(self._config.name, self._config.properties("spark"), report=report_file)

        # Configuring Spark runtime
        sm.setLogLevel("WARN")
        sm.setCheckpointDir(adjust_for_spark(os.path.join(self._config.checkpoint_dir, "tmp")))
        sm._jsc.hadoopConfiguration().setInt("parquet.block.size", 64 * _MB)

    @sm.assign_to_jobgroup
    def init_data(
        self,
        recipe_file,
        circuit_config,
        source,
        source_nodeset,
        target,
        target_nodeset,
        edges=None,
    ):
        """Initialize all data required.

        Will load the necessary cell collections from `source` and `target`
        parameters, and construct the underlying brain :class:`.Circuit`.
        The `recipe_file` will only be fully processed once filters are
        instantiated. Similarly, edge and node data will only be fully read once filters
        are applied.

        Args:
            recipe_file: A scientific prescription to be used by the filters on the
                circuit
            circuit_config: The basic configuration of the circuit
            source: The source population name
            source_nodeset: The source nodeset name
            target: The target population name
            target_nodeset: The target nodeset name
            edges: A list of files containing edges
        """
        logger.debug("Starting data initialization...")

        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        if not os.path.isdir(self._config.output_dir):
            os.makedirs(self._config.output_dir)

        if circuit_config:
            n_from = NodeData(circuit_config, source, source_nodeset, self._config.cache_dir)
            if source == target and source_nodeset == target_nodeset:
                n_to = n_from
            else:
                n_to = NodeData(circuit_config, target, target_nodeset, self._config.cache_dir)
        else:
            n_from = None
            n_to = None

        self.circuit = Circuit(n_from, n_to, EdgeData(*edges))

        self._recipe_file = recipe_file
        if self._recipe_file:
            self.recipe = Recipe(Path(self._recipe_file), circuit_config, (source, target))

        return self

    def _configure_shuffle_partitions(self):
        """Try to find an optimal setting for the amount of shuffle partitions."""
        # Grow suffle partitions with size of touches DF
        # Min: 100 reducers
        # NOTE: According to some tests we need to cap the amount of reducers to 4000 per node
        # NOTE: Some problems during shuffle happen with many partitions if shuffle
        #       compression is enabled!
        touch_partitions = self.circuit.touches.df.rdd.getNumPartitions()
        if touch_partitions == 0:
            raise ValueError("No partitions found in touch data")

        cfg_value = sm.conf.get("spark.sql.files.maxPartitionBytes")
        if cfg_value.endswith("b"):
            cfg_value = cfg_value[:-1]

        # Aim for about half the maximum partition size
        guessed_partitions_from_data = int(self.circuit.input_size / int(cfg_value) + 1)

        # Aim for data parallelism
        guessed_partitions_from_cluster = sm.sc.defaultParallelism

        shuffle_partitions = max([guessed_partitions_from_cluster, guessed_partitions_from_data, 1])

        logger.info(
            "Processing %d touch partitions (shuffle partitions: %d)",
            touch_partitions,
            shuffle_partitions,
        )
        sm.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    @property
    def output_directory(self):
        """:property: the directory to save results in."""
        return Path(self._config.output_dir)

    @property
    def touches(self):
        """:property: The current touch set without additional neuron data as Dataframe."""
        return self.circuit.touches

    # -------------------------------------------------------------------------
    # Main entry point of Filter Execution
    # -------------------------------------------------------------------------
    def process_filters(self, filters=None, overwrite=False):
        """Filter the circuit.

        Uses either the specified filters or a default set, based on the
        parameters passed to the :class:`.Functionalizer` constructor.

        Any filter that writes a checkpoint will be skipped if the sequence
        of data and filters leading up to said checkpoint did not change.
        Use the `overwrite` argument to change this behavior.

        Args:
            filters: A list of filter names to be run.  Any `Filter` suffix should be
                omitted.
            overwrite: Allows to overwrite checkpoints
        """
        if self.circuit is None:
            raise RuntimeError("No touches available, please load data first")

        logger.debug("Starting filter initialization...")
        checkpoint_resume.overwrite = overwrite

        try:
            filters = DatasetOperation.initialize(
                filters or self._config.filters,
                self.recipe,
                self.circuit.source,
                self.circuit.target,
            )
        except RecipeNotPresent:
            logger.fatal("No recipe provided, but specified filter(s) require one")
            raise

        if self._config.strict or self._config.dry_run:
            utils.Enforcer().check()
        if self._config.dry_run:
            return self.circuit

        self._configure_shuffle_partitions()

        logger.info("Starting filter application...")
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
        order: SortBy = SortBy.POST,
        filename: str = "circuit.parquet",
    ):
        """Writes the touches of the circuit to disk.

        Args:
            output_path: Allows to change the default output directory
            order: The sorting of the touches
            filename: Allows to change the default output name
        """

        def get_fields(df):
            # Transitional SYN2 spec fields
            for col in df.columns:
                if col in OUTPUT_MAPPING:
                    alias, cast = OUTPUT_MAPPING[col]
                    logger.info("Writing field %s", alias)
                    if cast:
                        yield getattr(df, col).cast(cast).alias(alias)
                    else:
                        yield getattr(df, col).alias(alias)
                else:
                    logger.info("Writing field %s", col)
                    yield col

        df = (
            Circuit.only_touch_columns(self.circuit.df)
            .withColumnRenamed("src", "source_node_id")
            .withColumnRenamed("dst", "target_node_id")
        )

        # Required for SONATA support
        if not hasattr(df, "edge_type_id"):
            df = df.withColumn("edge_type_id", F.lit(0))

        df = shift_branch_type(df)

        required = set(["population_name", "population_size"])
        if not required.issubset(df.schema["source_node_id"].metadata):
            logger.info("Augmenting metadata for field source_node_id")
            df = df.withColumn(
                "source_node_id",
                F.col("source_node_id").alias(
                    "source_node_id",
                    metadata={
                        "population_name": self.circuit.source.population,
                        "population_size": len(self.circuit.source),
                    },
                ),
            )
        if not required.issubset(df.schema["target_node_id"].metadata):
            logger.info("Augmenting metadata for field target_node_id")
            df = df.withColumn(
                "target_node_id",
                F.col("target_node_id").alias(
                    "target_node_id",
                    metadata={
                        "population_name": self.circuit.target.population,
                        "population_size": len(self.circuit.target),
                    },
                ),
            )

        output_path = os.path.realpath(os.path.join(self.output_directory, filename))
        partitions = sm.sc.defaultParallelism
        logger.info("Exporting touches with %d partitions...", partitions)
        df_output = df.select(*get_fields(df)).sort(*(order.value)).coalesce(partitions)
        df_output.write.parquet(adjust_for_spark(output_path, local=True), mode="overwrite")
        logger.info("Data written to disk")
        self._add_metadata(output_path)
        logger.info("Metadata added to data")
        logger.info("Data export complete")

    def _add_metadata(self, path):
        schema = pq.ParquetDataset(path, use_legacy_dataset=False).schema
        metadata = {k.decode(): v.decode() for k, v in schema.metadata.items()}
        metadata.update(self.circuit.metadata)
        this_run = 1
        for key in metadata:
            if m := METADATA_PATTERN_RE.match(key):
                # "version" numbers may contain a dot, thus: string → float → int
                this_run = max(this_run, int(float(m.group(1))) + 1)
        metadata.update(
            {
                METADATA_PATTERN.format(this_run, "version"): spykfunc_version,
                METADATA_PATTERN.format(this_run, "filters"): ",".join(self._config.filters),
            }
        )
        if self._recipe_file:
            checksum = hashlib.sha256()
            with open(self._recipe_file, "rb") as fd:
                for line in fd:
                    checksum.update(line)
            metadata.update(
                {
                    METADATA_PATTERN.format(this_run, "recipe_path"): os.path.realpath(
                        self._recipe_file
                    ),
                    METADATA_PATTERN.format(this_run, "recipe_sha256"): checksum.hexdigest(),
                }
            )
        if self.circuit.source and self.circuit.target:
            metadata.update(
                {
                    "source_population_name": self.circuit.source.population,
                    "source_population_size": str(len(self.circuit.source)),
                    "target_population_name": self.circuit.target.population,
                    "target_population_size": str(len(self.circuit.target)),
                }
            )
        new_schema = schema.with_metadata(metadata)
        pq.write_metadata(new_schema, os.path.join(path, "_metadata"))
