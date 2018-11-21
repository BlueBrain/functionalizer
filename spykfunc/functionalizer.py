# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
import os
import time
import sparkmanager as sm

from . import filters
from . import utils
from .filters import DatasetOperation
from .circuit import Circuit
from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from .definitions import CellClass, CheckpointPhases
from .utils.checkpointing import checkpoint_resume
from .utils.filesystem import adjust_for_spark, autosense_hdfs

__all__ = ["Functionalizer", "session", "CheckpointPhases"]

logger = utils.get_logger(__name__)
_MB = 1024**2


class _SpykfuncOptions:
    format_hdf5 = False
    output_dir = "spykfunc_output"
    properties = None
    name = "Functionalizer"
    cache_dir = None
    filters = None
    no_morphos = False
    checkpoint_dir = None

    def __init__(self, options_dict):
        filename = options_dict.get('configuration', None)
        for name, option in options_dict.items():
            # Update only relevat, non-None entries
            if option is not None and hasattr(self, name):
                setattr(self, name, option)
        if self.checkpoint_dir is None:
            local_p = os.path.join(self.output_dir, "_checkpoints")
            hdfs_p = '/_spykfunc_{date}/checkpoints'
            self.checkpoint_dir = autosense_hdfs(local_p, hdfs_p)
        if self.cache_dir is None:
            self.cache_dir = os.path.join(self.output_dir, "_mvd")
        if self.properties is None:
            self.properties = utils.Configuration(outdir=self.output_dir,
                                                  filename=filename,
                                                  overrides=options_dict.get('overrides'))
        if self.filters is None:
            raise AttributeError("Need to have filters specified!")


class Functionalizer(object):
    """ Functionalizer Session class
    """

    circuit = None
    """:property: ciruit containing neuron and touch data"""

    recipe = None
    """:property: The parsed recipe"""

    neuron_stats = None
    """:property: The :py:class:`~spykfunc.stats.NeuronStats` object for the current touch set"""

    exporter = None
    """:property: The :py:class:`~spykfunc.data_export.NeuronExporter` object"""

    # ==========
    def __init__(self, **options):
        # Create config
        self._config = _SpykfuncOptions(options)
        checkpoint_resume.directory = self._config.checkpoint_dir

        # Create Spark session with the static config
        report_file = os.path.join(self._config.output_dir, 'report.json')
        sm.create(self._config.name,
                  self._config.properties("spark"),
                  report=report_file)

        # Configuring Spark runtime
        sm.setLogLevel("WARN")
        sm.setCheckpointDir(adjust_for_spark(os.path.join(self._config.checkpoint_dir, "tmp")))
        sm._jsc.hadoopConfiguration().setInt("parquet.block.size", 64 * _MB)
        sm.register_java_functions([
            ("float2binary", "spykfunc.udfs.FloatArraySerializer"),
            ("int2binary", "spykfunc.udfs.IntArraySerializer"),
        ])

        self.neuron_stats = NeuronStats()

    # -------------------------------------------------------------------------
    # Data loading and Init
    # -------------------------------------------------------------------------
    @sm.assign_to_jobgroup
    def init_data(self, recipe_file, mvd_file, morpho_dir, touch_files):
        """ Initializes all data for a Functionalizer session, reading MVDs, morphologies, recipe,
        and making all conversions

        :param recipe_file: The recipe file (XML)
        :param mvd_file: The mvd file
        :param morpho_dir: The dir containing all required morphologies
        :param touch_files: A list of touch files. A single globbing expression can be specified as well"
        """
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        os.path.isdir(self._config.output_dir) or os.makedirs(self._config.output_dir)

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Load Neurons data
        fdata = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir), self._config.cache_dir)
        fdata.load_mvd_neurons_morphologies()

        # Init the Enumeration to contain fzer CellClass index
        CellClass.init_fzer_indexes(fdata.cellClasses)

        # Verify required morphologies are available
        if self._config.no_morphos:
            logger.info("Running in no-morphologies mode. No ChC cells handling performed.")
            filters.SynapseProperties._morphologies = False
        else:
            expected = set(n + '.h5' for n in fdata.morphologies)
            have = set(os.listdir(morpho_dir))
            missing = expected - have
            if len(missing) > 0:
                logger.error("Some morphologies could not be located. Missing:\n\t" + " ".join(missing) + "\n"
                             "Please provide a valid morphology path or restart with --no-morphos")
                raise ValueError("Morphologies missing")
            logger.debug("All morphology files found")

        # 'Load' touches
        touches = fdata.load_touch_parquet(*touch_files) \
            .withColumnRenamed("pre_neuron_id", "src") \
            .withColumnRenamed("post_neuron_id", "dst")

        self.circuit = Circuit(fdata, touches, self.recipe)
        self.neuron_stats.circuit = self.circuit

        # Grow suffle partitions with size of touches DF
        # Min: 100 reducers
        # NOTE: According to some tests we need to cap the amount of reducers to 4000 per node
        # NOTE: Some problems during shuffle happen with many partitions if shuffle compression is enabled!
        touch_partitions = touches.rdd.getNumPartitions()
        shuffle_partitions = ((touch_partitions-1) // 100 + 1) * 100
        if touch_partitions == 0:
            raise ValueError("No partitions found in touch data")
        elif touch_partitions <= 100:
            shuffle_partitions = 100

        logger.info("Processing %d touch partitions (shuffle counts: %d)", touch_partitions, shuffle_partitions)
        sm.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

        # Data exporter
        self.exporter = NeuronExporter(output_path=self._config.output_dir)

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

        filters = DatasetOperation.initialize(filters or self._config.filters,
                                              self.recipe,
                                              self.circuit.morphologies,
                                              self.neuron_stats)

        logger.info("Starting Filtering...")
        for f in filters:
            self.circuit = f(self.circuit)
        return self.circuit

    # -------------------------------------------------------------------------
    # Exporting results
    # -------------------------------------------------------------------------
    @sm.assign_to_jobgroup
    def export_results(self, format_hdf5=None, output_path=None, overwrite=False):
        """ Exports the current touches to storage, appending the synapse property fields

        :param format_parquet: If True will export the touches in parquet format (rather than hdf5)
        :param output_path: Changes the default export directory
        """
        logger.info("Exporting touches...")
        exporter = self.exporter
        if format_hdf5 is None:
            format_hdf5 = self._config.format_hdf5

        if format_hdf5:
            # Calc the number of NRN output files to target ~32 MB part ~1M touches
            n_parts = self.circuit.touches.rdd.getNumPartitions()
            if n_parts <= 32:
                # Small circuit. We directly count and target 1M touches per output file
                total_t = self.circuit.touches.count()
                n_parts = (total_t // (1024 * 1024)) or 1
            exporter.export_hdf5(self.circuit.touches,
                                 self.circuit.neuron_count,
                                 create_efferent=False,
                                 n_partitions=n_parts)
        else:
            exporter.export_syn2_parquet(self.circuit.touches)
        logger.info("Data export complete")

    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------
    def _ensure_data_loaded(self):
        """ Ensures required data is available"""
        if self.recipe is None or self.circuit is None:
            raise RuntimeError("No touches available. Please load data first.")


# -------------------------------------------
# Spark Functionalizer session creator
# -------------------------------------------
def session(options):
    """
    Helper function to create a functionalizer session given an options object

    :param options: An object containing the required option attributes, as built \
                    by the arg parser: :py:data:`commands.arg_parser`.
    """
    args = vars(options)
    fzer = Functionalizer(**args)
    fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    return fzer
