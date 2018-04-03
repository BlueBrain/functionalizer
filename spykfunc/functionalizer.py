# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
from fnmatch import filter as matchfilter
import time
import os

import sparkmanager as sm

from .circuit import Circuit
from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from .definitions import CellClass, CheckpointPhases, RunningMode
from . import _filtering
from . import filters
from . import schema
from . import utils
from .utils.spark import defaults as checkpoint_defaults, checkpoint_resume
from . import synapse_properties

__all__ = ["Functionalizer", "session", "CheckpointPhases", "ExtendedCheckpointAvail"]

logger = utils.get_logger(__name__)
_MB = 1024**2


class ExtendedCheckpointAvail(Exception):
    """An exception signalling that process_filters can be skipped
    """
    pass


class Functionalizer(object):
    """ Functionalizer Session class
    """

    _circuit = None
    """:property: ciruit containing neuron and touch data"""

    recipe = None
    """:property: The parsed recipe"""

    neuron_stats = None
    """:property: The :py:class:`~spykfunc.stats.NeuronStats` object for the current touch set"""

    # handler functions used in decorators
    _assign_to_circuit = utils.assign_to_property('circuit')
    _change_maxPartitionMB = lambda size: lambda: sm.conf.set(
        "spark.sql.files.maxPartitionBytes",
        size * _MB
    )

    # ==========
    def __init__(self, only_s2s=False, format_hdf5=False, spark_opts=None, checkpoints=None, output="spykfunc_output"):
        # Create Spark session with the static config
        filename = os.path.join(output, 'report.json')

        if checkpoints:
            self.__checkpoints = checkpoints
        else:
            self.__checkpoints = os.path.join(output, "_checkpoints")
        self.__output = output

        checkpoint_defaults.directory = self.__checkpoints

        spark_config = {
            "spark.shuffle.compress": False,
            "spark.checkpoint.compress": True,
            "spark.jars": os.path.join(os.path.dirname(__file__), "data/spykfunc_udfs.jar"),
            "spark.sql.autoBroadcastJoinThreshold": 0,
            "spark.sql.broadcastTimeout": 30 * 60,  # 30 minutes to do calculations that will be broadcasted
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.files.maxPartitionBytes": 128 * _MB,
            "derby.system.home": os.path.join(self.__checkpoints, "derby")
        }
        sm.create("Functionalizer", spark_config, spark_opts, report=filename)

        # Configuring Spark runtime
        sm.setLogLevel("WARN")
        sm.setCheckpointDir(os.path.join(self.__checkpoints, "tmp"))
        sm._jsc.hadoopConfiguration().setInt("parquet.block.size", 32 * _MB)
        sm.register_java_functions([
            ("gauss_rand", "spykfunc.udfs.GaussRand"),
            ("float2binary", "spykfunc.udfs.FloatArraySerializer"),
            ("int2binary", "spykfunc.udfs.IntArraySerializer"),
            ("poisson_rand", "spykfunc.udfs.PoissonRand"),
            ("gamma_rand", "spykfunc.udfs.GammaRand")
        ])
        # sm.spark._jvm.spykfunc.udfs.PoissonRand.registerUDF(sm.spark._jsparkSession)
        # sm.spark._jvm.spykfunc.udfs.GammaRand.registerUDF(sm.spark._jsparkSession)

        self._mode = RunningMode.S2S if only_s2s else RunningMode.S2F
        self._format_hdf5 = format_hdf5
        self.neuron_stats = NeuronStats()

        if only_s2s:
            logger.info("Running S2S only")

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
        os.path.isdir(self.__output) or os.makedirs(self.__output)
        os.path.isdir(self.__checkpoints) or os.makedirs(self.__checkpoints)

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Load Neurons data
        fdata = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir))
        fdata.load_mvd_neurons_morphologies()

        # Reverse DF name vectors
        self.mtypes_df = sm.createDataFrame(enumerate(fdata.mTypes), schema.INT_STR_SCHEMA)

        # Init the Enumeration to contain fzer CellClass index
        CellClass.init_fzer_indexes(fdata.cellClasses)

        # 'Load' touches
        touches = fdata.load_touch_parquet(*touch_files) \
            .withColumnRenamed("pre_neuron_id", "src") \
            .withColumnRenamed("post_neuron_id", "dst")

        self._circuit = Circuit(fdata, touches, self.recipe)
        self.neuron_stats.circuit = self._circuit

        # Grow suffle partitions with size of touches DF
        # In generic cases we dont shuffle all the fields, so we reduce this by a factor of 2
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
        self.exporter = NeuronExporter(output_path=self.__output)

    @property
    def output_directory(self):
        """:property: the directory to save results in
        """
        return self.__output

    # ----
    @property
    def circuit(self):
        """:property: The current touch set with neuron data as Dataframe.

        .. note::

           Setting the circuit with touch-data only will trigger a join the
           next time the circuit is accessed.
        """
        return self._circuit.dataframe

    @circuit.setter
    def circuit(self, circuit):
        self._circuit.dataframe = circuit

    @property
    def touches(self):
        """:property: The current touch set without additional neuron data as Dataframe.
        """
        return self._circuit.touches

    # ----
    @property
    def dataQ(self):
        """
        :property: A :py:class:`~spykfunc._filtering.DataSetQ` object, offering a high-level query API on
        the current Neuron-Touch Graph
        """
        return _filtering.DataSetQ(self._circuit.dataframe)

    # -------------------------------------------------------------------------
    # Main entry point of Filter Execution
    # -------------------------------------------------------------------------
    @_assign_to_circuit
    def process_filters(self, overwrite=False):
        """Runs all functionalizer filters in order, according to the classic functionalizer:
           (1.1) Soma-axon distance
           (1.2) Touch rules (s2f only)
           (2.1) Reduce (s2f only)
           (2.2) Cut (s2f only)
        """
        if overwrite:
            checkpoint_defaults.overwrite = True

        self._ensure_data_loaded()
        logger.info("Starting Filtering...")

        self.filter_by_rules(mode=self._mode)

        if self._mode == RunningMode.S2F:
            self.run_reduce_and_cut()

        # Filter helpers write result to self._circuit (@_assign_to_circuit)
        return self.touches

    # -------------------------------------------------------------------------
    # Exporting results
    # -------------------------------------------------------------------------
    @sm.assign_to_jobgroup
    def export_results(self, format_hdf5=None, output_path=None, overwrite=False):
        """ Exports the current touches to storage, appending the synapse property fields

        :param format_parquet: If True will export the touches in parquet format (rather than hdf5)
        :param output_path: Changes the default export directory
        """
        logger.info("Computing touch synaptical properties")
        extended_touches = self._assign_synpse_properties(overwrite, mode=self._mode)

        logger.info("Exporting touches...")
        exporter = self.exporter
        if output_path is not None:
            exporter.output_path = output_path
        if format_hdf5 is None:
            format_hdf5 = self._format_hdf5

        if format_hdf5:
            # Calc the number of NRN output files to target ~32 MB part ~1M touches
            n_parts = extended_touches.rdd.getNumPartitions()
            if n_parts <= 32:
                # Small circuit. We directly count and target 1M touches per output file
                total_t = extended_touches.count()
                n_parts = (total_t // (1024 * 1024)) or 1
            else:
                # Main settings define large parquet to be read in partitions of 32 or 64MB.
                # However, in s2s that might still be too much.
                if self._mode == RunningMode.S2S:
                    n_parts = n_parts * 2
            exporter.export_hdf5(extended_touches,
                                 self._circuit.neuron_count,
                                 create_efferent=False,
                                 n_partitions=n_parts)
        else:
            exporter.export_parquet(extended_touches)
        logger.info("Data export complete")

    # ---
    @checkpoint_resume(CheckpointPhases.SYNAPSE_PROPS.name,
                       before_save_handler=Circuit.only_touch_columns)
    def _assign_synpse_properties(self, overwrite=False, mode=None):
        # Calc syn props
        self._ensure_data_loaded()
        extended_touches = synapse_properties.compute_additional_h5_fields(
            self.circuit,
            self._circuit.reduced,
            self._circuit.synapse_class_matrix,
            self._circuit.synapse_class_properties
        )
        return extended_touches

    # -------------------------------------------------------------------------

    # ----
    @sm.assign_to_jobgroup
    @_assign_to_circuit
    @checkpoint_resume(CheckpointPhases.FILTER_RULES.name,
                       before_save_handler=Circuit.only_touch_columns)
    def filter_by_rules(self, mode):
        """Creates a TouchRules filter according to recipe and applies it to the current touch set
        """
        logger.info("Filtering by boutonDistance...")
        self._ensure_data_loaded()
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        result = distance_filter.apply(self.circuit)
        if mode == RunningMode.S2F:
            logger.info("Filtering by touchRules...")
            touch_rules_filter = filters.TouchRulesFilter(self._circuit.touch_rules)
            result = touch_rules_filter.apply(result)
        return result

    # ----
    @sm.assign_to_jobgroup
    @_assign_to_circuit
    @checkpoint_resume(CheckpointPhases.REDUCE_AND_CUT.name,
                       before_save_handler=Circuit.only_touch_columns,
                       before_load_handler=_change_maxPartitionMB(32),
                       bucket_cols=("src", "dst"))
    def run_reduce_and_cut(self):
        """Create and apply Reduce and Cut filter
        """
        self._ensure_data_loaded()
        # Index and distribute mtype rules across the cluster
        mtype_conn_rules = self._build_concrete_mtype_conn_rules(self.recipe.conn_rules, self._circuit.morphology_types)

        # cumulative_distance_f = filters.CumulativeDistanceFilter(distributed_conn_rules, self.neuron_stats)
        # self.touchDF = cumulative_distance_f.apply(self.circuit)

        logger.info("Applying Reduce and Cut...")
        rc = filters.ReduceAndCut(mtype_conn_rules, self.neuron_stats)
        return rc.apply(self.circuit, mtypes=self.mtypes_df)

    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------
    def _ensure_data_loaded(self):
        """ Ensures required data is available"""
        if self.recipe is None or self._circuit is None:
            raise RuntimeError("No touches available. Please load data first.")

    # ---
    @staticmethod
    def _build_concrete_mtype_conn_rules(src_conn_rules, mTypes):
        """ Transform conn rules into concrete rule instances (without wildcards) and indexed by pathway
        """
        mtypes_rev = {mtype: i for i, mtype in enumerate(mTypes)}
        conn_rules = {}

        for rule in src_conn_rules:  # type: ConnectivityPathRule
            srcs = matchfilter(mTypes, rule.source)
            dsts = matchfilter(mTypes, rule.destination)
            for src in srcs:
                for dst in dsts:
                    # key = src + ">" + dst
                    # Key is now an int
                    key = (mtypes_rev[src] << 16) + mtypes_rev[dst]
                    if key in conn_rules:
                        # logger.debug("Several rules applying to the same mtype connection: %s->%s [Rule: %s->%s]",
                        #                src, dst, rule.source, rule.destination)
                        prev_rule = conn_rules[key]
                        # Overwrite if it is specific
                        if (('*' in prev_rule.source and '*' not in rule.source) or
                                ('*' in prev_rule.destination and '*' not in rule.destination)):
                            conn_rules[key] = rule
                    else:
                        conn_rules[key] = rule

        return conn_rules


# -------------------------------------------
# Spark Functionalizer session creator
# -------------------------------------------
def session(options):
    """
    Helper function to create a functionalizer session given an options object

    :param options: An object containing the required option attributes, as built \
                    by the arg parser: :py:data:`commands.arg_parser`.
    """
    args = {
        'format_hdf5': options.format_hdf5,
        'only_s2s': options.s2s,
        'spark_opts': options.spark_opts
    }
    if options.output_dir:
        args['output'] = options.output_dir
    if options.checkpoint_dir:
        args['checkpoints'] = options.checkpoint_dir
    fzer = Functionalizer(**args)
    fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    return fzer
