# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
from fnmatch import filter as matchfilter
from glob import glob
import time
import os

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from .definitions import CellClass, CheckpointPhases
from . import _filtering
from . import filters
from . import schema
from . import utils
from .utils.spark import checkpoint_resume
from . import synapse_properties

__all__ = ["Functionalizer", "session", "CheckpointPhases", "ExtendedCheckpointAvail"]

logger = utils.get_logger(__name__)
_MB = 1024*1024

# Globals
spark = None
sc = None
GraphFrame = None


class ExtendedCheckpointAvail(Exception):
    """An exception signalling that process_filters can be skipped
    """
    pass


class Functionalizer(object):
    """ Functionalizer Session class
    """
    # Defaults for instance vars
    fdata = None
    """:property: Functionalizer low-level data"""

    recipe = None
    """:property: The parsed recipe"""

    neuron_stats = None
    """:property: The :py:class:`~spykfunc.stats.NeuronStats` object for the current touch set"""

    morphologies = None
    """:property: The morphology RDD"""

    neuronDF = None
    """:property: The Neurons info (from MVD) as a Dataframe"""

    neuronG = None
    """:property: The Graph representation of the touches (GraphFrame)"""

    # TouchDF is volatile and we trigger events on update
    _touchDF = None
    
    # handler functions used in decorators
    _assign_to_touchDF = utils.assign_to_property('touchDF')
    _change_maxPartitionMB = lambda size: lambda: spark.conf.set(
        "spark.sql.files.maxPartitionBytes", 
        size * _MB
    )
    
    # ==========
    def __init__(self, only_s2s=False, spark_opts=None):
        global spark, sc, GraphFrame

        if spark_opts:
            os.environ['PYSPARK_SUBMIT_ARGS'] = spark_opts + " pyspark-shell"

        # Create Spark session with the static config
        spark = (SparkSession.builder
                 .appName("Functionalizer")
                 .enableHiveSupport()
                 .config("spark.shuffle.compress", False)
                 .config("spark.checkpoint.compress", True)
                 .config("spark.jars", os.path.join(os.path.dirname(__file__), "data/spykfunc_udfs.jar"))
                 .config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11")
                 .config("spark.sql.files.maxPartitionBytes", 128*_MB)
                 .config("spark.sql.autoBroadcastJoinThreshold", 1024*_MB)
                 .config("spark.sql.broadcastTimeout", 1200) # 20min
                 .config("spark.driver.maxResultSize", "2G")
                 .getOrCreate())

        try:
            from graphframes import GraphFrame
        except ImportError:
            logger.error("Graphframes could not be imported\n"
                         "Please start a spark cluster with GraphFrames support."
                         " (e.g. pyspark --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11)")
            raise

        # Configuring Spark runtime
        sc = spark.sparkContext
        sc.setLogLevel("WARN")
        sc.setCheckpointDir("_checkpoints/tmp")
        sc._jsc.hadoopConfiguration().setInt("parquet.block.size", 32*_MB)
        sqlContext = SQLContext.getOrCreate(sc)
        sqlContext.registerJavaFunction("gauss_rand", "spykfunc.udfs.GaussRand")
        sqlContext.registerJavaFunction("float2binary", "spykfunc.udfs.FloatArraySerializer")
        sqlContext.registerJavaFunction("int2binary", "spykfunc.udfs.IntArraySerializer")

        self._run_s2f = not only_s2s
        self.output_dir = "spykfunc_output"
        self.neuron_stats = NeuronStats()
        self._initial_touchDF = None

        if only_s2s:
            logger.info("Running S2S only")

    # -------------------------------------------------------------------------
    # Data loading and Init
    # -------------------------------------------------------------------------
    def init_data(self, recipe_file, mvd_file, morpho_dir, touch_files):
        """ Initializes all data for a Functionalizer session, reading MVDs, morphologies, recipe,
        and making all conversions

        :param recipe_file: The recipe file (XML)
        :param mvd_file: The mvd file
        :param morpho_dir: The dir containing all required morphologies
        :param touch_file: The first touch file, given all others can be found followig the naming convention"
        """
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        os.path.isdir(self.output_dir) or os.makedirs(self.output_dir)
        os.path.isdir("_checkpoints") or os.makedirs("_checkpoints")

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Check touches files
        all_touch_files = glob(touch_files)
        if not all_touch_files:
            logger.critical("Invalid touch file path")
            
        touches_parquet_files_expr = touch_files
        
        if len(all_touch_files) == 1:
            # Most probably the user gave the first, we will find the others
            _file0 = all_touch_files[0]
            basename = _file0[:-len(".parquet")]
            index_pos = basename.rfind(".") + 1
            if index_pos > 0:
                touches_parquet_files_expr = basename[:index_pos] + "*.parquet"
                all_touch_files = glob(touches_parquet_files_expr)
                if not all_touch_files:
                    raise ValueError("Invalid touch files. Please provide touches in parquet format.")

        # Load Neurons data
        fdata = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir), spark)
        fdata.load_mvd_neurons_morphologies()

        # Reverse DF name vectors
        self.mtypes_df = spark.createDataFrame(enumerate(fdata.mTypes), schema.INT_STR_SCHEMA)

        # Init the Enumeration to contain fzer CellClass index
        CellClass.init_fzer_indexes(fdata.cellClasses)

        # Load synapse properties
        self.synapse_class_matrix = fdata.load_synapse_prop_matrix(self.recipe)
        self.synapse_class_prop_df = fdata.load_synapse_properties_and_classification(self.recipe)

        # Shortcuts
        self.fdata = fdata
        self.neuron_count = int(fdata.nNeurons)
        self.neuronDF = fdata.neuronDF
        self.morphologies = fdata.morphologyRDD

        # 'Load' touches
        self._touchDF = self._initial_touchDF = fdata.load_touch_parquet(touches_parquet_files_expr) \
            .withColumnRenamed("pre_neuron_id", "src") \
            .withColumnRenamed("post_neuron_id", "dst")
                
        # Grow suffle partitions with size of touches DF
        # In generic cases we dont shuffle all the fields, so we reduce this by a factor of 2
        # Min: 100 reducers
        # NOTE: According to some tests we need to cap the amount of reducers to 4000 per node
        #       otherwise performance is really bad. However it is difficult to create a generic 
        #       rule that translates this and an optimal number of reducers really depends on the 
        #       reduction being done. We leave this tunning to each reducing operation using coalesce
        #       So here we define the highest default, which is the same number of partitions rounded
        # NOTE: Some problems during shuffle happen with many partitions if shuffle compression is enabled!

        touch_partitions = self._touchDF.rdd.getNumPartitions()
        shuffle_partitions = ((touch_partitions-1) // 100 + 1) * 100
        if touch_partitions <= 100:
            shuffle_partitions = 100
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
        
        # Create graphFrame and set it as stats source without recalculating
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)  # Rebuild graph
        self.neuron_stats.touch_graph = self.neuronG
        self.neuron_stats._total_neurons = self.neuron_count

        # Data exporter
        self.exporter = NeuronExporter(output_path=self.output_dir)

    # ----
    @property
    def touchDF(self):
        """
        :property: The current touch set Dataframe.
        NOTE that setting to this attribute will trigger updating the graph
        """
        return self._touchDF

    @touchDF.setter
    def touchDF(self, new_touches):
        self._touchDF = new_touches
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)   # Rebuild graph
        self.neuron_stats.touch_graph = self.neuronG              # Reset stats source

    # ----
    @property
    def dataQ(self):
        """
        :property: A :py:class:`~spykfunc._filtering.DataSetQ` object, offering a high-level query API on
        the current Neuron-Touch Graph
        """
        return _filtering.DataSetQ(self.neuronG.find("(n1)-[t]->(n2)"))

    # ----
    def reset(self):
        """Discards any filtering and reverts the touches to the original state.
        """
        self.touchDF = self._initial_touchDF

    # -------------------------------------------------------------------------
    # Main entry point of Filter Execution
    # -------------------------------------------------------------------------
    @_assign_to_touchDF
    @checkpoint_resume(CheckpointPhases.ALL_FILTERS.name,
                       before_load_handler=_change_maxPartitionMB(32))
    def process_filters(self, overwrite=False):
        """Runs all functionalizer filters in order, according to the classic functionalizer:
        (1) Soma-axon distance, (2) Touch rules, (3.1) Reduce and (3.2) Cut
        """
        # Avoid recomputing filters unless overwrite=True
        skip_soma_axon = False
        skip_touch_rules = False

        if not overwrite:
            if self.checkpoint_exists(CheckpointPhases.SYNAPSE_PROPS):
                # We need to raise an exception, otherwise decorators expect a generated dataframe
                logger.warning("Extended Touches Checkpoint avail. Skipping filtering...")
                raise ExtendedCheckpointAvail("Extended Touches Checkpoint avail. Skip process_filters or set overwrite to True")
            if self.checkpoint_exists(CheckpointPhases.FILTER_TOUCH_RULES):
                skip_soma_axon = True
            if self.checkpoint_exists(CheckpointPhases.FILTER_REDUCED_TOUCHES):
                skip_soma_axon = True
                skip_touch_rules = True
        
        self._ensure_data_loaded()
        logger.info("Starting Filtering...")

        if not skip_soma_axon:
            self.filter_by_soma_axon_distance()
        if self._run_s2f:
            if not skip_touch_rules:
                self.filter_by_touch_rules()
            self.run_reduce_and_cut()

        # Filter helpers write result to self.touchDF (@_assign_to_touchDF)
        return self.touchDF

    # -------------------------------------------------------------------------
    # Exporting results
    # -------------------------------------------------------------------------
    def export_results(self, format_parquet=False, output_path=None, overwrite=False):
        """ Exports the current touches to storage, appending the synapse property fields

        :param format_parquet: If True will export the touches in parquet format (rather than hdf5)
        :param output_path: Changes the default export directory
        """
        logger.info("Computing touch synaptical properties")
        extended_touches = self._assign_synpse_properties(overwrite)

        # Calc the number of NRN output files to target ~32 MB part ~1M touches
        n_parts = extended_touches.rdd.getNumPartitions()
        if n_parts <=32:
            # Small circuit. We directly count and target 1M touches per output file
            total_t = extended_touches.count()
            n_parts = (total_t // (1024 * 1024)) or 1
        else:
            # Main settings define large parquet to be read in partitions of 32 or 64MB.
            # However, in s2s that might still be too much.
            if not self._run_s2f:
                n_parts = n_parts * 2

        # Export
        logger.info("Exporting touches...")
        exporter = self.exporter
        if output_path is not None:
            exporter.output_path = output_path

        if format_parquet:
            exporter.export_parquet(extended_touches)
        else:
            exporter.export_hdf5(extended_touches, self.neuron_count, 
                                 create_efferent=True, 
                                 n_partitions=n_parts)

        logger.info("Data export complete")
        
    # --- 
    @checkpoint_resume(CheckpointPhases.SYNAPSE_PROPS.name)
    def _assign_synpse_properties(self, overwrite=False):

        # Calc syn props 
        self._ensure_data_loaded()
        extended_touches = synapse_properties.compute_additional_h5_fields(
            self.neuronG,
            self.synapse_class_matrix,
            self.synapse_class_prop_df
        )
        return extended_touches
            
        # TODO: Eventually we could save by file group, but OutOfMem during sort
        # extended_touches = extended_touches.withColumn(
        #    "file_i", 
        #    (F.col("post_gid") / n_neurons_file).cast(T.IntegerType())
        # )
        # # Previous way of saving. Remind that we might need to add such option to checkpoint_resume
        # return self.exporter.save_temp(extended_touches, "extended_touches.parquet") #,
        # #                               partition_col="file_i")

    # -------------------------------------------------------------------------
    # Functions to create/apply filters for the current session
    # -------------------------------------------------------------------------
    @_assign_to_touchDF
    def filter_by_soma_axon_distance(self):
        """BLBLD-42: Creates a Soma-axon distance filter and applies it to the current touch set.
        """
        self._ensure_data_loaded()
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        return distance_filter.apply(self.neuronG)

    # ----
    @_assign_to_touchDF
    @checkpoint_resume(CheckpointPhases.FILTER_TOUCH_RULES.name)
    def filter_by_touch_rules(self):
        """Creates a TouchRules filter according to recipe and applies it to the current touch set
        """
        self._ensure_data_loaded()
        logger.info("Filtering by touchRules...")
        touch_rules_filter = filters.TouchRulesFilter(self.recipe.touch_rules)
        return touch_rules_filter.apply(self.neuronG)

    # ----
    @_assign_to_touchDF
    def run_reduce_and_cut(self):
        """Create and apply Reduce and Cut filter
        """
        self._ensure_data_loaded()
        # Index and distribute mtype rules across the cluster
        mtype_conn_rules = self._build_concrete_mtype_conn_rules(self.recipe.conn_rules, self.fdata.mTypes)

        # cumulative_distance_f = filters.CumulativeDistanceFilter(distributed_conn_rules, self.neuron_stats)
        # self.touchDF = cumulative_distance_f.apply(self.neuronG)

        logger.info("Applying Reduce and Cut...")
        rc = filters.ReduceAndCut(mtype_conn_rules, self.neuron_stats, spark, )
        return rc.apply(self.neuronG, mtypes=self.mtypes_df)

    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------
    def _ensure_data_loaded(self):
        """ Ensures required data is available"""
        if self.recipe is None or self.neuronG is None:
            raise RuntimeError("No touches available. Please load data first.")
    
    # ---
    @staticmethod
    def checkpoint_exists(phase):
        cp_file = os.path.join("_checkpoints", phase.name.lower()) + ".parquet"
        if os.path.exists(cp_file):
            try:
                df = spark.read.parquet(cp_file)
                del df
                return True
            except Exception as e:
                logger.warning("Checkpoint %s can't be loaded, probably result of failed action.", str(e))
        return False
    
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
    fzer = Functionalizer(options.s2s, options.spark_opts)
    if options.output_dir:
        fzer.output_dir = options.output_dir
    fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    return fzer
