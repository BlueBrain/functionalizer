# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
from fnmatch import filter as matchfilter
from glob import glob
import time
import os

from pyspark.sql import SparkSession, SQLContext
# from pyspark.sql import functions as F

from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from .definitions import CellClass
from . import _filtering
from . import filters
from . import schema
from . import utils
from . import synapse_properties

__all__ = ["Functionalizer", "session"]

# Globals
spark = None
sc = None
GraphFrame = None
logger = utils.get_logger(__name__)


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

    def __init__(self, only_s2s=False, spark_opts=None):
        global spark, sc, GraphFrame

        if spark_opts:
            os.environ['PYSPARK_SUBMIT_ARGS'] = spark_opts + " pyspark-shell"

        # Create Spark session with the static config
        spark = (SparkSession.builder
                 .appName("Functionalizer")
                 .config("spark.checkpoint.compress", True)
                 .config("spark.shuffle.file.buffer", 1024*1024)
                 .config("spark.jars", os.path.join(os.path.dirname(__file__), "data/spykfunc_udfs.jar"))
                 .config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11")
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
        sc.setCheckpointDir("_checkpoints")
        # spark.conf.set("spark.sql.shuffle.partitions", 256)  # we set later when reading touches
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024**3)  # 1GB for auto broadcast
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
        os.path.isdir("_tmp") or os.makedirs("_tmp")

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Check touches files
        all_touch_files = glob(touch_files)
        if not all_touch_files:
            logger.critical("Invalid touch file path")

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
        self.neuronDF = fdata.neuronDF
        self.morphologies = fdata.morphologyRDD

        # 'Load' touches, from parquet if possible
        _file0 = all_touch_files[0]
        touches_parquet_files_expr = _file0[:_file0.rfind(".")] + "Data.*.parquet"
        touch_files_parquet = glob(touches_parquet_files_expr)
        if touch_files_parquet:
            self._touchDF = self._initial_touchDF = fdata.load_touch_parquet(touches_parquet_files_expr) \
                .withColumnRenamed("pre_neuron_id", "src") \
                .withColumnRenamed("post_neuron_id", "dst")
                
            # I dont know whats up with Spark but sometimes when shuffle partitions is not 200
            # we have problems. We try to mitigate using only multiples of 200
            spark.conf.set("spark.sql.shuffle.partitions", 
                           max(100, (self._touchDF.rdd.getNumPartitions()-1) // 200 * 200))
        else:
            # self._touchDF = fdata.load_touch_bin(touch_files)
            raise ValueError("Invalid touch files. Please provide touches in parquet format.")

        # Create graphFrame and set it as stats source without recalculating
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)  # Rebuild graph
        self.neuron_stats.update_touch_graph_source(self.neuronG, overwrite_previous_gf=False)

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
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)    # Rebuild graph
        self.neuron_stats.update_touch_graph_source(self.neuronG)  # Reset stats source

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
    def process_filters(self):
        """Runs all functionalizer filters in order, according to the classic functionalizer:
        (1) Soma-axon distance, (2) Touch rules, (3.1) Reduce and (3.2) Cut
        """
        self._ensure_data_loaded()
        logger.info("%s: Starting Filtering...", time.ctime())
        try:
            self.filter_by_soma_axon_distance()
            if self._run_s2f:
                self.filter_by_touch_rules()
                self.run_reduce_and_cut()
        except Exception:
            logger.error(utils.format_cur_exception())
            return 1

        # Force compute, saving to parquet - fast and space efficient
        # We should be using checkpoint which is the standard way of doing it, but it still recomputes twice
        logger.info("Cutting touches...")
        self.touchDF = self.exporter.save_temp(self.touchDF)

        return 0

    # -------------------------------------------------------------------------
    # Exporting results
    # -------------------------------------------------------------------------
    def export_results(self, format_parquet=False, output_path=None):
        """ Exports the current touches to storage, appending the synapse property fields

        :param format_parquet: If True will export the touches in parquet format (rather than hdf5)
        :param output_path: Changes the default export directory
        """
        self._ensure_data_loaded()
        logger.info("Computing touch synaptical properties")
        extended_touches = synapse_properties.compute_additional_h5_fields(
            self.neuronG, self.synapse_class_matrix, self.synapse_class_prop_df)
        extended_touches = self.exporter.save_temp(extended_touches, "extended_touches.parquet")

        logger.info("Exporting touches...")
        exporter = self.exporter
        if output_path is not None:
            exporter.output_path = output_path

        try:
            if format_parquet:
                exporter.export_parquet(extended_touches)
            else:
                exporter.export_hdf5(extended_touches, self.fdata.nNeurons, create_efferent=True)
        except Exception:
            logger.error(utils.format_cur_exception())
            return 1

        logger.info("Done exporting.")
        logger.info("Finished")
        return 0

    # -------------------------------------------------------------------------
    # Functions to create/apply filters for the current session
    # -------------------------------------------------------------------------

    def filter_by_soma_axon_distance(self):
        """BLBLD-42: Creates a Soma-axon distance filter and applies it to the current touch set.
        """
        self._ensure_data_loaded()
        logger.info("Filtering by soma-axon distance...")
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        self.touchDF = distance_filter.apply(self.neuronG)

    # ----
    def filter_by_touch_rules(self):
        """Creates a TouchRules filter according to recipe and applies it to the current touch set
        """
        self._ensure_data_loaded()
        logger.info("Filtering by touchRules...")
        touch_rules_filter = filters.TouchRulesFilter(self.recipe.touch_rules)
        newtouchDF = touch_rules_filter.apply(self.neuronG)

        # So far there was quite some processing which would be recomputed
        self.apply_checkpoint_touches(newtouchDF)

    # ----
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
        self.touchDF = rc.apply(self.neuronG, mtypes=self.mtypes_df)

    # -------------------------------------------------------------------------
    # Helper functions
    # -------------------------------------------------------------------------

    def apply_checkpoint_touches(self, touchDF, checkpoint=True):
        """ Takes a new set of touches, checkpointing by default
        """
        if checkpoint:
            # self.touchDF = newtouchDF.checkpoint()  # checkpoint is still not working well
            logger.debug(" -> Checkpointing...")
            touchDF.write.parquet("_tmp/filtered_touches.parquet", mode="overwrite")
            self.touchDF = spark.read.parquet("_tmp/filtered_touches.parquet")
        else:
            self.touchDF = touchDF
        

    def _ensure_data_loaded(self):
        """ Ensures required data is available
        """
        if self.recipe is None or self.neuronG is None:
            raise RuntimeError("No touches available. Please load data first.")

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
    Main execution function to work similarly to functionalizer app

    :param options: An object containing the required option attributes, as built \
    by the arg parser: :py:data:`commands.arg_parser`.
    """
    assert "NeuronDataSpark" in globals(), "Use spark-submit to run your job"

    fzer = Functionalizer(options.s2s, options.spark_opts)
    if options.output_dir:
        fzer.output_dir = options.output_dir
    try:
        fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    except Exception:
        logger.error(utils.format_cur_exception())
        return None
    return fzer
