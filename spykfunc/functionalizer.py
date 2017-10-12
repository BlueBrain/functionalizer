# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
from fnmatch import filter as matchfilter
from glob import glob
import time
import os

from pyspark.sql import SparkSession, SQLContext
from pyspark import StorageLevel

from .definitions import CellClass, MType
from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from . import _filtering
from . import filters
from . import utils
if False: from .recipe import ConnectivityPathRule  # NOQA

logger = utils.get_logger(__name__)

# =====================================
# Global Initting
# =====================================
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", min(sc.defaultParallelism * 4, 256))

try:
    from graphframes import GraphFrame
except ImportError:
    GraphFrame = None
    logger.error("""graphframes could not be imported
    Please start a spark instance with GraphFrames support
    e.g. pyspark --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11""")


class Functionalizer(object):
    """ Functionalizer Session class
    """
    # Defaults for instance vars
    recipe = None
    touch_info = None
    neuron_stats = None
    cellClassesIndexed = None
    morphologies = None
    neuronDF = None
    neuronG = None
    eTypes = None
    mTypes = None
    # TouchDF is volatile and we trigger events on update
    _touchDF = None
    _spark_data = None

    def __init__(self, only_s2s=False):
        self._run_s2f = not only_s2s
        self.output_dir = "spykfunc_output"

        if only_s2s:
            logger.info("Running S2S only")

        # register random udef
        sqlContext = SQLContext.getOrCreate(sc)
        # Apparently functions are instantiated on every executed query
        sqlContext.registerJavaFunction("gauss_rand", "spykfunc.udfs.GaussRand")
        sqlContext.registerJavaFunction("float2binary", "spykfunc.udfs.FloatArraySerializer")

        # _conc = sc._jvm.spykfunc.udfs.BinaryConcat().apply
        # self.binary_agg_func = utils.make_agg_f(sc, _conc)

    # ---
    def init_data(self, recipe_file, mvd_file, morpho_dir, touch_files):
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)

        self.morpho_dir = morpho_dir

        logger.debug("%s: Data loading...", time.ctime())
        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Check touches files
        all_touch_files = glob(touch_files)
        if not all_touch_files:
            logger.critical("Invalid touch file path")
        self.neuron_stats = NeuronStats()

        # Load Neurons data
        fdata = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir), spark)
        fdata.load_mvd_neurons_morphologies()

        # Load synapse properties
        # self.synapse_properties_class = fdata.load_synapse_properties_and_classification(self.recipe)
        synapse_class_matrix = fdata.load_synapse_prop_matrix(self.recipe)
        synapse_class_prop_df = fdata.load_synapse_properties_and_classification(self.recipe)

        # Shortcuts
        self._spark_data = fdata
        self.neuronDF = fdata.neuronDF
        self.morphologies = fdata.morphologyRDD
        self.mTypes = [MType(mtype) for mtype in fdata.mtypeVec]
        self.eTypes = fdata.etypeVec
        self.cellClassesIndexed = [CellClass.from_string(syn_class_name)
                                   for syn_class_name in fdata.synaClassVec]

        # 'Load' touches, from parquet if possible
        _file0 = all_touch_files[0]
        touches_parquet_files_expr = _file0[:_file0.rfind(".")] + "Data.*.parquet"
        touch_files_parquet = glob(touches_parquet_files_expr)
        if touch_files_parquet:
            self._touchDF = fdata.load_touch_parquet(touches_parquet_files_expr) \
                .withColumnRenamed("pre_neuron_id", "src") \
                .withColumnRenamed("post_neuron_id", "dst")
        else:
            # Otherwise from the binary touches files
            self._touchDF = fdata.load_touch_bin(touch_files)

        # Create graphFrame and set it as stats source without recalculating
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)  # Rebuild graph
        self.neuron_stats.update_touch_graph_source(self.neuronG, overwrite_previous_gf=False)

        # Data exporter
        self.exporter = NeuronExporter(self.morpho_dir, self.recipe,
                                       synapse_class_matrix, synapse_class_prop_df,
                                       output_path=self.output_dir)

    # ---
    @property
    def touchDF(self):
        return self._touchDF

    @touchDF.setter
    def touchDF(self, new_touches):
        self._touchDF = new_touches
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)    # Rebuild graph
        self.neuron_stats.update_touch_graph_source(self.neuronG)  # Reset stats source

    # ---
    def dataQ(self):
        """
        Return a DataSetQ object, offering a high-level yet flexible query API on the current Neuron-Touch Graph
        Refer to the API of DataSetQ in _filtering.py
        """
        return _filtering.DataSetQ(self.neuronG.find("(n1)-[t]->(n2)"))

    # ---
    def reset(self):
        """Discards any filtering applied to touches
        """
        self.touchDF = self._spark_data.touchDF

    # ---
    def process_filters(self):
        """Runs all functionalizer filters
        """
        logger.info("%s: Starting Filtering...", time.ctime())
        try:
            self.filter_by_soma_axon_distance()
            if self._run_s2f:
                self.filter_by_touch_rules()
                self.run_reduce_and_cut()

        except Exception:
            import traceback
            logger.error(traceback.format_exc(1))
            return 1

        # Force compute, saving to parquet - fast and space efficient
        self.exporter.save_temp(self.neuronG)
        return 0

    # ---
    def export_results(self, format_parquet=False, output_path=None):
        logger.info("Exporting touches...")
        exporter = self.exporter
        if output_path is not None:
            exporter.output_path = output_path

        if True: #try:
            if format_parquet:
                exporter.export_parquet(self.neuronG)
            else:
                exporter.export_hdf5(self.neuronG)
        # except:
        #     import traceback
        #     logger.error(traceback.format_exc(1))
        #     return 1
        # else:
        #     logger.info("Done exporting.")
        #     return 0

    # ---------------------------------------------------------
    # Functions to create/apply filters for the current session
    # ---------------------------------------------------------
    def filter_by_soma_axon_distance(self):
        """BLBLD-42: filter by soma-axon distance
        """
        logger.info("Filtering by soma-axon distance...")
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        self.touchDF = distance_filter.apply(self.neuronG)

    # ---
    def filter_by_touch_rules(self):
        """Filter according to recipe TouchRules
        """
        logger.info("Filtering by touchRules...")
        touch_rules_filter = filters.TouchRulesFilter(self.recipe.touch_rules)
        newtouchDF = touch_rules_filter.apply(self.neuronG)

        # So far there was quite some processing which would be lost since data
        # is read everytime from disk, so we persist it for next RC step
        self.touchDF = newtouchDF.persist(StorageLevel.DISK_ONLY)

        # NOTE: Using count() or other functions which materialize the DF might incur
        #       an extra read step for the subsequent action (to be analyzed)
        #       In the case of DISK_ONLY caches() that would have a signifficant impact, so we avoid it.

    # ---
    def run_reduce_and_cut(self):
        """Apply Reduce and Cut
        """
        # Index and distribute mtype rules across the cluster
        mtype_conn_rules = self.build_concrete_mtype_conn_rules(self.recipe.conn_rules, self._spark_data.mtypeVec)

        # cumulative_distance_f = filters.CumulativeDistanceFilter(distributed_conn_rules, self.neuron_stats)
        # self.touchDF = cumulative_distance_f.apply(self.neuronG)

        logger.info("Applying Reduce and Cut...")
        rc = filters.ReduceAndCut(mtype_conn_rules, self.neuron_stats, sc)
        self.touchDF = rc.apply(self.neuronG)

    # ---
    @staticmethod
    def build_concrete_mtype_conn_rules(src_conn_rules, mTypes):
        """ Transform conn rules into concrete rule instances (without wildcards) and indexed by mtype-mtype
            Index is a string in the form "src>dst"
        """
        conn_rules = {}
        for rule in src_conn_rules:  # type: ConnectivityPathRule
            srcs = matchfilter(mTypes, rule.source)
            dsts = matchfilter(mTypes, rule.destination)
            for src in srcs:
                for dst in dsts:
                    key = src + ">" + dst
                    if key in conn_rules:
                        logger.warning("Several rules applying to the same mtype connection: %s->%s [Rule: %s->%s]",
                                       src, dst, rule.source, rule.destination)
                        if '*' not in rule.source and '*' not in rule.destination:
                            # Overwrite if it is specific
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
    """
    assert "NeuronDataSpark" in globals(), "Use spark-submit to run your job"

    fzer = Functionalizer(options.s2s)
    if options.output_dir:
        fzer.output_dir = options.output_dir

    if True: #try:
        fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    # except:
    #     import traceback
    #     logger.error(traceback.format_exc(1))
    #     return None
    return fzer
