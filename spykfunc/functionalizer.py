# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************
from __future__ import absolute_import
from fnmatch import filter as matchfilter
from glob import glob
import time
import os

import pyspark
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark import StorageLevel

from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .data_export import NeuronExporter
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from .definitions import CellClass
from . import _filtering
from . import filters
from . import utils
from . import synapse_properties
if False: from .recipe import ConnectivityPathRule  # NOQA

logger = utils.get_logger(__name__)

__all__ = ["Functionalizer", "session"]


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
    fdata = None
    recipe = None
    touch_info = None
    neuron_stats = None
    morphologies = None
    neuronDF = None
    neuronG = None
    # TouchDF is volatile and we trigger events on update
    _touchDF = None

    def __init__(self, only_s2s=False):
        self._run_s2f = not only_s2s
        self.output_dir = "spykfunc_output"
        self.neuron_stats = NeuronStats()
        self._initial_touchDF = None

        if only_s2s:
            logger.info("Running S2S only")

        # register random udef
        sqlContext = SQLContext.getOrCreate(sc)
        # Apparently functions are instantiated on every executed query
        sqlContext.registerJavaFunction("gauss_rand", "spykfunc.udfs.GaussRand")
        sqlContext.registerJavaFunction("float2binary", "spykfunc.udfs.FloatArraySerializer")
        sqlContext.registerJavaFunction("int2binary", "spykfunc.udfs.IntArraySerializer")

    # ---
    def init_data(self, recipe_file, mvd_file, morpho_dir, touch_files):
        # In "program" mode this dir wont change later, so we can check here
        # for its existence/permission to create
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)

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
        else:
            # Otherwise from the binary touches files
            self._touchDF = fdata.load_touch_bin(touch_files)

        # Create graphFrame and set it as stats source without recalculating
        self.neuronG = GraphFrame(self.neuronDF, self._touchDF)  # Rebuild graph
        self.neuron_stats.update_touch_graph_source(self.neuronG, overwrite_previous_gf=False)

        # Data exporter
        self.exporter = NeuronExporter(output_path=self.output_dir)

    #---
    def ensure_data_loaded(self):
        if self.recipe is None or self.neuronG is None:
            raise RuntimeError("No touches available. Please load data first.")

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
    @property
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
        self.touchDF = self._initial_touchDF

    # ---
    def process_filters(self):
        """Runs all functionalizer filters
        """
        self.ensure_data_loaded()
        logger.info("%s: Starting Filtering...", time.ctime())
        try:
            self.filter_by_soma_axon_distance()
            if self._run_s2f:
                self.filter_by_touch_rules()
                self.run_reduce_and_cut()
        except:
            logger.error(utils.format_cur_exception())
            return 1

        # Force compute, saving to parquet - fast and space efficient
        self.touchDF = self.exporter.save_temp(self.touchDF)

        return 0

    # ---
    def export_results(self, format_parquet=False, output_path=None):
        self.ensure_data_loaded()
        logger.info("Computing touch synaptical properties")
        extended_touches = synapse_properties.compute_additional_h5_fields(self.neuronG,
                                                                           self.synapse_class_matrix,
                                                                           self.synapse_class_prop_df)
        extended_touches = self.exporter.save_temp(extended_touches, "extended_touches.parquet")

        logger.info("Exporting touches...")
        exporter = self.exporter
        if output_path is not None:
            exporter.output_path = output_path

        try:
            if format_parquet:
                exporter.export_parquet(extended_touches)
            else:
                exporter.export_hdf5(extended_touches,self.fdata.nNeurons)
        except:
            logger.error(utils.format_cur_exception())
            return 1
        else:
            logger.info("Done exporting.")
            return 0
        logger.info("Finished")

    # ---------------------------------------------------------
    # Functions to create/apply filters for the current session
    # ---------------------------------------------------------
    def filter_by_soma_axon_distance(self):
        """BLBLD-42: filter by soma-axon distance
        """
        self.ensure_data_loaded()
        logger.info("Filtering by soma-axon distance...")
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        self.touchDF = distance_filter.apply(self.neuronG)

    # ---
    def filter_by_touch_rules(self):
        """Filter according to recipe TouchRules
        """
        self.ensure_data_loaded()
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
        self.ensure_data_loaded()
        # Index and distribute mtype rules across the cluster
        mtype_conn_rules = self._build_concrete_mtype_conn_rules(self.recipe.conn_rules, self.fdata.mTypes)

        # cumulative_distance_f = filters.CumulativeDistanceFilter(distributed_conn_rules, self.neuron_stats)
        # self.touchDF = cumulative_distance_f.apply(self.neuronG)

        logger.info("Applying Reduce and Cut...")
        rc = filters.ReduceAndCut(mtype_conn_rules, self.neuron_stats, sc)
        self.touchDF = rc.apply(self.neuronG)

    # ---
    @staticmethod
    def _build_concrete_mtype_conn_rules(src_conn_rules, mTypes):
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
    try:
        fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    except:
        logger.error(utils.format_cur_exception())
        return None
    return fzer
