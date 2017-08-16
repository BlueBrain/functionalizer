from __future__ import print_function, absolute_import
# *************************************************************************
#  An implementation of Functionalizer in spark
# *************************************************************************

import os.path as _path
from fnmatch import filter as matchfilter
from glob import glob
import time

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import StorageLevel

# from morphotool import MorphologyDB
from .definitions import CellClass, MType
from .recipe import Recipe
from .data_loader import NeuronDataSpark
from .dataio import touches
from .dataio.cppneuron import MVD_Morpho_Loader
from .stats import NeuronStats
from . import _filtering
from . import filters
from . import utils
if False: from .recipe import ConnectivityPathRule  # NOQA

logger = utils.get_logger(__name__)

try:
    from graphframes import GraphFrame
except ImportError:
    print("""Please start a spark instance with GraphFrames support
    e.g. pyspark --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11""")


class Functionalizer(object):
    """ Functionalizer Session class
    """
    # Class vars
    spark = None

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
        # Init spark as static class property
        if Functionalizer.spark is None:
            Functionalizer.spark = SparkSession.builder.getOrCreate()

    # ---
    def init_data(self, recipe_file, mvd_file, morpho_dir, touch_files):
        logger.debug("%s: Data loading...", time.ctime())

        # Load recipe
        self.recipe = Recipe(recipe_file)

        # Load info about touches
        all_touch_files = glob(touch_files)
        if not all_touch_files:
            logger.critical("Invalid touch file path")

        # self.touch_info = touches.TouchInfo(all_touch_files)
        self.neuron_stats = NeuronStats()

        # Load Neurons data
        fdata = NeuronDataSpark(MVD_Morpho_Loader(mvd_file, morpho_dir), self.spark)

        fdata.load_mvd_neurons_morphologies()

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
        self.neuron_stats.update_touch_graph_source(self.neuronG, skip_update=True)

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
        logger.debug("%s: Starting Filtering...", time.ctime())
        try:
            self.filter_by_soma_axon_distance()        
            if self._run_s2f:
                self.filter_by_touch_rules()
                
              
                
                self.run_reduce_and_cut()
            # DEBUG
            n = self.touchDF.count()
            logger.debug("%s: Number of touches after filter: %d", time.ctime(), n)
            
        except Exception as e:
            print(e)
            return 1

    # ---
    # Instantiation of filters for the sessions data

    def filter_by_soma_axon_distance(self):
        """BLBLD-42: filter by soma-axon distance
        """
        logger.info("Filtering by soma-axon distance...")
        distance_filter = filters.BoutonDistanceFilter(self.recipe.synapses_distance)
        self.touchDF = distance_filter.apply(self.neuronG)

    def filter_by_touch_rules(self):
        """Filter according to recipe TouchRules
        """
        logger.info("Filtering by touchRules...")
        touch_rules_filter = filters.TouchRulesFilter(self.recipe.touch_rules)
        newtouchDF = touch_rules_filter.apply(self.neuronG)

        # So far there was quite some processing which would be lost since data 
        # is read everytime from disk, so we persist it for next RC step
        logger.debug("... and dumping intermediate touches...")
        self.touchDF = newtouchDF.persist(StorageLevel.DISK_ONLY)
        
        # NOTE: Using count() or other functions which materialize the DF might incur
        # an extra read step for the subsequent action (to be analyzed)
        # In the case of DISK_ONLY caches() that would have a signifficant impact, so we avoid it.
        # 
        # n = self.touchDF.count()
        # logger.info("%s: Number of touches after Touch Rules filters: %d", time.ctime(), n)
        

    def run_reduce_and_cut(self):
        """Apply Reduce and Cut
        """
        # Index and distribute mtype rules across the cluster
        mtype_conn_rules = self.build_concrete_mtype_conn_rules(self.recipe.conn_rules, self._spark_data.mtypeVec)
        distributed_conn_rules = self.spark.sparkContext.broadcast(mtype_conn_rules)

        # cumulative_distance_f = filters.CumulativeDistanceFilter(distributed_conn_rules, self.neuron_stats)
        # self.touchDF = cumulative_distance_f.apply(self.neuronG)

        logger.info("Applying Reduce and Cut...")
        rc = filters.ReduceAndCut(distributed_conn_rules, self.neuron_stats)
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
                        logger.warning("Several rules applying to the same mtype connection: %s->%s", src, dst)
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
    fzer.init_data(options.recipe_file, options.mvd_file, options.morpho_dir, options.touch_files)
    return fzer
