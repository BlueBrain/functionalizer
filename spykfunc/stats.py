from pyspark.sql import functions as F
from pyspark.sql.functions import col
from .schema import to_pathway_i
from .utils import cache_to_property

class NeuronStats(object):
    """
    Retrieve and store the properties of the neurons,
    including statistics and MVD properties
    """

    _circuit = None
    """:property: circuit data, neurons and touches"""

    def __init__(self, circuit=None):
        self._circuit = circuit
        self._total_neurons = 0
        self._total_touches = 0
        self._neurons_touch_counts = None
        self._pathway_touches_conns = None

    @staticmethod
    def create_from_touch_info(touch_info):
        """ Builds a NeuronStats object with some fields prefiled
            with information from the binary touch file header
        """
        obj = NeuronStats()
        obj.total_neurons = touch_info.header.numberOfNeurons
        # Total touches can be already expensive to compute for large sets
        obj._total_touches = touch_info.touch_count if obj.total_neurons < 10000 else None
        # We wont probably have all this info in the front node
        # obj.neuron_pre_touch_counts = {n_id: count for n_id, count, _ in touch_info.neuron_stats}
        return obj

    @property
    def circuit(self):
        """The circuit data"""
        return self._circuit

    @circuit.setter
    def circuit(self, new_circuit):
        if self._circuit == new_circuit:
            return
        self._circuit = new_circuit
        self._total_neurons = new_circuit.neuron_count
        self._total_touches = None
        self._neurons_touch_counts = None
        self._pathway_touches_conns = None

    @property
    @cache_to_property("_total_neurons")
    def total_neurons(self):
        """The total neuron count in the circuit"""
        return self._circuit.neurons.count()

    @property
    @cache_to_property("_total_touches")
    def total_touches(self):
        """The total touch count in the circuit"""
        return self._circuit.touches.count()

    @property
    @cache_to_property("_neurons_touch_counts")
    def neurons_touch_counts(self):
        """Lazily calculate neurons_touch_counts
        """
        # Cache is not being used since it will just consume memory and potentially avoid optimizations
        # User can still cache explicitly the returned ds
        return self.get_neurons_touch_counts(self._circuit)

    @property
    @cache_to_property("_pathway_touches_conns")
    def pathway_touch_stats(self):
        # We better not cache yet, as there may be further calculations/cache
        return self.get_pathway_touch_stats_from_touch_counts(self.neurons_touch_counts)

    @staticmethod
    def get_neurons_touch_counts(circuit):
        """ Counts the total touches between morphologies and neurons.
        """
        return circuit.dataframe \
                      .select(
                          to_pathway_i("src_morphology_i", "dst_morphology_i"),
                          col("src"),
                          col("dst")
                      ) \
                      .groupBy("pathway_i", "src", "dst") \
                      .count()

    @staticmethod
    def get_pathway_touch_stats_from_touch_counts(neurons_touch_counts):
        """For every pathway (src-dst mtype) calc the number of touches, connections, and the mean (touches/connection)
        """
        # Group by pathway
        pathway_touches_conns = (
            neurons_touch_counts
            .groupBy("pathway_i").agg(
                F.sum("count").alias("total_touches"),
                F.count("pathway_i").alias("total_connections")
            )
        )

        return (
            pathway_touches_conns.withColumn(
                "average_touches_conn",
                pathway_touches_conns.total_touches / pathway_touches_conns.total_connections
            )
            # The resulting DF is very small (pathway^2~> 10K to 1M)
            # Anyway the suffle read can be ~ 1/10 size of touches
            .coalesce(pathway_touches_conns.rdd.getNumPartitions()//4)
        )

    @staticmethod
    def get_pathway_touch_stats_from_touches_with_pathway(touches_with_pathway):
        """For every pathway (src-dst mtype) calc the number of touches, connections, and the mean (touches/connection)    
        """
        neurons_touch_counts = touches_with_pathway.groupBy("pathway_i", "src", "dst").count()
        return NeuronStats.get_pathway_touch_stats_from_touch_counts(neurons_touch_counts)

    
class MTYPE_STATS_FIELDS:
    PRE_MORPHOLOGY = 0
    POST_MORPHOLOGY = 1
    TOTAL_TOUCHES = 2
    TOTAL_CONNECTIONS = 3
    AVERAGE_SYN_TOUCHES = 4
