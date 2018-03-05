from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from .schema import to_pathway_i
from .utils import assign_to_property
from .utils.spark import reduce_number_shuffle_partitions

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

    # Some of the following properties/methods assign the QueryPlan (DF not cached) to a property
    # so that further calls to the same query may reuse shuffles
    # ---
    @property
    @assign_to_property("_total_neurons", use_as_cache=True)
    def total_neurons(self):
        """The total neuron count in the circuit"""
        return self._circuit.neurons.count()

    @property
    @assign_to_property("_total_touches", True)
    def total_touches(self):
        """The total touch count in the circuit"""
        return self._circuit.touches.count()

    @property
    @assign_to_property("_neurons_touch_counts", True)
    def neurons_touch_counts(self):
        """Lazily calculate neurons_touch_counts
        """
        # Cache is not being used since it will just consume memory and potentially avoid optimizations
        # User can still cache explicitly the returned ds
        return self.get_neurons_touch_counts(self._circuit)

    @property
    @assign_to_property("_pathway_touches_conns", True)
    def pathway_touch_stats(self):
        return self.get_pathway_touch_stats_from_touch_counts(self.neurons_touch_counts)    

    @staticmethod
    def get_neurons_touch_counts(circuit):
        """ Counts the total touches between morphologies and neurons.
        """
        neurons = circuit.neurons
        touches = circuit.touches

        touches_with_pathway = (
            touches.alias("t")
            .join(neurons.alias("n1"), neurons.id == touches.src)
            .join(neurons.alias("n2"), neurons.id == touches.dst)
            .select(to_pathway_i("n1.morphology_i", "n2.morphology_i"),
                    col("t.src"),
                    col("t.dst"))
        )
        return NeuronStats.get_connections_counts_from_touches_with_pathway(touches_with_pathway)


    @staticmethod
    def get_connections_counts_from_touches_with_pathway(touches):
        connections_counts = (
            touches
            .groupBy("pathway_i", "src", "dst")
            .agg(F.count("*").cast(IntegerType()).alias("count"))
        )
        return connections_counts
        

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
            .withColumn(
                "average_touches_conn",
                F.col("total_touches") / "total_connections"
            )
        )

        return reduce_number_shuffle_partitions(pathway_touches_conns, 16, 4, 128)


    @staticmethod
    def get_pathway_touch_stats_from_touches_with_pathway(touches):
        """For every pathway (src-dst mtype) calc the number of touches, connections, and the mean (touches/connection)    
        """
        neurons_touch_counts = NeuronStats.get_connections_counts_from_touches_with_pathway(touches)
        return NeuronStats.get_pathway_touch_stats_from_touch_counts(neurons_touch_counts)

    
class MTYPE_STATS_FIELDS:
    PRE_MORPHOLOGY = 0
    POST_MORPHOLOGY = 1
    TOTAL_TOUCHES = 2
    TOTAL_CONNECTIONS = 3
    AVERAGE_SYN_TOUCHES = 4
