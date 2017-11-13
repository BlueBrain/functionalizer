from pyspark.sql import functions as F
from pyspark.sql.functions import col


class NeuronStats(object):
    """
    Retrieve and store the properties of the neurons,
    including statistics and MVD properties
    """

    def __init__(self):
        self.total_neurons = 0
        self._total_touches = 0
        self._touch_graph_frame = None
        self._prev_gf = None
        self._neurons_touch_counts = None
        self._morpho_touches_conns = None

    @staticmethod
    def create_from_touch_info(touch_info):
        """ Builds a NeuronStats object with some fields prefiled
            with information from the binary touch file header
        """
        obj = NeuronStats()
        obj.total_neurons = touch_info.header.numberOfNeurons
        # Total touches can be already expensive to compute for large sets
        obj._total_touches = touch_info.touch_count if obj.total_neurons < 10000 else None
        obj._prev_gf = True
        # We wont probably have all this info in the front node
        # obj.neuron_pre_touch_counts = {n_id: count for n_id, count, _ in touch_info.neuron_stats}
        return obj

    def update_touch_graph_source(self, touch_GF, overwrite_previous_gf=True):
        self._touch_graph_frame = touch_GF
        self._neurons_touch_counts = None
        self._morpho_touches_conns = None
        if overwrite_previous_gf:
            self._prev_gf = self._touch_graph_frame
            self.total_neurons = self._touch_graph_frame.vertices.count()

    @property
    def neurons_touch_counts(self):
        """Lazily calculate neurons_touch_counts
        """
        if not self._neurons_touch_counts:
            # Cache is not being used since it will just consume memory and potentially avoid optimizations
            # User can still cache explicitly the returned ds
            self._neurons_touch_counts = self.get_neurons_touch_counts(self._touch_graph_frame)
        return self._neurons_touch_counts

    @property
    def total_touches(self):
        if self._total_touches and self._prev_gf in (True, self._touch_graph_frame):
            return self._total_touches
        self._total_touches = self._touch_graph_frame.edges.count()
        self._prev_gf = self._touch_graph_frame
        return self._total_touches

    @property
    def pre_touch_counts(self):
        return self._touch_graph_frame.outDegrees

    @property
    def post_touch_counts(self):
        return self._touch_graph_frame.inDegrees

    @property
    def mtype_touch_stats(self):
        """For every pair of mtype (src-dst) calc the number of touches, connections, and the mean (touches/connection)
        """
        if self._morpho_touches_conns:
            return self._morpho_touches_conns

        neuron_touches = self.neurons_touch_counts

        # Group by morphos
        morpho_touches_conns = neuron_touches.groupBy("n1_morpho", "n2_morpho").agg(
            F.sum(col("count")).alias("total_touches"),
            F.count(col("*")).alias("total_connections"))

        morpho_touches_conns = morpho_touches_conns.withColumn(
            "average_touches_conn",
            morpho_touches_conns.total_touches / morpho_touches_conns.total_connections)

        self._morpho_touches_conns = morpho_touches_conns.cache()
        self._morpho_touches_conns.count()
        return self._morpho_touches_conns

    @staticmethod
    def get_neurons_touch_counts(neuronG):
        """ Counts the total touches between morphologies and neurons.
        """
        return neuronG.find("(n1)-[t]->(n2)").groupBy(
            col("n1.morphology").alias("n1_morpho"),
            col("n2.morphology").alias("n2_morpho"),
            col("n1.id").alias("n1_id"),
            col("n2.id").alias("n2_id"),
        ).count()


class MTYPE_STATS_FIELDS:
    PRE_MORPHOLOGY = 0
    POST_MORPHOLOGY = 1
    TOTAL_TOUCHES = 2
    TOTAL_CONNECTIONS = 3
    AVERAGE_SYN_TOUCHES = 4
