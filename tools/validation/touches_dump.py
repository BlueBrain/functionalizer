"""Dump output stats
"""
import argparse
import cPickle
import os
import sparkmanager as sm

import pyspark.sql.functions as F

from spykfunc.data_loader import NeuronDataSpark
from spykfunc.dataio import cppneuron

parser = argparse.ArgumentParser()
parser.add_argument("--spark-opts", help="Spark options to be passed through")
parser.add_argument("neurons", help="a neuron data file")
parser.add_argument("morphologies", help="a directory with morphology data")
parser.add_argument("old", help="baseline touches")
parser.add_argument("new", help="comparison touches")
parser.add_argument("output", help="the output directory")
args = parser.parse_args()

if not os.path.exists(args.output):
    os.makedirs(args.output)

jar = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'java', 'spykfunc_udfs.jar')
sm.create("validation", config={'spark.jars': jar}, options=args.spark_opts)

touches_old = sm.read.parquet(args.old)
touches_new = sm.read.parquet(args.new)

neuron_data = NeuronDataSpark(cppneuron.MVD_Morpho_Loader(args.neurons, args.morphologies), "_mvd")
neuron_data.load_mvd_neurons_morphologies()
neurons = neuron_data.neuronDF


def prefixed(pre):
    tmp = neurons
    for col in tmp.schema.names:
        tmp = tmp.withColumnRenamed(col, pre if col == "id" else "{}_{}".format(pre, col))
    return tmp


circuit_old = touches_old.withColumn("src", touches_old.pre_gid - 1) \
                         .withColumn("dst", touches_old.post_gid - 1) \
                         .join(F.broadcast(prefixed("src")), "src") \
                         .join(F.broadcast(prefixed("dst")), "dst")
circuit_new = touches_new.withColumn("src", touches_new.pre_gid - 1) \
                         .withColumn("dst", touches_new.post_gid - 1) \
                         .join(F.broadcast(prefixed("src")), "src") \
                         .join(F.broadcast(prefixed("dst")), "dst")


def count(circuit, fix):
    res = circuit.groupBy('src_morphology', 'dst_morphology')
    return res.count().withColumnRenamed("count", "count_" + fix)


def avgs(circuit, columns, fix):
    res = circuit.groupBy('src_morphology', 'dst_morphology')
    aggs = []
    for column in columns:
        aggs.extend((F.mean(getattr(circuit, column)).alias(column + "_avg_" + fix),
                     F.stddev(getattr(circuit, column)).alias(column + "_dev_" + fix)))
    return res.agg(*aggs)


counts_old = count(circuit_old, "old")
counts_new = count(circuit_new, "new")

res = counts_old.join(counts_new,
                     [counts_old.src_morphology == counts_new.src_morphology,
                      counts_old.dst_morphology == counts_new.dst_morphology])
counts = res.toPandas()
with open(os.path.join(args.output, 'counts.pkl'), 'wb') as fd:
    cPickle.dump(counts, fd, protocol=2)

sample = counts.sample(18)
for index, row in sample.iterrows():
    src_morphology = row.src_morphology[0]
    dst_morphology = row.dst_morphology[0]
    cols = "u d f gsyn nrrp".split()
    data_old = circuit_old.where((circuit_old.src_morphology == src_morphology) &
                                 (circuit_old.dst_morphology == dst_morphology)) \
                          .withColumnRenamed('ase', 'nrrp') \
                          .select(cols).toPandas()
    with open(os.path.join(args.output, 'data_{}_{}_old.pkl'.format(src_morphology, dst_morphology)), 'wb') as fd:
        cPickle.dump(data_old, fd, protocol=2)
    data_new = circuit_new.where((circuit_new.src_morphology == src_morphology) &
                                 (circuit_new.dst_morphology == dst_morphology)) \
                          .withColumnRenamed('ase', 'nrrp') \
                          .select(cols).toPandas()
    with open(os.path.join(args.output, 'data_{}_{}_new.pkl'.format(src_morphology, dst_morphology)), 'wb') as fd:
        cPickle.dump(data_new, fd, protocol=2)
