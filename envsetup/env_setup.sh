#!/usr/bin/env bash
PROJ_DIR=$HOME/dev/Functionalizer
CURDIR=$HOME/dev/Functionalizer/spykfunc
. $HOME/dev/sparkenv/bin/activate
export PYSPARK_DRIVER_PYTHON=ipython
export SPARK_CONF_DIR=$CURDIR/spark_conf
# Add spark Py path
#export PYTHONPATH="~/usr/local/spark-2.2.0/python":$PYTHONPATH

export PATH=~/usr/spark-2.2.0/bin:$PATH

export SPARK_OPTS="--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 $ss --conf spark.sql.shuffle.partitions=$((`nproc`*4)) --jars java/spykfunc_udfs.jar"
