#!/usr/bin/env bash
PROJ_DIR=$HOME/dev/Functionalizer
CURDIR=$HOME/dev/Functionalizer/spykfunc
. $HOME/dev/sparkenv/bin/activate
export PYSPARK_DRIVER_PYTHON=ipython
export SPARK_CONF_DIR=$CURDIR/spark_conf

# Set Spark paths
export PYTHONPATH="/home/leite/usr/spark-2.2.0/python":$PYTHONPATH
export PATH=/home/leite/usr/spark-2.2.1/bin:$PATH

export SPARK_OPTS="$ss --conf spark.sql.shuffle.partitions=$((`nproc`*4)) --jars java/spykfunc_udfs.jar"
