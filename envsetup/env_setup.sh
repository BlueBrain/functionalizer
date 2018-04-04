#!/usr/bin/bash -i
###########################################
# Prepare/Load a Python env to run spark
##########################################

set -e

BASEDIR=$HOME/dev/Functionalizer/pyspark
CURDIR=$BASEDIR/envsetup
PYENV=$CURDIR/sparkenv
export SPARK_HOME=/home/leite/usr/spark-2.2.1

if [ ! -d $PYENV ]; then
    echo "Creating virtualenv in $PYENV"
    virtualenv $PYENV -p `which python`
    . $PYENV/bin/activate
    pip install --upgrade setuptools pip
    pip install -e $SPARK_HOME/python  # Access spark via install develop
    pip install -r $BASEDIR/dev-requirements.txt
    # Build install spykfunc
    rm $BASEDIR/build -rf
    BUILD_TYPE=DEVEL pip install -e $BASEDIR[dev]
else
    . $PYENV/bin/activate
fi

export SPARK_CONF_DIR=$CURDIR/spark_conf

# Set Spark paths
export PATH=$SPARK_HOME/bin:$PATH

# Options to pyspark for running spykfunc
export SPYKFUNC_SPARK_OPTS="--conf spark.sql.shuffle.partitions=$((`nproc`*4)) --jars java/spykfunc_udfs.jar"

function pyspark_ipython {
  PYSPARK_DRIVER_PYTHON=ipython pyspark $SPYKFUNC_SPARK_OPTS
}
export -f pyspark_ipython

# Start subshell
bash

