#!/usr/bin/env bash
###########################################
# Prepare/Load a Python env to run spark
##########################################

BASEDIR=$HOME/dev/Functionalizer/pyspark
CURDIR=$BASEDIR/envsetup
PYENV=$CURDIR/sparkenv

module purge
module load nix/gcc
module load nix/hdf5
module load spark


if [ ! -d $PYENV ]; then
    echo "Creating virtualenv in $PYENV"
    module load nix/python/2.7-light
    virtualenv $PYENV -p `which python`
    module unload python
    . $PYENV/bin/activate
    pip install --upgrade setuptools pip
    pip install "ipython<6"
    pip install -r $BASEDIR/requirements.txt
    pip install -r $BASEDIR/test-requirements.txt
    pip install -r $BASEDIR/dev-requirements.txt
    #pip install -e $BASEDIR[dev]
else
    . $PYENV/bin/activate
fi

#export PATH=$HOME/usr/spark-2.1.1/bin:$HOME/usr/jdk1.8.0_144/jre/bin:$PATH
#export JAVA_HOME=$HOME/usr/jdk1.8.0_144/jre

export PYSPARK_DRIVER_PYTHON=ipython
export SPARK_CONF_DIR=$CURDIR/spark_conf_viz
# Avoid add things to PYTHONPATH. Use setup.py develop (pip install -e .)
# PYTHONPATH = $CURDIR

#export PYSPARK_ARGS="--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11"
#export PYSPARK_DRIVER_PYTHON_OPTS="-i tests/test_functionalizer.py"
#export SPARK_WORKER_DIR=/gpfs/bbp.cscs.ch/scratch/gss/spark/leite
#export SPARK_LOG_DIR=/gpfs/bbp.cscs.ch/scratch/gss/spark/leite

