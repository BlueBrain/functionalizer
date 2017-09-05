###########################################
# Prepare/Load a Python env to run spark
##########################################

BASEDIR=$HOME/dev/Functionalizer/pyspark
CURDIR=$BASEDIR/envsetup
PYENV=$CURDIR/sparkenv

if [ ! -d $PYENV ]; then
    echo "Creating virtualenv in $PYENV"
    python -m virtualenv $PYENV
    . $PYENV/bin/activate
    pip install --upgrade setuptools pip
    pip install "ipython<6"
    pip install -r $BASEDIR/requirements.txt
    pip install -r $BASEDIR/test-requirements.txt
    pip install -e $BASEDIR[dev]
else
    . $PYENV/bin/activate
fi

export SPARK_HOME=$HOME/usr/spark-2.1.1
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python

export PYSPARK_DRIVER_PYTHON=ipython
export SPARK_CONF_DIR=$CURDIR/spark_conf_sage

export MODULEPATH=/home/kumbhar/SPACK_INSTALL/modules/linux-rhel7-x86_64/
module load hdf5

# Avoid add things to PYTHONPATH. Use setup.py develop (pip install -e .)
# PYTHONPATH = $CURDIR

#export PYSPARK_ARGS="--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11"
#export PYSPARK_DRIVER_PYTHON_OPTS="-i tests/test_functionalizer.py"
#export SPARK_WORKER_DIR=/gpfs/bbp.cscs.ch/scratch/gss/spark/leite
#export SPARK_LOG_DIR=/gpfs/bbp.cscs.ch/scratch/gss/spark/leite

