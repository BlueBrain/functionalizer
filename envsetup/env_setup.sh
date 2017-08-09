CURDIR=$HOME/dev/Functionalizer/spykfunc
. $HOME/dev/sparkenv/bin/activate
export PYTHONPATH=$HOME/dev/Functionalizer:$PYTHONPATH
export PYSPARK_DRIVER_PYTHON=ipython
#export PYSPARK_DRIVER_PYTHON_OPTS="-i tests/test_functionalizer.py"
export SPARK_CONF_DIR=$CURDIR/spark_conf
#export PYSPARK_ARGS="--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11"
