#!/usr/bin/bash -i
###########################################
# Prepare/Load a Python env to run spark
##########################################

if [ "${BASH_SOURCE[0]}" != "$0" ]; then
    echo "Dont source this"
    return
fi

set -e

CURDIR=$(readlink -f "$(dirname "$0")")
PYENV="$CURDIR/sparkenv"
SPARK_FZER=$(readlink -f "$CURDIR/..")


if [[ ! -d $PYENV || $1 -eq "force" ]]; then

    if [ -n "$1" ]; then
        SPARK_FZER=$(readlink -f "$1")
    else
        if [ -f "$SPARK_FZER/setup.py" ]; then
            echo "Using DEFAULT SPARK FUNCZER location: $SPARK_FZER"
        else
            echo "Syntax: env_setup.sh SPARK_FZER_LOCATION"
            exit
        fi
    fi

    if [ -z "$SPARK_HOME"  ]; then
        echo "Please set SPARK_HOME before launching the script"
        exit
    fi

    if [ -d $PYENV ]; then
        echo "Creating virtualenv in $PYENV"
        virtualenv $PYENV -p `which python`
    fi
    . $PYENV/bin/activate
    pip install --upgrade setuptools pip
    pip install -e $SPARK_HOME/python  # Access spark via install develop

    # Spark conf
    SPARK_ENV_CONF=$CURDIR/spark_conf
    echo "BASEDIR=$CURDIR" > $SPARK_ENV_CONF/spark-env.sh
    cat $SPARK_ENV_CONF/spark-env.sh.tpl >> $SPARK_ENV_CONF/spark-env.sh

    # Build install spykfunc
    pip install -r $SPARK_FZER/dev-requirements.txt
    rm $SPARK_FZER/build -rf
    pip install -e $SPARK_FZER[dev]
else
    . $PYENV/bin/activate
fi

export SPARK_CONF_DIR=$CURDIR/spark_conf

# Options to pyspark for running spykfunc
export SPYKFUNC_SPARK_OPTS="-p spark.sql.shuffle.partitions=$((`nproc`*4))"

function pyspark_ipython {
  PYSPARK_DRIVER_PYTHON=ipython pyspark $SPYKFUNC_SPARK_OPTS
}
export -f pyspark_ipython

# Start subshell
bash

