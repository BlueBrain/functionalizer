#!/bin/sh

SYNTAX="sfrun.sh CIRCUIT_DIR [NUM_NODES] [EXEC_PER_NODE] [CORES_PER_EXEC]"

CIRCUIT_DIR=$1
# Num nodes is second arg. Defaults to 1
NUM_NODES=${2:-1}
# Num exec per node. Default: 2
EXEC_PER_NODE=${3:-2}
# Num cores per exec. Default: 16
CORES_PER_EXEC=${4:-16}

if [[ -z $CIRCUIT_DIR || ! -d $CIRCUIT_DIR ]]; then
    echo "$SYNTAX"
    exit
fi
set -e

info=0
while [[ ! -f spark_master ]]; do 
    [ $info ] || echo "Waiting for the cluster" && info=1
    sleep 1
    printf "."
done

_LOCALDIR=""
if [ -n $SPARK_LOCAL_DIR ]; then
    _LOCALDIR="-p spark.local.dir=$SPARK_LOCAL_DIR"
fi

MASTER=$(cat spark_master)
MAX_CORES=$((NUM_NODES * EXEC_PER_NODE * CORES_PER_EXEC))

echo "Running with $NUM_NODES nodes * $EXEC_PER_NODE executors * $CORES_PER_EXEC cores. TOTAL: $MAX_CORES cores"

set -x
spykfunc $CIRCUIT_DIR/builderRecipeAllPathways.xml $CIRCUIT_DIR/circuit.mvd3 $CIRCUIT_DIR/morphologies/h5 $CIRCUIT_DIR/touches/\*.parquet -p spark.master=$MASTER -p spark.driver.memory=8G -p spark.executor.memory=64G -p spark.executor.cores=$CORES_PER_EXEC -p spark.cores.max=$MAX_CORES -p spark.ui.showConsoleProgress=false $_LOCALDIR

