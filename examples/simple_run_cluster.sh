#!/usr/bin/env bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CIRCUIT_DIR=$SCRIPTPATH/../tests/circuit_1000n
MORPHO_DIR="."

spykfunc $CIRCUIT_DIR/builderRecipeAllPathways.xml \
         $CIRCUIT_DIR/circuit.mvd3 \
         $MORPHO_DIR \
         $CIRCUIT_DIR/touches/touches.0 \
         --spark-opts "--master spark://localhost:7077 --executor-memory 2G"

# possible options to spark:
# --driver-memory 8G
# --executor-memory 32G
