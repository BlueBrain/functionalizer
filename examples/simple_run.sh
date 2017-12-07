#!/usr/bin/env bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CIRCUIT_DIR=$SCRIPTPATH/../tests/circuit_1000n
MORPHO_DIR="."
spykfunc $CIRCUIT_DIR/builderRecipeAllPathways.xml \
         $CIRCUIT_DIR/circuit.mvd3 \
         $MORPHO_DIR \
         $CIRCUIT_DIR/touches/touches.0 \
         --spark-opts "--driver-memory 4G"

