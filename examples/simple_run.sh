#!/usr/bin/env bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CIRCUIT_DIR=$SCRIPTPATH/../tests/circuit_1000n
MORPHO_DIR="."
spykfunc $CIRCUIT_DIR/builderRecipeAllPathways.xml \
         $CIRCUIT_DIR/nodes.h5 \
         $MORPHO_DIR \
         $CIRCUIT_DIR/touches/touchesData.*.parquet \
         -p "spark.driver.memory=4G"

