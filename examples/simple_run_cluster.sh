#!/usr/bin/env bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CIRCUIT_DIR=$SCRIPTPATH/../tests/circuit_1000n
MORPHO_DIR="."

spykfunc $CIRCUIT_DIR/builderRecipeAllPathways.xml \
         $CIRCUIT_DIR/circuit.mvd3 \
         $MORPHO_DIR \
         $CIRCUIT_DIR/touches/touchesData.*.parquet \
         -p "spark.master=spark://localhost:7077" \
	 -p "spark.executor.memory=2G"

# possible options to spark:
# --driver-memory 8G
# --executor-memory 32G
