#!/usr/bin/env bash
BASE_DIR="circuit_1000n"
recipe_file="$BASE_DIR/recipe/builderRecipeAllPathways.xml"
circuit_file="$BASE_DIR/circuits/nodes.h5"
morpho_dir="$BASE_DIR/morphologies/h5"
touch_files="$BASE_DIR/BlueDetector_output/touchesData.*.parquet"

spark-submit \
    spykfunc/commands.py -- \
    $recipe_file \
    $circuit_file \
    $morpho_dir \
    $touch_files
