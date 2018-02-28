#!/usr/bin/env bash
BASE_DIR="circuit_1000n"
recipe_file="$BASE_DIR/recipe/builderRecipeAllPathways.xml"
mvd_file="$BASE_DIR/circuits/circuit.mvd3"
morpho_dir="$BASE_DIR/morphologies/h5"
touch_files="$BASE_DIR/BlueDetector_output/touches.0"

spark-submit \
    spykfunc/commands.py -- \
    $recipe_file \
    $mvd_file \
    $morpho_dir \
    $touch_files
