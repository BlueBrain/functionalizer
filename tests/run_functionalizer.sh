#!/usr/bin/env bash
BASE_DIR="/home/leite/dev/TestData/circuitBuilding_1000neurons"
recipe_file="$BASE_DIR/recipe/builderRecipeAllPathways.xml"
mvd_file="$BASE_DIR/circuits/circuit.mvd3"
morpho_dir="$BASE_DIR/morphologies/h5"
touch_files="$BASE_DIR/BlueDetector_output/touches.0"

spark-submit --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 \
    commands.py -- \
    $recipe_file \
    $mvd_file \
    $morpho_dir \
    $touch_files
