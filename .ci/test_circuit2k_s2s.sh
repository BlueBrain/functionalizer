export BASE=$DATADIR/cellular/circuit-2k/
export CIRCUIT=$BASE/circuit_config.json
export RECIPE=$BASE/bioname/recipe.json
export TOUCHES=$BASE/touches/parquet/*.parquet

srun dplace functionalizer \
    -H \
    --s2s \
    --output-dir="$PWD" \
    --checkpoint-dir="$PWD" \
    --circuit-config=$CIRCUIT \
    --recipe=$RECIPE \
    -- $TOUCHES

parquet-compare \
    ${CIRCUIT%circuit_config.json}nodes.h5 \
    circuit.parquet \
    $BASE/touches/structural/circuit.parquet
