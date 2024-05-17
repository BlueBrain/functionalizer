export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/circuit_config.json
export RECIPE=$BASE/bioname/recipe.json
export TOUCHES=$BASE/touches/parquet/*.parquet

srun dplace functionalizer \
    -H \
    --s2s \
    --output-dir=$PWD \
    --checkpoint-dir=$PWD \
    --from-nodeset test \
    --to-nodeset test \
    --circuit-config=$CIRCUIT \
    --recipe=$RECIPE \
    -- $TOUCHES

parquet-compare-ns \
    ${CIRCUIT%circuit_config.json}nodes.h5 \
    $BASE/touches/structural/circuit.parquet \
    circuit.parquet \
    0
