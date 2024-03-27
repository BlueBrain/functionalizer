export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/circuit_config.json
export RECIPE=$BASE/bioname/recipe.json
export TOUCHES=$BASE/touches/parquet/*.parquet

srun dplace functionalizer \
    -H \
    --gap-junctions \
    --output-dir="$PWD" \
    --checkpoint-dir="$PWD" \
    --circuit-config=$CIRCUIT \
    --recipe=$RECIPE \
    -- $TOUCHES
