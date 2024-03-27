export BASE=$DATADIR/cellular/circuit-1k/
export NODES=$BASE/nodes.h5
export CIRCUIT=$BASE/circuit_config.json
export NODESETS=$BASE/nodesets.json
export RECIPE=$BASE/bioname/recipe.json
export TOUCHES=$BASE/touches/parquet/*.parquet

for half in empty full; do
    srun dplace functionalizer \
        -H \
        --s2f \
        --output-dir="$PWD/half_${half}_out" \
        --checkpoint-dir="$PWD/half_${half}_check" \
        --from-nodeset half_$half \
        --circuit-config=$CIRCUIT \
        --recipe=$RECIPE \
        -- $TOUCHES
done

srun dplace functionalizer \
    -H \
    --merge \
    --output-dir="$PWD/merged_out" \
    --checkpoint-dir="$PWD/merged_check" \
    $PWD/half_*_out/circuit.parquet

parquet-compare \
    $NODES \
    merged_out/circuit.parquet \
    $BASE/touches/functional/circuit.parquet
