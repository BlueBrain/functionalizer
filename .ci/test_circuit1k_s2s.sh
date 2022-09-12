export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/nodes.h5
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

srun functionalizer \
    -H \
    --s2s \
    --output-dir=$PWD \
    --checkpoint-dir=$PWD \
    --from $CIRCUIT All --to $CIRCUIT All \
    --recipe $RECIPE \
    --morphologies $MORPHOS \
    -- $TOUCHES

parquet-compare \
    $CIRCUIT \
    circuit.parquet \
    $BASE/touches/structural/circuit.parquet
