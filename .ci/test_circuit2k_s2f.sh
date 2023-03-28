export BASE=$DATADIR/cellular/circuit-2k/
export CIRCUIT=$BASE/nodes.h5
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

srun dplace functionalizer \
    --s2f \
    --output-dir=$PWD \
    --from $CIRCUIT All --to $CIRCUIT All \
    --recipe $RECIPE \
    --morphologies $MORPHOS \
    -- $TOUCHES

parquet-compare \
    $CIRCUIT \
    circuit.parquet \
    $BASE/touches/functional/circuit.parquet
