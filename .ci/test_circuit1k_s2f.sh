export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/nodes.h5
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

sm_run -H \
    spykfunc --s2f \
             --output-dir="$PWD" \
             --checkpoint-dir="$PWD" \
             --from $CIRCUIT All --to $CIRCUIT All \
             --recipe $RECIPE \
             --morphologies $MORPHOS \
             $TOUCHES

parquet-compare \
    $CIRCUIT \
    circuit.parquet \
    $BASE/touches/functional/circuit.parquet
