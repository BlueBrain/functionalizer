export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/nodes.h5
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

sm_run -H \
    spykfunc --s2s \
             --output-dir=$PWD \
             --checkpoint-dir=$PWD \
             --from $CIRCUIT All --to $CIRCUIT All \
             --from-nodeset $BASE/nodesets.json test \
             --to-nodeset $BASE/nodesets.json test \
             --recipe $RECIPE \
             --morphologies $MORPHOS \
             -- $TOUCHES

parquet-compare-ns \
    $CIRCUIT \
    $BASE/touches/structural/circuit.parquet \
    circuit.parquet \
    4
