part=$([ $(date +%H) -gt 8 -a $(date +%H) -lt 20 ] && echo interactive || echo prod)

export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/nodes.h5
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

salloc -p$part -Cnvme -N1 --exclusive --mem=0 \
    sm_run -H \
        spykfunc --s2s \
                 --output-dir=$PWD \
                 --checkpoint-dir=$PWD \
                 --from $CIRCUIT All --to $CIRCUIT All \
                 --from-nodeset $BASE/nodesets.json test \
                 --to-nodeset $BASE/nodesets.json test \
                 $RECIPE $MORPHOS \
                 --parquet $TOUCHES

parquet-coalesce circuit.parquet single_nodesets.parquet
parquet-compare-ns \
    $CIRCUIT \
    $BASE/touches/structural/circuit.parquet \
    single_nodesets.parquet \
    4
