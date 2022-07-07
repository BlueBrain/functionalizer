export BASE=$DATADIR/cellular/circuit-1k/
export NODES=$BASE/nodes.h5
export NODESETS=$BASE/nodesets.json
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

for half in empty full; do
    sm_run -H \
        spykfunc --s2f \
                 --output-dir="$PWD/half_${half}_out" \
                 --checkpoint-dir="$PWD/half_${half}_check" \
                 --from-nodeset $NODESETS half_$half \
                 --from $NODES All --to $NODES All \
                 --recipe $RECIPE \
                 --morphologies $MORPHOS \
                 -- $TOUCHES
done

sm_run -H \
    spykfunc --merge \
             --output-dir="$PWD/merged_out" \
             --checkpoint-dir="$PWD/merged_check" \
             $PWD/half_*_out/circuit.parquet

parquet-compare \
    $NODES \
    merged_out/circuit.parquet \
    $BASE/touches/functional/circuit.parquet
