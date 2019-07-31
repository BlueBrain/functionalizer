part=$([ $(date +%H) -gt 8 -a $(date +%H) -lt 20 ] && echo interactive || echo prod)

export BASE=$DATADIR/cellular/circuit-2k/
export CIRCUIT=$BASE/circuit.mvd3
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet

salloc -Aproj16 -p$part -Cnvme -N1 --exclusive --mem=0 \
    sm_run -H \
        spykfunc --s2f \
                 --output-dir=$PWD \
                 --checkpoint-dir=$PWD \
                 -p spark.master=spark://\$\(hostname\):7077 \
                 $RECIPE --circuit $CIRCUIT $MORPHOS $TOUCHES

parquet-coalesce circuit.parquet single.parquet
parquet-compare \
    $DATADIR/cellular/circuit-2k/circuit.mvd3 \
    single.parquet \
    $DATADIR/cellular/circuit-2k/touches/functional/circuit.parquet
