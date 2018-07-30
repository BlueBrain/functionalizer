export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/circuit.mvd3
export MORPHOS=$BASE/morphologies/h5
export RECIPE=$BASE/builderRecipeAllPathways.xml
export TOUCHES=$BASE/touches/parquet/*.parquet
salloc -Aproj16 -pinteractive -N1 --exclusive --mem=0 \
    sm_run -H \
        spykfunc --s2s \
                 --output-dir=$PWD \
                 --checkpoint-dir=$PWD \
                 -p spark.master=spark://\$\(hostname\):7077 \
                 $RECIPE $CIRCUIT $MORPHOS $TOUCHES

script=$(mktemp)
cat >$script <<EOF
import glob, sys, pyarrow.parquet as pq
sort_cols = ['connected_neurons_post', 'connected_neurons_pre', 'delay']
base, comp = [pq.ParquetDataset(glob.glob(d)).read().to_pandas()                 .sort_values(sort_cols).reset_index(drop=True)
              for d in sys.argv[1:]]
print("comparison " + ("successful" if base.equals(comp) else "failed"))
sys.exit(0 if base.equals(comp) else 1)
EOF

python $script circuit.parquet $DATADIR/cellular/circuit-1k/touches/structural/circuit.parquet
