part=$([ $(date +%H) -gt 8 -a $(date +%H) -lt 20 ] && echo interactive || echo prod)

export BASE=$DATADIR/cellular/circuit-1k/
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
                 $RECIPE $CIRCUIT $MORPHOS $TOUCHES

script=$(mktemp)
cat >$script <<EOF
import glob, sys, pyarrow.parquet as pq
sort_cols = ['connected_neurons_post', 'connected_neurons_pre', 'delay']
base, comp = [pq.ParquetDataset(glob.glob(d)).read().to_pandas() \\
                .sort_values(sort_cols).reset_index(drop=True)
              for d in sys.argv[1:]]
eq = (base == comp).eq(True).all().all()
print("comparison " + ("successful" if eq else "failed"))
sys.exit(0 if eq else 1)
EOF

python $script circuit.parquet $DATADIR/cellular/circuit-1k/touches/functional/circuit.parquet
