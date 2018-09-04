modes=(s2s s2f)
labels=(structural functional)
for i in $(seq 1 2); do
    for m in $(seq 0 $((${#modes[*]} - 1))); do
        fn="test_circuit${i}k_${modes[$m]}.sh"
        cat >$fn <<EOS
export BASE=\$DATADIR/cellular/circuit-${i}k/
export CIRCUIT=\$BASE/circuit.mvd3
export MORPHOS=\$BASE/morphologies/h5
export RECIPE=\$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=\$BASE/touches/parquet/*.parquet
salloc -Aproj16 -pinteractive -Cnvme -N1 --exclusive --mem=0 \\
    sm_run -H \\
        spykfunc --${modes[$m]} \\
                 --output-dir=\$PWD \\
                 --checkpoint-dir=\$PWD \\
                 -p spark.master=spark://\\\$\\(hostname\\):7077 \\
                 \$RECIPE \$CIRCUIT \$MORPHOS \$TOUCHES

script=\$(mktemp)
cat >\$script <<EOF
import glob, sys, pyarrow.parquet as pq
sort_cols = ['connected_neurons_post', 'connected_neurons_pre', 'delay']
base, comp = [pq.ParquetDataset(glob.glob(d)).read().to_pandas() \\
                .sort_values(sort_cols).reset_index(drop=True)
              for d in sys.argv[1:]]
print("comparison " + ("successful" if base.equals(comp) else "failed"))
sys.exit(0 if base.equals(comp) else 1)
EOF

python \$script circuit.parquet \$DATADIR/cellular/circuit-${i}k/touches/${labels[$m]}/circuit.parquet
EOS
    done
done
