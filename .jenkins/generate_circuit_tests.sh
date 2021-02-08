modes=(s2s s2f)
labels=(structural functional)
for i in $(seq 1 2); do
    for m in $(seq 0 $((${#modes[*]} - 1))); do
        fn="test_circuit${i}k_${modes[$m]}.sh"
        cat >$fn <<EOS
part=\$([ \$(date +%H) -gt 8 -a \$(date +%H) -lt 20 ] && echo interactive || echo prod)

export BASE=\$DATADIR/cellular/circuit-${i}k/
export CIRCUIT=\$BASE/nodes.h5
export MORPHOS=\$BASE/morphologies/h5
export RECIPE=\$BASE/bioname/builderRecipeAllPathways.xml
export TOUCHES=\$BASE/touches/parquet/*.parquet

salloc -Aproj16 -p\$part -Cnvme -N1 --exclusive --mem=0 \\
    sm_run -H \\
        spykfunc --${modes[$m]} \\
                 --output-dir=\$PWD \\
                 --checkpoint-dir=\$PWD \\
                 -p spark.master=spark://\\\$\\(hostname\\):7077 \\
                 \$RECIPE \$CIRCUIT \$MORPHOS \$TOUCHES

parquet-coalesce circuit.parquet single.parquet
parquet-compare \\
    \$CIRCUIT \\
    single.parquet \\
    \$BASE/touches/${labels[$m]}/circuit.parquet
EOS
    done
done
