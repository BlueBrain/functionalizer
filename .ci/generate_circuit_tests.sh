modes=(s2s s2f)
labels=(structural functional)
for i in $(seq 1 2); do
    for m in $(seq 0 $((${#modes[*]} - 1))); do
        fn="test_circuit${i}k_${modes[$m]}.sh"
        cat >$fn <<EOS
export BASE=\$DATADIR/cellular/circuit-${i}k/
export CIRCUIT=\$BASE/circuit_config.json
export RECIPE=\$BASE/bioname/recipe.json
export TOUCHES=\$BASE/touches/parquet/*.parquet

srun dplace functionalizer \\
    -H \\
    --${modes[$m]} \\
    --output-dir="\$PWD" \\
    --checkpoint-dir="\$PWD" \\
    --circuit-config=\$CIRCUIT \\
    --recipe=\$RECIPE \\
    -- \$TOUCHES

parquet-compare \\
    \${CIRCUIT%circuit_config.json}nodes.h5 \\
    circuit.parquet \\
    \$BASE/touches/${labels[$m]}/circuit.parquet
EOS
    done
done
