export BASE=$DATADIR/cellular/circuit-1k/
export CIRCUIT=$BASE/circuit_config.json
export RECIPE=$BASE/bioname/recipe.json
export TOUCHES=$BASE/touches/parquet/*.parquet

srun dplace functionalizer \
    -H \
    --filters SynapseProperties,SpineMorphologies \
    --output-dir="$PWD" \
    --checkpoint-dir="$PWD" \
    --circuit-config=$CIRCUIT \
    --recipe=$RECIPE \
    -- $TOUCHES

python << EOF
import numpy as np
import pandas as pd
df = pd.read_parquet("circuit.parquet")
assert len(df["spine_morphology"].unique()) > 1
assert set(df["spine_psd_id"].unique()) == set([-1, 0, 1])
assert np.all(df["spine_sharing_id"] == -1)  # currently not yet implemented
EOF
