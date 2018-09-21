script=$(mktemp)
cat >$script <<EOF
# Dependencies to build test environment
module load nix/boost
module load nix/hdf5
module load nix/hpc/highfive

cd $WORKSPACE
git submodule update --init --recursive
export SPARK_LOCAL_DIRS=/nvme/\$(whoami)/\$SLURM_JOBID/spark-local
export SPARK_WORKER_DIR=/nvme/\$(whoami)/\$SLURM_JOBID/spark-worker
python setup.py test
EOF

part=$([ $(date +%H) -gt 8 -a $(date +%H) -lt 20 ] && echo interactive || echo prod)
# srun here rather than salloc since the latter executes on the local
# machine, with internal sruns running on remote nodes with requested
# resources.  `sm_run` needs salloc, here we want to be on an /nvme node!
srun -Aproj16 -p$part -Cnvme -N1 --exclusive --mem=0 \
    sh $script
