echo TMPDIR is defined as ${TMPDIR} on $(hostname)
script=$(mktemp -p .)
cat >$script <<EOF
cd $WORKSPACE
export SPARK_LOCAL_DIRS=/\${TMPDIR}/\${SLURM_JOBID}/spark-local
export SPARK_WORKER_DIR=/\${TMPDIR}/\${SLURM_JOBID}/spark-worker
export PYTHONPATH=$PWD:$PYTHONPATH
python setup.py test
EOF

module load unstable boost cmake

part=$([ $(date +%H) -gt 8 -a $(date +%H) -lt 20 ] && echo interactive || echo prod)
# srun here rather than salloc since the latter executes on the local
# machine, with internal sruns running on remote nodes with requested
# resources.  `sm_run` needs salloc, here we want to be on an /nvme node!
srun --mpi=none -Aproj16 -p$part -Cnvme -N1 --exclusive --mem=0 \
    sh $script
