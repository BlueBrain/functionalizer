##################################################################
# DEFINE THE FOLLOWING PARAMS IN ANOTHER SBATCH AND THEN SOURCE ME
##################################################################
# #!/bin/bash
# #SBATCH --job-name spykfunc_optimize
# #SBATCH -N1 -n1 --exclusive --mem=0
# #SBATCH --account=proj16 --partition=prod 
# #SBATCH --constraint=nvme 
# #SBATCH --time 8:00:00
# #SBATCH -d singleton
#
# N_RUNS=2
# OVERWRITE=0
# RUN_NAME="run"

# CIRCUIT=/gpfs/bbp.cscs.ch/project/proj16/spykfunc/circuits/10x10
# EXEC_PER_NODE=2
# CORES_PER_EXEC=20
###################################################################

THISDIR=$(dirname "$BASH_SOURCE")
DATA=$HOME/spykfunc
source $DATA/env_setup.sh

set -e

CURSM=0
SMDIR="_smdata$CUR_SM"
while [ -d $SMDIR ]; do
    CUR_SM=$((CUR_SM+1))
    SMDIR="_smdata$CUR_SM"
done
mkdir $SMDIR

if [ -n "$SPARK_MASTER" ]; then
    echo "Attaching to cluster master $SPARK_MASTER"
    echo "$SPARK_MASTER" > $SMDIR/spark_master
else
    echo "Starting the cluster up"
    sm_cluster startup $SMDIR $DATA/env_setup.sh &
    sleep $((SLURM_JOB_NUM_NODES+10))
fi

for i in `seq 1 $N_RUNS`; do
  RUNDIR="$RUN_NAME.$i"
  [ $OVERWRITE ] && rm -rf $RUNDIR
  [ -d $RUNDIR ] || mkdir $RUNDIR
  (
    cd $RUNDIR
    ln -sf ../$SMDIR/spark_master
    echo "Launching the driver"
    export SPARK_LOCAL_DIR="/nvme/leite/$SLURM_JOB_ID"
    sh $THISDIR/../sfrun.sh $CIRCUIT $SLURM_JOB_NUM_NODES $EXEC_PER_NODE $CORES_PER_EXEC
    mv spykfunc_output/report.json ../report.$RUN_NAME.$i.json
  )
  rm -rf $RUNDIR
done

rm -rf $SMDIR

