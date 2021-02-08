#!/bin/sh
#SBATCH --account=proj16
#SBATCH --partition=prod
#SBATCH --time=12:00:00
#SBATCH --job-name=fz_dev-O1
#SBATCH --output=std-%j.out
#SBATCH --error=std-%j.err
#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH --nodes=4
# #SBATCH --exclude=r2i1n21
# #SBATCH --qos=bigjob
#SBATCH --constraint=nvme
#SBATCH --cpus-per-task=72

if ! hash module 2> /dev/null; then
  . /etc/profile.d/modules.sh
fi

module purge
module load /nix/store/v9wb5jp9yb4al670h9sg3s9x9gbq499l-spykfunc/share/modulefiles/nix/hpc/spykfunc/0.9.0

export DATADIR=/gpfs/bbp.cscs.ch/project/proj16/spykfunc/circuits/O1.v6a
export OUTDIR=/gpfs/bbp.cscs.ch/project/proj16/matwolf/validation/O1.v6a_chc_s2f
export SM_WORKER_CORES=$((36 + 1))
export SM_EXECUTE='spykfunc --s2f --output-dir=$OUTDIR -p spark.master=spark://$(hostname):7077 $DATADIR/{builderRecipeAllPathways.xml,nodes.h5,morphologies/h5,touches/*.parquet} --checkpoint-dir=/hadoopy_me'
mkdir -p $OUTDIR
cd $OUTDIR
sm_cluster startup $OUTDIR/_cluster $SPARK_HOME/conf/spark-env.sh
exit $?
