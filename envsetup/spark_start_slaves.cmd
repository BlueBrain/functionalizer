#!/bin/bash
#SBATCH -N 2
#SBATCH -t 1:00:00
#SBATCH --ntasks-per-node 2
#SBATCH --account proj16
/nfs4/bbp.epfl.ch/sw/tools/spark/2.0.1/sbin/start-slave.sh $MASTER
echo $MASTER
sleep infinity

