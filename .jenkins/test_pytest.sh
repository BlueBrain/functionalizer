# Dependencies to build test environment
module load nix/boost
module load nix/hdf5
module load nix/hpc/highfive

cd $WORKSPACE
git submodule update --init --recursive
srun -Aproj16 -pinteractive -Cnvme --exclusive --mem=0 python setup.py test
