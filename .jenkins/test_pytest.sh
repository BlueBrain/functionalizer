# Dependencies to build test environment
module load nix/boost
module load nix/hdf5
module load nix/hpc/highfive

cd $WORKSPACE
git submodule update --init --recursive
python setup.py test
