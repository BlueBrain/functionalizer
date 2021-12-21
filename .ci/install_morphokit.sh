set -x
set -e
set -o pipefail

if [[ -n "$CI_JOB_TOKEN" ]]; then
    mkdir -p "xdg_config/git"
    cat > "xdg_config/git/config" <<EOF
[url "https://gitlab-ci-token:${CI_JOB_TOKEN}@bbpgitlab.epfl.ch/"]
    insteadOf = git@bbpgitlab.epfl.ch:
EOF
    export XDG_CONFIG_HOME="$PWD/xdg_config"
fi

INSTALL_DIR=$(mktemp -d)
echo "${INSTALL_DIR}/morphokit" > morphokit_location.txt
mkdir -p "${INSTALL_DIR}/src"
cd "${INSTALL_DIR}/src"
(
    git clone --depth=1 --recursive --shallow-submodules https://github.com/BlueBrain/MorphIO.git
    cd MorphIO
    cmake -B cmake_build -S . -GNinja -DCMAKE_INSTALL_PREFIX="${1:-${INSTALL_DIR}/morphokit}" -DBUILD_BINDINGS=OFF -DMORPHIO_TESTS:BOOL=OFF
    cmake --build cmake_build
    cmake --install cmake_build
)
(
    git clone --depth=1 --recursive --shallow-submodules git@bbpgitlab.epfl.ch:hpc/morpho-kit.git
    cd morpho-kit
    cmake -B cmake_build -S . -GNinja -DCMAKE_INSTALL_PREFIX="${1:-${INSTALL_DIR}/morphokit}" -DBUILD_BINDINGS=OFF
    cmake --build cmake_build
    cmake --install cmake_build
)
(
    git clone --depth=1 --recursive --shallow-submodules https://github.com/BlueBrain/Random123.git
    cd Random123
    cp -R include/Random123 "${INSTALL_DIR}/morphokit/include"
)
