# Nix development environment
#
# build:
# export NIX_PATH=BBPpkgs=path/to/bbp-nixpkgs
#
# cd [project] && nix-build ./ -A converter
#

with import <BBPpkgs> { };

{
  spykfunc = spykfunc.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });

  spykfunc-py3 = spykfunc-py3.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev-py3";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });
}
