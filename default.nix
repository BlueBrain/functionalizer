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
    name = "spykfunc-DEV_ENV";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });
}
