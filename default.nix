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

  mod-spykfunc = modules.spykfunc.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev";
    moduleFilePrefix = "dev";
    packages = with python3Packages; [
      spykfunc
      parquet-converters
    ] ++ ( getPyModRec [ spykfunc ] ++ [ add-site-dir ] );
  });

  mod-spykfunc-py3 = modules.spykfunc-py3.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev-py3";
    moduleFilePrefix = "dev";
    packages = with python3Packages; [
      spykfunc-py3
      parquet-converters
    ] ++ ( getPyModRec [ spykfunc-py3 ] ++ [ add-site-dir ] );
  });
}
