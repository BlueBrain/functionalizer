# Nix development environment
#
# build module:
#   official: nix-build -I BBPpkgs=https://goo.gl/wJkFfj -A mod-spykfunc-dev
#   custom:   nix-build -I "BBPpkgs=path/to/bbp-nixpkgs" -A mod-spykfunc-dev
#
# development environment:
#   official: nix-shell -I BBPpkgs=https://goo.gl/wJkFfj -A spykfunc-dev
#   custom:   nix-shell -I "BBPpkgs=path/to/bbp-nixpkgs" -A spykfunc-dev
#
with import <BBPpkgs> { };
rec {
  spykfunc-dev = spykfunc.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });

  mod-spykfunc-dev = modules.spykfunc.override {
    packages = with spykfunc-dev.pythonPackages; getPyModRec [ spykfunc-dev ] ++ [ add-site-dir parquet-converters ];
  };
}
