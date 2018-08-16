# Nix development environment
#
# build:
# export NIX_PATH=BBPpkgs=path/to/bbp-nixpkgs
#
# cd [project] && nix-build ./ -A converter
#

with import <BBPpkgs> { };

rec {
  spykfunc-dev = spykfunc.overrideDerivation (oldAttr: rec {
    name = "spykfunc-dev";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });

  mod-spykfunc-dev = envModuleGen rec {
    name = "mod-spykfunc-dev";
    moduleFilePrefix = "dev";
    packages = with python3Packages; ([
      spykfunc-dev
      parquet-converters
    ] ++  getPyModRec [ spykfunc-dev ] ++ [ add-site-dir ] );
    extraContent = ''
      setenv HADOOP_HOME "${hadoop-bbp}"
      setenv JAVA_HOME "${jre}"
      setenv SPARK_HOME "${spark-bbp}"

      setenv PYSPARK_DRIVER_PYTHON " "
      setenv PYSPARK_PYTHON "${python3Packages.python}/bin/${python3Packages.python.executable}"
    '';
    dependencies = [ modules.python36-full ];
  };
}
