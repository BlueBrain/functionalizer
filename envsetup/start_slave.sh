BASEDIR=$HOME/dev/Functionalizer/pyspark
CURDIR=$BASEDIR/envsetup

export SPARK_HOME=/nfs4/bbp.epfl.ch/sw/tools/spark/2.2.0
export SPARK_CONF_DIR=$CURDIR/spark_conf_viz

$SPARK_HOME/sbin/start-slave.sh spark://$1:7077

