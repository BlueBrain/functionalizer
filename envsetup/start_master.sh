BASEDIR=$HOME/dev/Functionalizer/pyspark
CURDIR=$BASEDIR/envsetup

export SPARK_HOME=$HOME/usr/spark-2.1.1
export SPARK_CONF_DIR=$CURDIR/spark_conf_sage

$SPARK_HOME/sbin/start-master.sh
sleep 2
$SPARK_HOME/sbin/start-slave.sh spark://`hostname`:7077

