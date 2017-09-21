module load spark
PYTHONHOME=$HOME/dev/Functionalizer/pyspark/envsetup/sparkenv
export PATH=$PYTHONHOME/bin:$PATH
#PATH=$HOME/usr/spark-2.1.1/bin:$HOME/usr/jdk1.8.0_144/jre/bin:$PATH
#JAVA_HOME=$HOME/usr/jdk1.8.0_144/jre

BASE_DIR=$HOME/dev/Functionalizer/pyspark/var

SPARK_LOG_DIR=$BASE_DIR/logs
SPARK_WORKER_DIR=$BASE_DIR/workers

