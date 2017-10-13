import numpy as np
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark import SparkContext

conf = pyspark.SparkConf()
conf.set("spark.jars", "spykfunc_udfs.jar")
sc = SparkContext.getOrCreate(conf)
sqlContext = SQLContext.getOrCreate(sc)
sqlContext.registerJavaFunction("float2binary", "spykfunc.udfs.FloatArraySerializer")
sqlContext.registerJavaFunction("int2binary", "spykfunc.udfs.IntArraySerializer")
_conc = sc._jvm.spykfunc.udfs.BinaryConcat().apply

def concat_bin(col):
    return F.Column(_conc(F._to_seq(sc, [col], F._to_java_column)))


def test_float2bin():
    df0 = sc.parallelize([(1.1,2.2,3.3),(4.4,5.5,6.6)]).toDF(["a", "b", "c"])
    df1 = df0.select(df0.a.cast("float"), df0.b.cast("float"),df0.c.cast("float"))
    df2 = df1.select(F.array("*").alias("floatvec"))

    indiv = df2.selectExpr("float2binary(floatvec) as binary")
    indiv.show()

    merged = indiv.agg(concat_bin("binary"))
    merged.show()

    myarr = merged.rdd.keys().collect()[0]
    nparr = np.frombuffer(myarr, dtype="f4").reshape((-1, 3))
    print(nparr)


def test_int2bin():
    df0 = sc.parallelize([(1, 2, 3), (4, 5, 6)]).toDF(["a", "b", "c"])
    df1 = df0.select(df0.a.cast("int"), df0.b.cast("int"), df0.c.cast("int"))
    df2 = df1.select(F.array("*").alias("vec"))

    indiv = df2.selectExpr("int2binary(vec) as binary")
    indiv.show()

    merged = indiv.agg(concat_bin("binary"))
    merged.show()

    myarr = merged.rdd.keys().collect()[0]
    nparr = np.frombuffer(myarr, dtype="i4").reshape((-1, 3))
    print(nparr)


if __name__ == "__main__":
    test_float2bin()
    test_int2bin()
