import pyspark
from pyspark.sql import functions as F

spark = pyspark.sql.SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df0 = sc.parallelize([(1.1, 2.2, 3.3), (4.4, 5.5, 6.6)]).toDF(["a", "b", "c"])
df1 = df0.select(df0.a.cast("float"), df0.b.cast("float"), df0.c.cast("float"))
df2 = df1.select(F.array("*").alias("floatvec"))


j_f2b = sc._jvm.spykfunc.udfs.Float2Bin().getUDF().apply


def wrap_java_udf(sc, java_f):
    return lambda col: F.Column(j_f2b(F._to_seq(sc, [col], F._to_java_column)))


f2b = wrap_java_udf(sc, j_f2b)

df2.select(f2b("floatvec")).show()

# import numpy as np
# indiv.show()
#
# merged = indiv.agg(concat_bin("binary"))
# merged.show()
#
# myarr = merged.rdd.keys().collect()[0]
# nparr = np.frombuffer(myarr, dtype=">f4").reshape((-1, 3))
# print(nparr)
