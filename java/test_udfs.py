import numpy as np
from pyspark.sql import functions as F

sqlContext.registerJavaFunction("float2binary", "spykfunc.udfs.FloatArraySerializer")
# sqlContext.registerJavaFunction("concat_bin", "spykfunc.udfs.BinaryConcat")

_conc = sc._jvm.spykfunc.udfs.BinaryConcat().apply
def concat_bin(col):
    return F.Column(_conc(F._to_seq(sc, [col], F._to_java_column)))


df0 = sc.parallelize([(1.1,2.2,3.3),(4.4,5.5,6.6)]).toDF(["a", "b", "c"])
df1 = df0.select(df0.a.cast("float"), df0.b.cast("float"),df0.c.cast("float"))
df2 = df1.select(F.array("*").alias("myvec"))
df2.registerTempTable("df")

indiv = sqlCtx.sql("select float2binary(*) as binary from df")
indiv.show()

merged = indiv.agg(concat_bin("binary"))
merged.show()

myarr = merged.rdd.keys().collect()[0]
nparr = numpy.frombuffer(myarr, dtype=">f4").reshape((-1, 3))
print(nparr)
