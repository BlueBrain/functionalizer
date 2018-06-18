"""Test the random number functionality

Spykfunc uses Hadoken via JNI to generate random numbers. This test ensures
that the seeding yields smooth distributions.
"""
from __future__ import print_function

import matplotlib
matplotlib.use('agg')  # noqa
import matplotlib.pyplot as plt
import numpy as np
from pathlib2 import Path
import pyspark.sql.types as T
import seaborn as sns
import sparkmanager as sm

libs = str(Path(__file__).parent.resolve())
jar_filename = str((Path(__file__).parent / 'spykfunc_udfs.jar').resolve())

sm.create("test_udfs", [
    ("spark.jars", jar_filename),
    ("spark.driver.extraLibraryPath", libs),
    ("spark.executor.extraLibraryPath", libs)
])
sm.register_java_functions([
    ("gauss_rand", "spykfunc.udfs.GaussRand"),
    ("float2binary", "spykfunc.udfs.FloatArraySerializer"),
    ("int2binary", "spykfunc.udfs.IntArraySerializer"),
    ("poisson_rand", "spykfunc.udfs.PoissonRand"),
    ("gamma_rand", "spykfunc.udfs.GammaRand")
])

base = sm.createDataFrame([(i,) for i in range(1000)], T.StructType([T.StructField("nums", T.IntegerType(), False)]))
data = base.withColumnRenamed("nums", "a").crossJoin(base.withColumnRenamed("nums", "b"))
rand = data.selectExpr("*",
                       "gauss_rand(a, b, 0, cast(1.0 as float), cast(1.0 as float)) as r1",
                       "gauss_rand(a, b, 1, cast(1.0 as float), cast(1.0 as float)) as r2").toPandas()
sns.jointplot(rand.r1, rand.r2, kind="scatter", joint_kws={'s': 1})
filename = "test_udfs3.png"
plt.savefig(filename)
print("see {} for output".format(filename))
