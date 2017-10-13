
SPARK_JARS := /home/leite/usr/spark-2.2.0/jars


random.jar: spykfunc/udfs/*.java spykfunc/udfs/Float2Bin.class
	javac random/*.java spykfunc/udfs/*.java -cp $(SPARK_JARS)/spark-sql_2.11-2.2.0.jar:$(SPARK_JARS)/scala-library-2.11.8.jar:$(SPARK_JARS)/spark-catalyst_2.11-2.2.0.jar
	jar cf spykfunc_udfs.jar random/*.class spykfunc/udfs/*.class

spykfunc/udfs/Float2Bin.class: spykfunc/udfs/Float2Bin.scala
	#scalac spykfunc/udfs/Float2Bin.scala -cp $(SPARK_JARS)/spark-catalyst_2.11-2.2.0.jar:$(SPARK_JARS)/spark-sql_2.11-2.2.0.jar:$(SPARK_JARS)/spark-tags_2.11-2.2.0.jar:$(SPARK_JARS)/spark-sql_2.11-2.2.0.jar:$(SPARK_JARS)/spark-core_2.11-2.2.0.jar
	echo "Skipping scala Float2Bin"


clean:
	find . -name "*.class" -delete
