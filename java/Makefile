
random.jar: spykfunc/udfs/GaussRand.java
	javac random/*.java spykfunc/udfs/*.java -cp /usr/local/spark-2.1.1/jars/spark-sql_2.11-2.1.1.jar:/usr/local/spark-2.1.1/jars/scala-library-2.11.8.jar
	jar cf spykfunc_udfs.jar random/*.class spykfunc/udfs/*.class

clean:
	find . -name "*.class" -delete

