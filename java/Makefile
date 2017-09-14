
random.jar: spykfunc/udfs/GaussRand.java
	javac random/*.java spykfunc/udfs/GaussRand.java -cp /usr/local/spark-2.1.1/jars/spark-sql_2.11-2.1.1.jar
	jar cf random.jar random/*.class spykfunc/udfs/GaussRand.class

clean:
	find . -name "*.class" -delete

