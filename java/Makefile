space :=
space +=

DEPENDENCIES := spark-catalyst spark-sql scala-library commons-math3

SPARK_JARS := $(SPARK_HOME)/jars
JAR_FILES = $(foreach lib,$(DEPENDENCIES),$(wildcard $(SPARK_JARS)/$(lib)*.jar))
CLASSPATH := $(subst $(space),:,$(strip $(JAR_FILES))):.

SCALA_CLASSES := $(foreach fn,$(wildcard spykfunc/udfs/*.scala),$(subst .scala,.class,$(fn)))
JAVA_CLASSES := $(foreach fn,$(wildcard spykfunc/udfs/*.java),$(subst .java,.class,$(fn))) \
	$(foreach fn,$(wildcard spykfunc/udfs/*/*.java),$(subst .java,.class,$(fn)))

INCLUDES := -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I../deps/hadoken/include
CXXFLAGS := -std=c++11 -fPIC $(INCLUDES)


%.class: %.scala
	scalac -cp $(CLASSPATH) $<

%.class: %.java
	javac -cp $(CLASSPATH) $<


all: spykfunc_udfs.jar librandom.so


show-classpath:
	echo $(CLASSPATH)


spykfunc_udfs_hadoken_Random.h: spykfunc/udfs/hadoken/Random.java
	javah -cp $(CLASSPATH) -jni spykfunc.udfs.hadoken.Random

librandom.so: spykfunc_udfs_hadoken_Random.h Random.h Random.cpp
	$(CXX) $(CXXFLAGS) -shared -o $@ $^


spykfunc_udfs.jar: $(JAVA_CLASSES) # $(SCALA_CLASSES)
	jar cf $@ $^


clean:
	find . -name "*.class" -delete
