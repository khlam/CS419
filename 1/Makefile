default: run

HADOOP_CLASSPATH := $(shell hadoop classpath)

run: mapReduceTask1.java
	javac -cp $(HADOOP_CLASSPATH) mapReduceTask1.java
	java mapReduceTask1

clean:
	rm -f *.class