default: run

HADOOP_CLASSPATH := $(shell hadoop classpath)

run: assignment1.java
	javac -cp $(HADOOP_CLASSPATH) assignment1.java
	java assignment1

clean:
	rm -f *.class