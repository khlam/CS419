default: all
HADOOP_CLASSPATH := $(shell hadoop classpath)
SCRIPTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

all: task1

# Resets HDFS 
hdfsFiles:
	hadoop fs -mkdir /cs419/
	hadoop fs -mkdir /cs419/assignment1
	hadoop fs -mkdir /cs419/assignment1/task1/
	hadoop fs -copyFromLocal ./in.txt /cs419/assignment1/task1/

# Make task1
task1: task1.java
	javac -cp $(HADOOP_CLASSPATH) task1.java
	jar cf task1.jar task1*.class

# Make and Run task1
runtask1: task1
	make hdfsFiles
	hadoop jar task1.jar task1 /cs419/assignment1/task1/in.txt /cs419/assignment1/task1/out
	hadoop fs -cat /cs419/assignment1/task1/out/part-r-00000

# Clean
clean:
	hadoop fs -rm -f -r /cs419
	rm -f *.jar
	rm -f *.class