## Assignment 1 - Map Reduce with Hadoop
Usage


    make                Compiles Tasks 1 and 2
    make runtask1       Cleans, compiles, and runs Task 1
    make runtask2       Cleans, compiles, and runs Task 2
    make clean          Deletes all *.class, *.jar, and HDFS files

## Enviornment
Docker image: https://hub.docker.com/r/cloudera/quickstart/

Example command: Starts Docker container into terminal and maps `~/Documents/CS419` to `/mnt/CS419`:

`docker run --hostname=quickstart.cloudera --privileged=true -t -i -v ~/Documents/CS419:/mnt/CS419 --publish-all=true -p 8888 cloudera/quickstart /usr/bin/docker-quickstart`
