TARG=ExperimentOne

all:
	javac *.java
	jar -cvf $(TARG).jar *.class

clean:
	rm *.class
	rm *.jar
	hdfs dfs -rm -r /user/seh/lab3/exp1/output /user/seh/lab3/exp1/temp

run:
	hadoop jar $(TARG).jar $(TARG)

