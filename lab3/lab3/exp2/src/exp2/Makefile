TARG=Lab3Exp2

all:
	javac *.java
	jar -cvf $(TARG).jar *.class

clean:
	rm *.class
	rm *.jar
	hdfs dfs -rm -r /user/seh/lab3/exp2/output /user/seh/lab3/exp2/temp

run:
	hadoop jar $(TARG).jar $(TARG)

