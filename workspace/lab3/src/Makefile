TARG=Lab3Exp2
PACKAGE=exp2

all:
	javac $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	hdfs dfs -rm -r /user/seh/lab3/exp2/output /user/seh/lab3/exp2/temp

run:
	hadoop jar $(TARG).jar $(PACKAGE).$(TARG)

