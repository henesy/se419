TARG=Lab1Exp1
PACKAGE=exp1

all:
	javac $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	hdfs dfs -rm -r /user/seh/lab3/exp1/output /user/seh/lab3/exp1/temp

run:
	hadoop jar $(TARG).jar $(PACKAGE).$(TARG)

