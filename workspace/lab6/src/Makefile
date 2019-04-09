TARG=Lab6Exp1
PACKAGE=exp1
LAB=lab6
USER=seh

all:
	javac $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	hdfs dfs -rm -r /user/$USER/$(LAB)/$(PACKAGE)/output /user/$USER/$(LAB)/$(PACKAGE)/temp

run:
	hadoop jar $(TARG).jar $(PACKAGE).$(TARG)

