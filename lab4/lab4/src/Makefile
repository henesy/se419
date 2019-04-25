TARG=Lab4Exp2
PACKAGE=exp2
LAB=lab4

all:
	javac $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	hdfs dfs -rm -r /user/seh/$(LAB)/$(PACKAGE)/output /user/seh/$(LAB)/$(PACKAGE)/temp

run:
	hadoop jar $(TARG).jar $(PACKAGE).$(TARG)

