TARG=Lab7Exp1
PACKAGE=exp1
LAB=lab7
USER=seh

all:
	javac -source 1.8 -target 1.8 -classpath .:/home/seh/se419/sparkjars/spark-core_2.11-2.4.1.jar:/home/seh/se419/sparkjars/scala-library-2.11.12.jar $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	#hdfs dfs -rm -r /user/$USER/$(LAB)/$(PACKAGE)/output /user/$USER/$(LAB)/$(PACKAGE)/temp

run:
	spark-submit --class $(PACKAGE).$(TARG) $(TARG).jar

