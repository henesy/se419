TARG=Lab7Exp2
PACKAGE=exp2
LAB=lab7
USER=nlosby

all:
	javac -source 1.8 -target 1.8 -classpath .:/home/blurbdust/school/se419/sparkjars/spark-core_2.11-2.4.1.jar:/home/blurbdust/school/se419/sparkjars/scala-library-2.11.12.jar:/home/blurbdust/school/se419/sparkjars/spark-graphx_2.12-2.4.2.jar $(PACKAGE)/*.java
	jar -cvf $(TARG).jar $(PACKAGE)/*.class

clean:
	rm $(PACKAGE)/*.class
	rm *.jar
	rm -rf output
	#hdfs dfs -rm -r /user/$USER/$(LAB)/$(PACKAGE)/output /user/$USER/$(LAB)/$(PACKAGE)/temp

run:
	spark-submit --conf spark.timeout=10000000 --conf spark.executor.memory=5g --conf spark.driver.memory=5g --class $(PACKAGE).$(TARG) $(TARG).jar

