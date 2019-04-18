// Load the shakespeare corpus
val inputfile = sc.textFile("../input/shakespeare");

// flat map splitting on word, this returns and iterable mappable RDD, iterate over RDD and map to new RDD adding count reference
// reduce by the key = sum across keys, swap them, sort based off the count ascending
val counts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false, 1);

// svae the output as a textfile
counts.saveAsTextFile("../output/exp1s");
