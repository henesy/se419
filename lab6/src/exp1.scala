val inputfile = sc.textFile("../input/gutenberg");
val counts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false, 1);
counts.saveAsTextFile("../output/exp1");
