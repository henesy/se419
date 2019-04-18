import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class wordcount {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		/*
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}
		*/

		SparkConf sparkConf = new SparkConf().setAppName("WordCount in Spark").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(Your_Input_Data_Path);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split("\\s+"));
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		counts.saveAsTextFile(Your_Output_Data_Path);
		context.stop();
		context.close();
		
	}
}

