package exp1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Lab7Exp1 {

	public static void main(String[] args) {
		String ghpath	= "./github.csv";
		String outpath	= "./output.txt";
		JavaPairRDD<String, Integer> output = null;

		// Setup
		SparkConf sparkConf = new SparkConf().setAppName("Lab7Exp1 in Spark").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(ghpath);

		// Teardown
		output.saveAsTextFile(outpath);
		context.stop();
		context.close();
		
	}

}
