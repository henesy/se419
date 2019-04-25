package exp1;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Lab7Exp1 {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String ghpath	= "./github.csv";
		String outpath	= "./output-lab7exp1.txt";
		JavaPairRDD<Integer, String> output = null;	// TODO -- Stub

		// == Setup
		SparkConf sparkConf = new SparkConf().setAppName("Lab7Exp1 in Spark").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(ghpath);

		// == Process
		JavaRDD<String> entries = lines.flatMap(
			// Split on whitespace to get x,y,z style blocks
			new FlatMapFunction<String, String>() {
				@Override
				public Iterator<String> call(String s) {
					return Arrays.asList(s.split("\\s+")).iterator();
				}
			}
		);
		
		// NOTE: Names are unique as they are a ACCOUNT/REPONAME union
		
		// langrep	= Split into pairs of <Language, Repo>
		JavaPairRDD<String, String> langrep = entries.mapToPair(
			new PairFunction<String, String, String>() {
				@Override
				public Tuple2<String, String> call(String s) {
					return new Tuple2<String, String>(s.split(",")[1], s.split(",")[0]);
				}
			}
		);
		
		// repstar	= Split into pairs of <Stars, Repo>
		JavaPairRDD<Integer, String> repstar = entries.mapToPair(
			new PairFunction<String, Integer, String>() {
				@Override
				public Tuple2<Integer, String> call(String s) {
					return new Tuple2<Integer, String>(Integer.parseInt(s.split(",")[12]), s.split(",")[0]);
				}
			}
		);

		// Sort repstar by Stars
		
		JavaPairRDD<Integer, String> srepstar = repstar.sortByKey(
			new Comparator<Integer>() {
			
				// Sort by largest at the beginning
				@Override
				public int compare(Integer arg0, Integer arg1) {
					if(arg0 > arg1)
						return -1;
					if(arg0 < arg1)
						return 1;
					return 0;
				}
			}
		);
		
		// temp
		output = srepstar; 
		
		// Calculate number of repos / language by counting repo's per language in langrep
		
		// TODO -- calculate star list per language (keeping repo name)
		
		// TODO -- join data into final rdd
		
		// TODO -- Sort final rdd by number of repositories
		
		// Output format: <lang> <n-repos> <repo-name> <n-stars>

		// == Emit
		output.saveAsTextFile(outpath);
		context.stop();
		context.close();
		
	}

}
