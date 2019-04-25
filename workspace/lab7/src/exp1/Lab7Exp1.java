package exp1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class Lab7Exp1 {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String ghpath	= "./github.csv";
		String outpath	= "./output-lab7exp1";
		//RDD<Tuple2<Integer, String>> output = null;	// TODO -- Stub

		// == Setup
		SparkConf sparkConf = new SparkConf().setAppName("Lab7Exp1 in Spark").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(ghpath);

		// == Process
		JavaRDD<String> entries = lines.flatMap(
			// Split on whitespace to get x,y,z style blocks
			s ->
				Arrays.asList(s.split("\\s+")).iterator()
		);
		
		// NOTE: Names are unique as they are a ACCOUNT/REPONAME union
		
		// langrep	= Split into pairs of <Language, Repo>
		JavaPairRDD<String, String> langrep = entries.mapToPair(
			s ->
				new Tuple2<String, String>(s.split(",")[1], s.split(",")[0])
		);
		
		// langrep	= Split into pairs of <Language, Repo>
		JavaPairRDD<String, String> replang = entries.mapToPair(
			s ->
				new Tuple2<String, String>(s.split(",")[0], s.split(",")[1])
		);
		
		// repstar	= Split into pairs of <Stars, Repo>
		JavaPairRDD<Integer, String> repstar = entries.mapToPair(
			s ->
				new Tuple2<Integer, String>(Integer.parseInt(s.split(",")[12]), s.split(",")[0])
		);

		// Sort repstar by Stars
		
		JavaPairRDD<Integer, String> srepstar = repstar.sortByKey(
			(arg0, arg1) -> {
				if(arg0 > arg1)
					return -1;
				if(arg0 < arg1)
					return 1;
				return 0;
			}
		);
				
		// -- Calculate number of repos / language by counting repo's per language in langrep
		
		// Initialize a table of languages and counts starting at 0
		JavaRDD<String> langs = langrep.keys();
		JavaPairRDD<String, Integer> counts0 = langs.mapToPair(
			s -> 
				new Tuple2<String, Integer>(s, 1)
		);
		
		// Group values into iterable set of values, countng the number of values
		JavaPairRDD<String, Integer> counts = counts0.groupByKey().mapValues(
			f -> {
				int count = 0;
				Iterator<Integer> iter = f.iterator();
				while(iter.hasNext()) {
					count++;
					iter.next();
				}
				return count;
			}
		);
		
		// counts.saveAsTextFile(outpath);		
		
		// -- Calculate top repo per language by star count
		
		// Make <repo, star> into tuple2
		JavaRDD<Tuple2<String, Integer>> stardd = srepstar.map(
				f -> 
					new Tuple2<String, Integer>(f._2(), f._1())
		);
		
		// Make <lang, <repo, star>> of repositories
		JavaPairRDD<String, Tuple2<String, Integer>> langrepstar = stardd.mapToPair(
			f -> {
				// TODO -- find language of repo name
				String lang = replang.lookup(f._1()).get(0);
				return new Tuple2<String, Tuple2<String, Integer>>(lang, f);
			}
		);
		
		// Get max star for a lang
		JavaPairRDD<String, Tuple2<String, Integer>> maxlangrepstar = langrepstar.reduceByKey(
				(v1, v2) -> {
					if(v1._2() > v2._2())
						return v1;
					return v2;
				}
		);
		
		// TODO -- Sort final rdd by number of repositories
		
		// Make serializable
		JavaRDD<String> output = maxlangrepstar.map(
				f -> {
					String s = "";
					
					// Lang
					s += f._1 + " ";
					
					// Repo name
					s += f._2()._1;
					
					// Nstars
					s += f._2()._2;
					
					return s;
				}
		);
				
		// Output format: <lang> <n-repos> <repo-name> <n-stars>

		// == Emit
		
		try {
			RandomAccessFile out = new RandomAccessFile("./fuckers.txt", "rw");
			output.foreach(s -> out.writeUTF(s + "\n"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//output.saveAsTextFile(outpath);
		context.stop();
		context.close();
		
	}

}
