package exp2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class Lab7Exp2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String patentspath	= "./patents.txt";

		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
        JavaSparkContext context = new JavaSparkContext(conf);
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        
        JavaRDD<String> lines = context.textFile(patentspath);

		
		List<Edge<String>> graphEdges = new ArrayList<>();
		
		// open the file, go through each line and append an edge
		
		// == Process
		JavaRDD<String> entries = lines.flatMap(
			// Split on whitespace to get x,y style blocks
			new FlatMapFunction<String, String>() {
				@Override
				public Iterator<String> call(String s) {
					return Arrays.asList(s.split("\n")).iterator();
				}
			}
		);
		
		int counter = 0;
		
		// edge	= Split into pairs of <X, Y>
		JavaPairRDD<String, String> fullEdges = entries.mapToPair(
			new PairFunction<String, String, String>() {
				@Override
				public Tuple2<String, String> call(String s) {
					graphEdges.add(new Edge<String>(Long.parseLong(s.split("\\s+")[0]), 
							Long.parseLong(s.split("\\s+")[1]), ""));
					return new Tuple2<String, String>(s.split("\\s+")[0], s.split("\\s+")[1]);
				}
			}
		);


        JavaRDD<Edge<String>> edgeRDD = context.parallelize(graphEdges);


        Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);


        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
	}

}
