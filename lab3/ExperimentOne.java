// http://hpc-class.its.iastate.edu:8088/cluster
// hadoop jar out.jar ExperimentOne -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts=-Xmx3686m
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExperimentOne {

	private static Map<String, List<String>> graph = new HashMap<String, List<String>>();

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/patents.txt"; 
		String temp = "/user/nlosby/lab3/exp1/temp";
		String output = "/user/nlosby/lab3/exp1/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 10; 
		
		Configuration conf = new Configuration();

		conf.set("mapred.child.java.opts", "-Xms3686m -Xmx2g -XX:+UseSerialGC");
		conf.set("mapreduce.map.memory.mb", "4096");
		conf.set("mapreduce.reduce.memory.mb", "4096");

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Lab3 Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(ExperimentOne.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);

		
		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// This is legal
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// This is not allowed
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); 

		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1

		Job job_two = Job.getInstance(conf, "Lab3 Program Round Two");
		job_two.setJarByClass(ExperimentOne.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(IntWritable.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(IntWritable.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);

	}

	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();

			String[] tokens = new String[2];

			tokens = line.split("\t");

			// for value, add to hashmap
			List<String> tmp = new ArrayList<String>();
			tmp = graph.get(tokens[0]);

			if (tokens[0] != tokens[1]){
				if (tmp != null){
					tmp.add(tokens[1]);
					graph.put(tokens[0], tmp);
				}
				else {
					tmp = new ArrayList<String>(Arrays.asList(tokens[1]));
					System.out.println(tmp.toString());
					graph.put(tokens[0], tmp);
				}

				StringBuilder sb = new StringBuilder();
				for (String s : tmp){
					sb.append(s);
					sb.append("\t");
				}

				context.write(new Text(tokens[0]), new Text(sb.toString()));
			}
		} 
	} 

	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {

		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				List<String> tmp = new ArrayList<String>();

				tmp = graph.get(val.toString());

				// remove duplicates
				if (tmp != null){
					tmp = (ArrayList) tmp.stream().distinct().collect(Collectors.toList());
						
	
					graph.put(val.toString(), tmp);
	
					StringBuilder sb = new StringBuilder();
					for (String s : tmp){
						sb.append(s);
						sb.append("\t");
					}
	
					context.write(val, new Text(sb.toString()));
				}
			}

		} 
	}

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {

		private Text endKey = new Text("end");

		public void map(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			List<String> temp = new ArrayList<String>();
			List<String> tmp = new ArrayList<String>();

			// prune two depth loops

			for (Text v : values){

				temp = graph.get(v.toString());

				if (temp != null){

					for (String s : temp){

						tmp = graph.get(s);

						for (String t : tmp){

							// if key == key.ref.ref
							if (v.toString() == t){

								tmp.remove(tmp.indexOf(t));
								graph.put(v.toString(), tmp);

							}

						}

					}

					StringBuilder sb = new StringBuilder();
					for (String s : temp){
						sb.append(s);
						sb.append("\t");
					}

					context.write(v, new Text(sb.toString()));
				}
			}
		} 
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Map<String, Integer> toOutput = new HashMap<String, Integer>();

			for (Text text : values) {

				List<String> check = new ArrayList<String>();
				check = graph.get(text.toString());

				if (check != null){

					int s = check.size();

					// make sure we actually fill the hashmap first
					if (toOutput.size() < 10){
						toOutput.put(text.toString(), s);
					}
					else {
						// apparently sorting a hashmap is not a thing in Java?
						// algo of find min and pop min was stolen from online
						Map.Entry<String, Integer> min = null;

						for (Map.Entry<String, Integer> potential : toOutput.entrySet()) {
							if (min == null){
								min = potential;
							}
							else {
								if (potential.getValue() < min.getValue()) {
									min = potential;
								}
							}
						}
						// because we serach for the min everytime,
						// I don't have to keep track of nextMin or max
						if (s > min.getValue()) {
							toOutput.remove(min.getKey());
							toOutput.put(text.toString(), s);
						}
					}
				}
			}

			for (Map.Entry<String, Integer> out : toOutput.entrySet()) {
				context.write(new Text(out.getKey()), new IntWritable(out.getValue()));
			}
			
		} 
	} 

} // End ExperimentOne.class
