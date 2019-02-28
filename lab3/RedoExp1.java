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

public class RedoExp1 {

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
		job_one.setJarByClass(RedoExp1.class);

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
		job_two.setJarByClass(RedoExp1.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(IntWritable.class);

		// Nic maybe flip
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

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();

			String[] tokens = new String[2];

			tokens = line.split("\t");

			if (!tokens[0].equals(tokens[1])){
				
				// removed out one self refernce, woo

				context.write(new Text(tokens[1]), new Text("!" + tokens[0]));
				context.write(new Text(tokens[0]), new Text(tokens[1]));

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

			// list one
			ArrayList<String> reals = new ArrayList<String>();
			// list two
			ArrayList<String> flags = new ArrayList<String>();

			int count = 0;

			for (Text val : values){

				String check = val.toString();
				if (check.charAt(0) == '!'){
					flags.add(check.substring(1, check.length()));
				}
				else {
					reals.add(check);
				}
			}

			// one hop counter
			for (String f : flags){
				for (String r : reals){
					if (r.equals(f)){
						// count++;
						context.write(new Text(key), new Text("1"));

						reals.remove(reals.indexOf(r));
					}
				}
			}

			// two hop counter
			// count += reals.size() * flags.size();
			for(String r : reals) {
				for(int i = 0; i < flags.size(); i++){
					context.write(new Text(r), new Text("1"));
				}
			}

			// count += reals.size();

			// now find biggest 10, but soonTM
			// context.write(new Text(key), new Text(Integer.toString(count)));
		} 
	}


	// The second Map Class
		public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable> {

			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String line = value.toString();
				
				String node = line.split("\t")[0];
				int one = Integer.parseInt(line.split("\t")[1]);
				
				context.write(new Text(node), new IntWritable(one));
			} 
		} 

		// The second Reduce class
		public static class Reduce_Two extends Reducer<Text, IntWritable, Text, Text> {

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int sum = 0;

				for(IntWritable v : values) {

					int val = Integer.parseInt(v.toString());
					sum += val;
				}

				context.write(key, new Text(Integer.toString(sum)));
			} 
		} 

} // End RedoExp1.class
