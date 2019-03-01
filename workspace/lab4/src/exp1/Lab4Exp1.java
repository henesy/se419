package exp1;

/** Lab 4 Experiment 1
 * @author Sean Hinchee
 * @author Nicholas Losby
 */

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Lab4Exp1 {
	
	static final String user = "seh";
	static final String lab = "lab4";
	static final String exp = "exp1";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String prefix = "/user/" + user + "/" + lab + "/" + exp;

		String input = "/cpre419/input-50m"; 
		String temp = prefix + "/temp";
		String output = prefix + "/output";
		String totalout = prefix + "/total";

		// The number of reduce tasks 
		int reduce_tasks = 10;
		
		Configuration conf = new Configuration();

		// Slight memory expansion
		conf.set("mapred.child.java.opts", "-Xms3686m -Xmx2g -XX:+UseSerialGC");
		conf.set("mapreduce.map.memory.mb", "4096");
		conf.set("mapreduce.reduce.memory.mb", "4096");
		
		// Configure TotalORderPartitioner -- TODO?
		TotalOrderPartitioner.setPartitionFile(conf, new Path(output));

		/* == Round 1 == */
		
		Job job_one = Job.getInstance(conf, "Lab4 Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Lab4Exp1.class);

		// Fix the number of reduce tasks to run
		job_one.setNumReduceTasks(reduce_tasks);

		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(NullWritable.class);

		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input));
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		job_one.waitForCompletion(true);
		
		// TOP for this job
		job_one.setPartitionerClass(TotalOrderPartitioner.class);

		/* == Round 2 == 

		Job job_two = Job.getInstance(conf, "Lab4 Program Round Two");
		job_two.setJarByClass(Lab4Exp1.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output data type of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(DoubleWritable.class);

		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		job_two.waitForCompletion(true);
		*/
	}

	/* == Round 1 == */
	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] fields = line.split("\\s+");
			
			Text ourKey = new Text(fields[0]);
			
			context.write(ourKey, value);
		} 
	} 

	public static class Reduce_One extends Reducer<Text, Text, Text, NullWritable> {
	    
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text v : values) {
				context.write(v, NullWritable.get());
			}
		}
	}
	
	/* == Round 2 == */
	
	public static class Map_Two extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
		}
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, DoubleWritable, Text, Text> {

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		}
	} 
}
