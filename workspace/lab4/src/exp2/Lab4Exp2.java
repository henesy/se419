package exp2;

/** Lab 4 Experiment 2
 * @author Sean Hinchee
 * @author Nicholas Losby
 * 
 * Output:

Actual data set:



Our output (Ran in 1m, 8s):



 */

import java.io.IOException;
import java.util.*;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Lab4Exp2 {
	
	static final String user = "seh";
	static final String lab = "lab4";
	static final String exp = "exp2";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String prefix = "/user/" + user + "/" + lab + "/" + exp;

		String input = "/cpre419/input-5k"; 
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
		// TotalOrderPartitioner.setPartitionFile(conf, new Path(output));

		/* == Round 1 == */
		
		Job job_one = Job.getInstance(conf, "☺ Lab4 Exp2 Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Lab4Exp2.class);

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
		job_one.setPartitionerClass(MyPartitioner.class);

	}

	/* == Round 1 == */
	
	/* References
	 *  http://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/mapreduce/Partitioner.html
	 *  https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/mapreduce/lib/partition/TotalOrderPartitioner.html#getPartition-K-V-int-
	 */
	public static class MyPartitioner extends Partitioner<Text, Text> {

		// TODO
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {


			return 0;
		}
		
	}
	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] fields = line.split("\\s+");
			
			Text ourKey = new Text(fields[0]);
			
			context.write(ourKey, value);
		} 
	} 

	public static class Reduce_One extends Reducer<Text, Text, Text, NullWritable> {
	    
		// Note that the values will already be sorted
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text v : values) {
				context.write(v, NullWritable.get());
			}
		}
	}
	
}
