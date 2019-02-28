package exp2;

// http://hpc-class.its.iastate.edu:8088/cluster
// hadoop jar out.jar ExperimentOne -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts=-Xmx3686m
/**
Nic Losby
Sean Hinchee
  */



import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Lab3Exp2 {
	
	static int nCr(int n, int r) { 
		int res = (fact(r) * fact(n - r));
		if(res == 0)
			return 0;
	    return fact(n) / res; 
	} 
	  
	// Returns factorial of n 
	static int fact(int n) { 
	    int res = 1; 
	    for (int i = 2; i <= n; i++) 
	        res = res * i; 
	    return res; 
	} 

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/patents.txt"; 
		String temp = "/user/seh/lab3/exp2/temp";
		String output = "/user/seh/lab3/exp2/output"; 

		// The number of reduce tasks 
		int reduce_tasks = 10; 
		
		Configuration conf = new Configuration();

		conf.set("mapred.child.java.opts", "-Xms3686m -Xmx2g -XX:+UseSerialGC");
		conf.set("mapreduce.map.memory.mb", "4096");
		conf.set("mapreduce.reduce.memory.mb", "4096");

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Lab3 Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Lab3Exp2.class);

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

		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input));
		
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// Run the job
		job_one.waitForCompletion(true);

		/* == round 2 == */

		Job job_two = Job.getInstance(conf, "Lab3 Program Round Two");
		job_two.setJarByClass(Lab3Exp2.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(DoubleWritable.class);

		// Nic maybe flip
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

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

	/* == round 1 == */
	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] tokens = new String[2];

			tokens = line.split("\t");

			if (!tokens[0].equals(tokens[1])){
				
				// removed out one self refernce, woo

				context.write(new Text(tokens[1]), new Text("!" + tokens[0]));
				context.write(new Text(tokens[1]), new Text(tokens[0]));
				context.write(new Text(tokens[0]), new Text(tokens[1]));

			} 
		} 
	} 

	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
	    
	    // Reduce round 1
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// list not flags
			ArrayList<String> reals = new ArrayList<String>();
			
			// list flags
			ArrayList<String> flags = new ArrayList<String>();
			
			// list of triangle corners
			ArrayList<String> trinodes = new ArrayList<String>();

			// Number of triangles
			int tricount = 0;
			
			for (Text val : values){

				String check = val.toString();
				if (check.charAt(0) == '!'){
					flags.add(check.substring(1, check.length()));
				}
				else {
					reals.add(check);
				}
			}
			
			// Everything populated ^
			
			// Identify all potential triangle corners -- need 2 corners for a triangle
			if(flags.size() > 1) {
				for (String f : flags){
					for (String r : reals){
						if (r.equals(f)){
							trinodes.add(r);
						}
					}
				}
			}

			// Build triangles
			for(String t0 : trinodes) {
				for(String t1 : trinodes) {
					if(!t0.equals(t1)) {
						tricount++;
					}
				}
			}
			
						
			// Get number of triplets -- magic uwu
			int nlets = reals.stream().distinct().collect(Collectors.toList()).size();
			
			int gcc = 0;
			
			// Calculate GCC
			if(nlets >= 3) {
				int letcount = nCr(nlets, 3);
				if(letcount < 1) {
					// We have no triplets
					context.write(new Text("gcc"), new Text(Integer.toString(0)));
				} else {
					// We have >0 triplets
					gcc = (3*tricount) / letcount;
					context.write(new Text("gcc"), new Text(Integer.toString(gcc)));
				}
			} else {
				context.write(new Text(reals.toString()), new Text(Integer.toString(gcc)));
			}
		}
	}


	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.toLowerCase().split("\t");
			
			if(fields.length < 2) {
				context.write(new Text("nope"), new DoubleWritable(-1));
			} else {
				// GCC	sum
				double sum = Integer.parseInt(fields[1]);
				
				context.write(new Text("sum-gcc"), new DoubleWritable(sum));
			}
		} 
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, DoubleWritable, Text, Text> {

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;

			for(DoubleWritable v : values) {

				double val = Double.parseDouble(v.toString());
				sum += val;
			}

			context.write(key, new Text(Double.toString(sum)));
		} 
	} 

} // End RedoExp1.class
