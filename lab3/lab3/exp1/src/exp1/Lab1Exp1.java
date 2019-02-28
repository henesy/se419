package exp1;
// Sean Hinchee
// Lab1Exp1

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/* Strategy:
 * Map round 1 -> emit [src]dest
 * Reduce round 1 -> convert to [src]destA destB destC
 * Map round 2 -> Have list of toLookup = <> AND Hashmap as described of [src]<List of dests> 
 * 		- Sum one-hop for a dest by looping through a src's tuples
 * 		- For each dest, add to toLookup
 * 		- For each dest, add to src's list of dests in hashmap
 * 		(after processing line)
 * 		- For each toLookup .mid, check if in hashmap:
 * 			- if in hashmap, pop off toLookup, loop through list of dests for the pop'd val
 * 				- if in list, Sum  with weight of 2-hop
 * Reduce round 2 -> read in sorted input, emit all of it (to start)
 */

public class Lab1Exp1 {

	public static void main(String[] args) throws Exception {
		/* format:
		 * src \\s dest
		 * use:
		 * .split("\\s+);
		 */
		String input = "/cpre419/patents.txt"; 
		String temp = "/user/seh/lab3/exp1/temp";
		String output = "/user/seh/lab2/exp2/output"; 
		
		// Limit cores to 10
		int reduce_tasks = 10; 
		
		Configuration conf = new Configuration();

		/* == Round 1 == */
		
		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Lab1Exp1.class);

		// Fix the number of reduce tasks to run
		job_one.setNumReduceTasks(reduce_tasks);

		// Set map output types
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		// Set reduce output types
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		// Associate classes with map/reduce ops
		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		// Associate input/output types
		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// Associate input/output paths
		FileInputFormat.addInputPath(job_one, new Path(input));
		FileOutputFormat.setOutputPath(job_one, new Path(temp));

		// Join to job
		job_one.waitForCompletion(true);
		
		/* == Round two == */

		Job job_two = Job.getInstance(conf, "Driver Program Round Two");
		job_two.setJarByClass(Lab1Exp1.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		
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
	
	// First map round
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {
		/* Strategy:
		 * 1. Create hashmap refs [values]list
		 * 2. Add all [src]ref tuples to the hashmap as they come in
		 * -- end values iteration
		 * 
		 * 3. Create hashmap counts [values]sum
		 * 4. For all values in refs, [ref]sum++ (since it's referenced)
		 * -- end one-hop count
		 * 
		 * 5. For all values in refs, [r]sum++ if,
		 * 		for each ref in [src] which is not r and [!r].contains(r)
		 * -- end two-hop count
		 * 
		 * 6. Emit all [values]sum entries as value,sum pairs
		 * 
		 * Note: (5) is very expensive, definitely very exponential, O(n^3)?
		 */
		
		
		// Hashmap of values and what they reference
		private Map<String, List<String>> refs = new HashMap<>();
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] halves = line.toLowerCase().split("\\s+");
			
			if(halves.length < 2)
				return;
			
			context.write(new Text(halves[0]), new Text(halves[1]));
		}
	}
	
	// First reduce round
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
		/* Strategy:
		 * 1. Read in values (sorted
		 * 2. Emit values
		 * 
		 * Note: might have to take the 10 at the beginning/end depending on order, or just tail -10/head -10 the file outputted
		 */
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	/* == Round 2 == */
	
	// Second map round
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	// Second reduce round
	public static class Reduce_Two extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Emit values in key,val order (sorted)
			for(Text v : values)
				context.write(key, v);
		}
	}

}
