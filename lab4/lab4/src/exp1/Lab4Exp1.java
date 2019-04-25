package exp1;

/** Lab 4 Experiment 1
 * @author Sean Hinchee
 * @author Nicholas Losby
 * 
 * Output:

Actual data set:

[seh@hadoop000 src]$ hdfs dfs -cat /cpre419/input-50m | tail
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hadoop/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/tez/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Kejc2HviJQcagVh IEdVRI1v63gjJtWDlC3RJjrSPgtzRJARNV235Uqi
Wa0gnyFIQr4gmiW NbL3o3R2m1EkuF5rryc73ijBKPqNz4R6rUUIe0oH
nSKmaPWyNTuRnkb qcu5Sg3BIlibOysLbTdGCREBiCKQtdCr6bHGKbrj
jIUq5DOASJpZndL dgc4ezXV4mU22pCaMo5ax8NBBCereYZtVtC1NDGi
zDDiqKBaKPgmz3M gEEiu0StXTOGPRTHMb6E1TJHKautN2ur43ZyHkgl
MMUjesqpEs7KoCd FeNhIgqOIkcKJ5iRCOi91ZZtkFPPEnJKLXNcoJ0s
KUgYzUPQM5a1FI5 ZlFuVY7fIXjWNlcPQlUpCB4R8tdcAvVOrnvQThun
JGeIQQ327GCZKqV EZ7TIlto3234Hfn4QmBOSk4isubPd1fgnIUA5fFE
eQTA3gWaMTKdfxL rkFLaWtqz5D7HP09N9zyi6JjiyxYTFr4hksaYHtC
tCquLsEZGeekjl7 HJ8OrSizMdVUL64XND6F827gv4Rl6D6WjoueCv7F

Our output (Ran in 2mins, 0sec):

[seh@hadoop000 src]$ hdfs dfs -cat /user/seh/lab4/exp1/output/part-r-00009 | tail
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hadoop/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/tez/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
zzzvIQoAEjnuOrk zzzvIQoAEjnuOrk Vjr3mpJIut5PtoP5Q9m5sZxDhztO5mqotdT5DnuA
zzzvYek8qI9OZs2 zzzvYek8qI9OZs2 vQevgvyHUmd0XdEQPrRP2k6Sdf0acMflxjMJuCRK
zzzxJkOpV02fpYV zzzxJkOpV02fpYV R2RvD9V8Wj59jp2pEquT7pCniVUJ2tfEprRuf6CK
zzzxtBJpvKAyeAj zzzxtBJpvKAyeAj ajFZy4Gab6uPbbNME4EOc6NfYM1DLJC4jfuqGPsR
zzzy8SA6bUgHCML zzzy8SA6bUgHCML 0DyjlV3Qr8h7vbESRn0ffNr5PIDzdXPzk4jB47f9
zzzyEq5IcqyinSK zzzyEq5IcqyinSK 1aR2VRYqS1hjYMvILLcS2pcCQRA4HW4Minhn62IV
zzzyIkOstrV5QIe zzzyIkOstrV5QIe D6YfjF9TtEa7NRz9y64nPVrcKUz1U7kCAxpHbBKL
zzzyRClBhs5chdA zzzyRClBhs5chdA vzyuXIye2ktexkgXnQXmySAXHoieXF0jPKUdWSTU
zzzyvJ5FMgenBGa zzzyvJ5FMgenBGa WVq6ZMqITXWmLthY5tJ83oq5VxoHXJjngevrZL4U
zzzzJXxBguTEdpM zzzzJXxBguTEdpM eWMIDSpNlWHUWXuttlFkbCV84pRprMucRjpmyyQ7
[seh@hadoop000 src]$


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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
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
		String parts = prefix + "/parts";

		// The number of reduce tasks 
		int reduce_tasks = 10;
		
		Configuration conf = new Configuration();

		// Slight memory expansion
		conf.set("mapred.child.java.opts", "-Xms3686m -Xmx2g -XX:+UseSerialGC");
		conf.set("mapreduce.map.memory.mb", "4096");
		conf.set("mapreduce.reduce.memory.mb", "4096");

		/* == Round 1 == */
		
		Job job_one = Job.getInstance(conf, "Lab4 Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Lab4Exp1.class);

		// Fix the number of reduce tasks to run
		job_one.setNumReduceTasks(reduce_tasks);

		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input));
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		job_one.waitForCompletion(true);
		
		/* == Round 2 == */
		
		Job job_two = Job.getInstance(conf, "Lab4 Program Round Two");
		job_two.setJarByClass(Lab4Exp1.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);

		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Configure TotalOrderPartitioner -- TODO?
		job_two.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(job_two.getConfiguration(), new Path(parts));
		
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, reduce_tasks);
		InputSampler.writePartitionFile(job_two, sampler);
		
		// Run the job
		job_two.waitForCompletion(true);

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

	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
	    
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text v : values) {
				context.write(key, v);
			}
		}
	}
	
	/* == Round 2 == */
	
	public static class Map_Two extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] fields = line.split("\\s+");
			
			Text ourKey = new Text(fields[0]);
			
			context.write(ourKey, value);
		} 
	} 

	public static class Reduce_Two extends Reducer<Text, Text, Text, Text> {
	    
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text v : values) {
				context.write(key, v);
			}
		}
	}
	
}
