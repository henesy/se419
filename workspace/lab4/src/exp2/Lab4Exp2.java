package exp2;

/** Lab 4 Experiment 2
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

Our output (Ran in 1mins, 32sec):

[seh@hadoop000 src]$ hdfs dfs -cat /user/seh/lab4/exp2/temp/part-r-00009 | tail
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hadoop/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/tez/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
zzzvIQoAEjnuOrk Vjr3mpJIut5PtoP5Q9m5sZxDhztO5mqotdT5DnuA
zzzvYek8qI9OZs2 vQevgvyHUmd0XdEQPrRP2k6Sdf0acMflxjMJuCRK
zzzxJkOpV02fpYV R2RvD9V8Wj59jp2pEquT7pCniVUJ2tfEprRuf6CK
zzzxtBJpvKAyeAj ajFZy4Gab6uPbbNME4EOc6NfYM1DLJC4jfuqGPsR
zzzy8SA6bUgHCML 0DyjlV3Qr8h7vbESRn0ffNr5PIDzdXPzk4jB47f9
zzzyEq5IcqyinSK 1aR2VRYqS1hjYMvILLcS2pcCQRA4HW4Minhn62IV
zzzyIkOstrV5QIe D6YfjF9TtEa7NRz9y64nPVrcKUz1U7kCAxpHbBKL
zzzyRClBhs5chdA vzyuXIye2ktexkgXnQXmySAXHoieXF0jPKUdWSTU
zzzyvJ5FMgenBGa WVq6ZMqITXWmLthY5tJ83oq5VxoHXJjngevrZL4U
zzzzJXxBguTEdpM eWMIDSpNlWHUWXuttlFkbCV84pRprMucRjpmyyQ7
[seh@hadoop000 src]$


 */

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
		
		job_one.setPartitionerClass(MyPartitioner.class);

		
		job_one.waitForCompletion(true);
		
		// TOP for this job

	}

	/* == Round 1 == */
	
	/* References
	 *  http://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/mapreduce/Partitioner.html
	 *  https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/mapreduce/lib/partition/TotalOrderPartitioner.html#getPartition-K-V-int-
	 */
	public static class MyPartitioner extends Partitioner<Text, Text> {

		// "zzzzzzzzzzzzzzz" -> 74^15
		private static BigInteger parse75(String s) {
			BigInteger res = new BigInteger("0");
			
			for(Byte b : s.getBytes()) {
				res = res.multiply(new BigInteger("75"));
				res = res.add(new BigInteger("" + (b.intValue() - '0')));
			}
			
			return res;
		}
		
		// Partition into 10 segments a given set of data ;; that is, route keys into a 10-slot set
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// "zzzzzzzzzzzzzzz"
			// 15 numbers, base 74
			// max = 74^15
			
			BigInteger max = parse75("zzzzzzzzzzzzzzz");
			BigInteger k = parse75(key.toString());
			BigInteger seg = max.divide(new BigInteger("" + numPartitions));
			
			
			for(int i = 0; i < numPartitions; i++) {
				BigInteger div = seg.multiply(new BigInteger("" + (i + 1)));
				
				if(k.compareTo(div) <= 0) {
					return i;
				}
				
				// Check if not in our bounds
				if(k.compareTo(seg.multiply(new BigInteger("" + numPartitions))) > 0) {
					return numPartitions-1;
				}
			}
			
			// Should never happen
			return -1;
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
