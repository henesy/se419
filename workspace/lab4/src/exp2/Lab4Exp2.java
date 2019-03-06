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

Our output (Ran in 1m, 8s):

[seh@hadoop000 src]$ hdfs dfs -cat /user/seh/lab4/exp2/temp/part-r-00009 | tail
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hadoop/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/tez/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
zzzELTQ3vylDUmR a8fHoXzae5M3QGyaiujjdsMC45faVUrq1oA9ktOe
zzzMC6e3LaIXxOS L5MOLs3yF52Zx5P78yKJBHzBBrACx6Fkj1ek2TZj
zzzRhNL67R3TOpk e3hBNt9My9artGWV2HcXE26eoWEeayY9I1tpkC5Z
zzzT2AxjUKKo5uY rJPMt3SY0yyPLknsjt79FUFWS9fjHqafU6AsvO2r
zzziapoVW6xoE4h 6QZpMCxhezizO18JNf4A7H1aBpm3QZ8UrqBNUKC9
zzzjmbBqj0zjyTS 1vkoRF2XJAxseaWvhxTPj5hY4NcsT7sAAgE45gIC
zzzoGlnWjyVM4PK sK1nOKL1cIgeA2SPNJdfRmPSbuuBW0ToCpGJvsxA
zzzpG2TH8f4OOG8 NIYN0i0X3SLsjEeDoGN7WbXmpDk9uQcZHnK6CZIj
zzzrS7l9I3x7JTT ZNHg28osyR8UALOEk1l6EBTxdAO19CUoxfMfDQnF
zzzzJXxBguTEdpM eWMIDSpNlWHUWXuttlFkbCV84pRprMucRjpmyyQ7


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

		// TODO
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			if(numPartitions == 0)
				return 0;

			MessageDigest md;
			try {
				// Try to sha-512 and mod the integer based on the number of necessary partitions
				md = MessageDigest.getInstance("SHA-512");
				byte[] messageDigest = md.digest(key.getBytes());
				BigInteger no = new BigInteger(1, messageDigest);
				return no.intValue() % numPartitions;
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				return -1;
			}
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
