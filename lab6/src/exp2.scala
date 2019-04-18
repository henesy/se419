// load the ip_trace corpus
val ip_trace = sc.textFile("../../lab5/input/ip_trace");

// load the raw_block corpus
val raw_block = sc.textFile("../../lab5/input/raw_block");

// flatmap across each line, map into new RDDs based on whitespace delimeters, filter to only get thos that are blocked
val blocked = raw_block.flatMap(line => line.split("\n")).map(word => (word.split(" ")(0), word.split(" ")(1))).filter(pair => pair._2 == "Blocked");
/* Debug output
// blocked.saveAsTextFile("tmp");
*/

// original: <Time> <Connection ID> <Source IP> ">" <Destination IP> <protocol> <protocol dependent data>
//																		<Connection ID>		<Time> 		 			<Source IP> 	">" 	<Destination IP> <protocol> <protocol dependent data>
//val ip_parse = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(0), word.split(" ")(2), 		word.split(" ")(4)))

// Debug hints to myself about how to split on . since it matches all characters
// val k = l.map(x => x._1.split("\\.")(0) + ".");

// for the three below lines, flat map then map to new RDD with a subset of data, will join later
val ip_parse_time = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(0)));
val ip_parse_srcip = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(2)));
val ip_parse_dstip = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(4)));

// strip everything after and including the 4th .
// probably could reuse function variable 'o' but decided not to risk it
val ip_parse_srcip_no_port = ip_parse_srcip.map(o => (o._1, o._2.split("\\.")(0) + "." + o._2.split("\\.")(1) + "." + o._2.split("\\.")(2) + "." + o._2.split("\\.")(3)));
val ip_parse_dstip_no_port = ip_parse_dstip.map(p => (p._1, p._2.split("\\.")(0) + "." + p._2.split("\\.")(1) + "." + p._2.split("\\.")(2) + "." + p._2.split("\\.")(3)));

// join based off the connid for each of the required things
val joined_time = blocked.join(ip_parse_time);
val joined_srcip = blocked.join(ip_parse_srcip_no_port);
val joined_dstip = blocked.join(ip_parse_dstip_no_port);

/* Dugbug output
//joined_time.saveAsTextFile("join.tmp");
//joined_srcip.saveAsTextFile("joinsrc.tmp");
//joined_dstip.saveAsTextFile("joindst.tmp");
*/ 

// join all of the things
val join_all = joined_time.join(joined_srcip).join(joined_dstip);

// have 
// (557825,(((Blocked,19:34:49.249434),(Blocked,10.31.6.145.5353)),(Blocked,224.0.0.251.5353:)))

// parse the joined things to not include all the "Blocked" and only one of the connids
// ._X indicated the Xst or Xth element of the RDD, since we joined so many times, everything is nested
// the data has the format of the line after '// have'
val formatted_join = join_all.map(x => (x._2._1._1._2, x._1, x._2._1._2._2, x._2._2._2, x._2._1._1._1));
// gives
// ((Blocked,10.31.6.145.5353),557825,224.0.0.251.5353:,224.0.0.251.5353:)

// want 
// (19:34:49.249434,557825,10.31.6.145.5353,224.0.0.251.5353:)

// val formatted_join = join_all.map(x => (x._2(1)(2), x._1, x._2(2)(2), x._2(3)(2)));

// output 2a results to a textfile
formatted_join.saveAsTextFile("../output/exp2a");

// keep only the srcIP, map that with count, sum across count, sort based off of count
val counted_block_src = formatted_join.map(y => (y._3, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false, 1).map(item => item.swap);

// output final to textfile
counted_block_src.saveAsTextFile("../output/exp2b");

// <Time> <Connection ID> <Source IP> <Destination IP> "Blocked"
