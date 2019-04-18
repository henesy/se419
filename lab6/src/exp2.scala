val ip_trace = sc.textFile("../../lab5/input/ip_trace");
val raw_block = sc.textFile("../../lab5/input/raw_block");

val blocked = raw_block.flatMap(line => line.split("\n")).map(word => (word.split(" ")(0), word.split(" ")(1))).filter(pair => pair._2 == "Blocked");
// blocked.saveAsTextFile("tmp");

// original: <Time> <Connection ID> <Source IP> ">" <Destination IP> <protocol> <protocol dependent data>
//																		<Connection ID>		<Time> 		 			<Source IP> 	">" 	<Destination IP> <protocol> <protocol dependent data>
//val ip_parse = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(0), word.split(" ")(2), 		word.split(" ")(4)))

// val k = l.map(x => x._1.split("\\.")(0) + ".");

val ip_parse_time = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(0)));
val ip_parse_srcip = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(2)));
val ip_parse_dstip = ip_trace.flatMap(line => line.split("\n")).map(word => (word.split(" ")(1), word.split(" ")(4)));

val ip_parse_srcip_no_port = ip_parse_srcip.map(o => (o._1, o._2.split("\\.")(0) + "." + o._2.split("\\.")(1) + "." + o._2.split("\\.")(2) + "." + o._2.split("\\.")(3)));
val ip_parse_dstip_no_port = ip_parse_dstip.map(p => (p._1, p._2.split("\\.")(0) + "." + p._2.split("\\.")(1) + "." + p._2.split("\\.")(2) + "." + p._2.split("\\.")(3)));

val joined_time = blocked.join(ip_parse_time);
val joined_srcip = blocked.join(ip_parse_srcip_no_port);
val joined_dstip = blocked.join(ip_parse_dstip_no_port);

//joined_time.saveAsTextFile("join.tmp");
//joined_srcip.saveAsTextFile("joinsrc.tmp");
//joined_dstip.saveAsTextFile("joindst.tmp");

val join_all = joined_time.join(joined_srcip).join(joined_dstip);

// have 
// (557825,(((Blocked,19:34:49.249434),(Blocked,10.31.6.145.5353)),(Blocked,224.0.0.251.5353:)))

val formatted_join = join_all.map(x => (x._2._1._1._2, x._1, x._2._1._2._2, x._2._2._2, x._2._1._1._1));
// gives
// ((Blocked,10.31.6.145.5353),557825,224.0.0.251.5353:,224.0.0.251.5353:)

// want 
// (19:34:49.249434,557825,10.31.6.145.5353,224.0.0.251.5353:)

// val formatted_join = join_all.map(x => (x._2(1)(2), x._1, x._2(2)(2), x._2(3)(2)));

formatted_join.saveAsTextFile("../output/exp2a");

val counted_block_src = formatted_join.map(y => (y._3, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false, 1).map(item => item.swap);

counted_block_src.saveAsTextFile("../output/exp2b");

// <Time> <Connection ID> <Source IP> <Destination IP> "Blocked"
