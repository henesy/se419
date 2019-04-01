-- /cpre419/network_trace.txt
-- 10:20:00.000020 IP 244.131.189.196.22379 > 245.184.172.199.80: tcp 0
-- TIME "IP" SRCIP.PORT ">" DESTIP.PORT PROTO SEQ
-- Need to trim .PORT off
-- Interested in SRCIP
-- if (PROTO != "tcp")
-- 	   prune
-- SUM SRCIP occurances
-- SORT

data_ = load '../input/network_trace' USING PigStorage(' ') AS (TIME:chararray, IP:chararray, SRCIP:chararray, DIR:chararray, DESTIP:chararray, PROTO:chararray, NUM:chararray);

grouped = FILTER data_ BY PROTO == 'tcp';

conn_pair = FOREACH grouped GENERATE SUBSTRING(SRCIP, 0, LAST_INDEX_OF(SRCIP, '.')) AS srcip, SUBSTRING(DESTIP, 0, LAST_INDEX_OF(DESTIP, '.')) AS destip;

uniq = DISTINCT conn_pair;

uniq_group = GROUP uniq BY srcip;

total = FOREACH uniq_group GENERATE group, COUNT(uniq) AS count;

ordered = ORDER total BY count DESC;

head = LIMIT ordered 10;

STORE head INTO '/home/nlosby/exp2.output';
