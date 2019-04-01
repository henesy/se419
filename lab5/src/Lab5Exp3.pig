-- Lab5Exp3
-- /cpre419/ip_trace
-- /cpre419/raw_block

-- output format
-- TIME CONNID SRCIP DESTIP "Blocked"

-- input for ip_trace has format
-- TIME CONNID SRCIP ">" DESTIP PROTO SEQ

-- input for raw_block
-- CONNID ACTION
-- Blocked if ((ACTION == 1) || (ACTION == 3))

-- output /home/<NETID>/lab6/exp3/firewall

-- JOIN off CONNID

ip_trace = LOAD '../input/ip_trace' USING PigStorage(' ') AS (TIME:chararray, CONNID:long, SRCIP:chararray, DIR:chararray, DESTIP:chararray, PROTO:chararray, NUM:chararray);
-- ip_trace = LOAD ' /cpre419/ip_trace' USING PigStorage(' ') AS (TIME:chararray, CONNID:long, SRCIP:chararray, DIR:chararray, DESTIP:chararray, PROTO:chararray, NUM:chararray);

raw_block = LOAD '../input/raw_block' USING PigStorage(' ') AS (CONNID:long, ACTION:chararray);
-- raw_block = LOAD ' /cpre419/raw_block' USING PigStorage(' ') AS (CONNID:long, ACTION:chararray);

blocked = FILTER raw_block BY ACTION == 'Blocked';
-- blocked = FILTER raw_block BY (ACTION == '1') OR (ACTION == '3');

to_out = JOIN ip_trace BY CONNID, blocked BY CONNID;

out = FOREACH to_out GENERATE TIME, ip_trace::CONNID AS id, SRCIP, DESTIP, ACTION;

STORE out INTO '/home/nlosby/firewall.log';
-- STORE out INTO '/users/nlosby/lab5/exp3/firewall.log';

groups = GROUP out BY SRCIP;

total = FOREACH groups GENERATE group, COUNT(out) as count;

head = ORDER total BY count DESC;

STORE head INTO '/home/nlosby/exp3.output';
-- STORE head INTO '/users/nlosby/lab5/exp3/output';
