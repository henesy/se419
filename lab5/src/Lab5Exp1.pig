-- input /cpre419/gaz_tracts_national.txt
-- Interesetd in ALAND
-- format is 
-- STATE	GEOID	POP10	HU10	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPLANT	INTPTLONG

-- Want top 10 states by land area
-- Group by STATE
-- SUM across ALAND_SQMI
-- SORT
-- basically
-- https://github.com/alanfgates/programmingpig/blob/master/examples/ch2/average_dividend.pig
-- 2019-04-01 16:53:25,245 [main] ERROR org.apache.pig.tools.grunt.Grunt 
-- ERROR 1025: <file Lab5Exp1.pig, line 16, column 31> Invalid field projection. 
-- Projected field [GROUP] does not exist in schema: group:bytearray,data_:bag{:tuple(
-- STATE:bytearray,GEOID:bytearray,POP10:bytearray,HU10:bytearray,ALAND:bytearray,
-- AWATER:bytearray,ALAND_SQMI:bytearray,AWATER_SQMI:bytearray,INTPLANT:bytearray,INTPTLONG:bytearray)}.

-- I guess it did not like me either excluding the typing or the fact they
-- were uppercase? Really not sure. Nop[e, just typing.

-- data_ = load '/home/nlosby/CPRE.419/labs/lab5/input/gaz.txt' USING PigStorage('\t') AS (STATE, GEOID, POP10, HU10, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPLANT, INTPTLONG);
data_ = load '../input/gaz.txt' AS (STATE:chararray, GEOID:chararray, POP10:int, HU10:int, ALAND:long, AWATER:long, ALAND_SQMI:long, AWATER_SQMI:long, INTPLANT:long, INTPTLONG:long);

grouped = GROUP data_ BY STATE; 

tot = FOREACH grouped GENERATE group, SUM(data_.ALAND) AS sum;

ordered = ORDER tot BY sum DESC;

head = LIMIT ordered 10;

STORE head INTO '/home/nlosby/sumd_sqmi';
