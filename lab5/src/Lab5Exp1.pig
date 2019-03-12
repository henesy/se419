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
data_ = load '/cpre419/gaz_tracts_national.txt' as (STATE, GEOID, POP10, HU10, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPLANT, INTPTLONG);
grouped = group data_ by STATE; 
sum = foreach grouped GENERATE GROUP, SUM(data_.ALAND_SQMI);
store sum into 'sumd_sqmi';