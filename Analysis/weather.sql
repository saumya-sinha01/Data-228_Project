--create table "public"."weather"( EventId character varying(256) encode lzo,
--Type character varying(256) encode lzo,
--Severity character varying(256) encode lzo,
--StartTime_UTC timestamp,
--EndTime_UTC timestamp,
--TimeZone character varying(256) encode lzo,
--AirportCode character varying(256) encode lzo,
--LocationLat numeric(18,0) encode az64,
--LocationLng numeric(18,0) encode az64,
--City character varying(256) encode lzo,
--County character varying(256) encode lzo,
--State character varying(256) encode lzo,
--ZipCode integer
--);
--
--drop table "public"."weather";

--create table weather1 (LIKE weather);
--insert into weather1 select * from weather where severity <> 'UNK';
--select count(*) from weather1 where severity = 'UNK' or severity = 'Other';
--delete from weather1 where severity = 'UNK';
--delete from weather1 where type = 'Rain' and severity = 'Light';
--delete from weather1 where datediff(min, starttime_utc, endtime_utc) < 120 and severity = 'Light';
--select datediff(min, starttime_utc, endtime_utc) as duration from weather where severity <> 'UNK' and duration > 120 order by duration asc;


select count(*) from weather;
select count(*) from weather1;
select distinct(type) from weather1;
select distinct(severity) from weather1;

select * from weather1 order by "starttime_utc" desc limit 2;

select count(*) from weather1 where datediff(min, starttime_utc, endtime_utc) < 120 and severity = 'Light';

select count(*) from weather1  where datediff(year, starttime_utc, '2017-01-01') <=1;

select starttime_utc, endtime_utc from weather1 limit 2 ;

--delete from weather1 where DATEPART(year,starttime_utc) = 2020;

select count(*) from weather_final;
-- 1.4479728
with mincomp as (select min(composite) from weather_final group by airportcode)
select composite; 

 with m as (select min(composite) as min from weather_final group by airportcode)
 select composite  from weather_final;

-- A view to see minimum composite for weather event on each airportcode
create view min_comp as select distinct(airportcode), min(composite) as min from dev_project.public.weather_final group by airportcode WITH NO SCHEMA BINDING;

drop view min_comp;

select * from min_comp limit 2;

-- a view to see weather_final table with updated positive composite score
create view final_min_comp as select weather_final.*, (weather_final.composite - min_comp.min) as finalcomp from dev_project.public.min_comp,dev_project.public.weather_final where weather_final.airportcode =min_comp.airportcode WITH NO SCHEMA BINDING;


-- A query for avg composite score on weather events for each airport code for all years.
select airportcode, avg(avg_comp_ac) from (select airportcode, avg(finalcomp) as avg_comp_ac, DATE_PART_YEAR(to_date(starttime_utc, 'MM-DD-YY HH24:MI:SS')) as year from dev_project.public.final_min_comp group by airportcode, year order by airportcode) group by airportcode order by airportcode limit 2;

-- A query for for avg comp score on weather events per year per airport code
select airportcode, avg(finalcomp) as avg_comp_ac, DATE_PART_YEAR(to_date(starttime_utc, 'MM-DD-YY HH24:MI:SS')) as year from dev_project.public.final_min_comp group by airportcode, year order by airportcode;

-- A query for avg comp score on weather for a particular year per airportcode
select airportcode, avg(finalcomp) from dev_project.public.final_min_comp where DATE_PART_YEAR(to_date(starttime_utc, 'MM-DD-YY HH24:MI:SS')) = 2018 group by airportcode;

--
--select airportcode, avg(finalcomp) from dev_project.public.final_min_comp where DATE_PART_YEAR(to_date(starttime_utc, 'MM-DD-YY HH24:MI:SS')) = 2019 group by airportcode;
--

--UNLOAD ('select * from weather_final') TO 's3://project-datasets/weather_final' IAM_ROLE 'arn:aws:iam::357782602849:role/AWSGlueServiceRoleMounica_A' MAXFILESIZE 1 GB CSV DELIMITER AS ',' PARALLEL OFF;

--UNLOAD ('select *, datediff(min, starttime_utc, endtime_utc) as duration from weather1') TO 's3://project-datasets/weather1' IAM_ROLE 'arn:aws:iam::357782602849:role/AWSGlueServiceRoleMounica_A' MAXFILESIZE 1 GB CSV DELIMITER AS ',' PARALLEL OFF;
