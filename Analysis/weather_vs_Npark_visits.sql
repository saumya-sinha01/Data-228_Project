select count(*) from weather_final;
select * from weather_final limit 1;
select * from w_comp limit 1;
select count(*) from npark_visits;

create view w_comp as select type, severity, "starttime_utc", "endtime_utc", city, locationlat, locationlng, state, duration, composite from dev_project.public.weather_final with no schema binding;



create view nv_final as select "unit name", "unit type", visitors, yearraw from dev_project.public.npark_visits with no schema binding;

create view sp_final as select name, popularity, length, elevation_gain, difficulty_rating, area_name, city_name,composite,lat,lng from dev_project.public.sparks_final with no schema binding;

select * from sp_final limit 1;
select name, popularity, length, elevation_gain, difficulty_rating, area_name, city_name,composite,lat,lng from sparks_final limit 1;


select distinct("unit name") from nv_final where yearraw = '2016';

select * from nv_final limit 1;

select * from min_comp limit 1;

select * from nparks_final limit 1;

--query to know different N Parks in this table 
select count(*) from nparks_final group by area_name;

-- Query for selecting trails with max popularity for each National Park
with nfp as (select max(popularity) as popularity, area_name from nparks_final group by area_name) 
select * from nparks_final where nparks_final.popularity in (select popularity from nfp) and nparks_final.area_name in (select area_name from nfp);

-- Query for selecting max popular trail for each national park in Nparks then picking up those Nparks visitation from
-- Nparks_visitations.
with nfp as (select max(popularity) as popularity, area_name from nparks_final group by area_name),
nfa as (select * from nparks_final where nparks_final.popularity in (select popularity from nfp) and nparks_final.area_name in (select area_name from nfp))
select * from nv_final where "unit name" in (select area_name from nfa) and yearraw = '2016';

drop table top_5;


-- Query for top 50 most popular National Park trails in US
create table dev_project.public.top_50 as (with nfp as (select max(popularity) as popularity, area_name from nparks_final group by area_name),
nfa as (select * from nparks_final where nparks_final.popularity in (select popularity from nfp) and nparks_final.area_name in (select area_name from nfp)),
nfb as (select * from nv_final where "unit name" in (select area_name from nfa) and yearraw = '2016')
select * from nparks_final where area_name in (select "unit name" from nfb) and popularity in (select popularity from nfp) order by popularity desc limit 5);

--with venuecopy as (select * from venue)
--select * from venuecopy order by 1 limit 10;
