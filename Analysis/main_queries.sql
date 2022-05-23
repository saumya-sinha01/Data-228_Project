-- Query for top 50 most popular National Park trails in US
create table dev_project.public.top_50 as (with nfp as (select max(popularity) as popularity, area_name from nparks_final group by area_name),
nfa as (select * from nparks_final where nparks_final.popularity in (select popularity from nfp) and nparks_final.area_name in (select area_name from nfp)),
nfb as (select * from nv_final where "unit name" in (select area_name from nfa) and yearraw = '2016')
select * from nparks_final where area_name in (select "unit name" from nfb) and popularity in (select popularity from nfp) order by popularity desc limit 5);
--select * from nparks_final limit 5;
select * from top_50;
----------------------------------------------------------------------------------------
-- Query for selecting trails with max popularity for each National Park
with nfp as (select max(popularity) as popularity, area_name from nparks_final group by area_name) 
select * from nparks_final where nparks_final.popularity in (select popularity from nfp) and nparks_final.area_name in (select area_name from nfp);
----------------------------------------------------------------------------------------
--Query for getting all the parks linked along with their weather airport code, city and distance from it.
create table distance_list as (select distinct(top_50.name), top_50.lat, weather_final.locationlat, weather_final.locationlng, top_50.lon, top_50.city_name, top_50.state_name, weather_final.airportcode, weather_final.city, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(weather_final.locationlat)) * cos(radians(weather_final.locationlng) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(weather_final.locationlat )))) as distance from top_50, weather_final where top_50.lat between weather_final.locationlat-1 and weather_final.locationlat+1 and top_50.lon between weather_final.locationlng-1 and weather_final.locationlng+1 order by name);

select * from distance_list;
-------------------------------------------------------------------------------------------
-- A Query to get list of what national park is in vicinity of 50 miles to a popular city 
select distinct(top_50.name) as trail_name, top_50.city_name,  top_us_cities.city as popular_city, top_50.state_name, top_us_cities.population, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(top_us_cities.lat )))) as distance, top_50.lat as np_lat, top_50.lon as np_lon, top_us_cities.lat as city_lat, top_us_cities.lon as city_lon from top_50, top_us_cities where top_50.lat between top_us_cities.lat-1 and top_us_cities.lat+1 and top_50.lon between top_us_cities.lon-1 and top_us_cities.lon+1 order by top_50.name asc, population desc;
--------------------------------------------------------------------------------------------
-- A Query to know how many cities is a trail near to. (Considered max distance ~80 miles as limit)
create view trail_to_city_count as select count(*) as city_to_trail_count, avg(distance), trail_name from (select distinct(top_50.name) as trail_name, top_50.city_name,  top_us_cities.city as popular_city, top_50.state_name, top_us_cities.population, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(top_us_cities.lat )))) as distance, top_50.lat as np_lat, top_50.lon as np_lon, top_us_cities.lat as city_lat, top_us_cities.lon as city_lon from dev_project.public.top_50, dev_project.public.top_us_cities where top_50.lat between top_us_cities.lat-1 and top_us_cities.lat+1 and top_50.lon between top_us_cities.lon-1 and top_us_cities.lon+1 order by top_50.name asc, distance desc) group by trail_name order by trail_name asc WITH NO SCHEMA BINDING;

select * from trail_to_city_count;
----------------------------------------------------------------------------------------------
--Query to get list of cities near each trail
create view trail_to_city as select distinct(top_50.name) as trail_name, top_50.city_name,  top_us_cities.city as popular_city, top_50.state_name, top_us_cities.population, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(top_us_cities.lat )))) as distance, top_50.lat as np_lat, top_50.lon as np_lon, top_us_cities.lat as city_lat, top_us_cities.lon as city_lon from dev_project.public.top_50, dev_project.public.top_us_cities where top_50.lat between top_us_cities.lat-1 and top_us_cities.lat+1 and top_50.lon between top_us_cities.lon-1 and top_us_cities.lon+1 order by distance desc WITH NO SCHEMA BINDING;

select * from park_to_city;
select * from park_to_city;
-------------------------------------------------------------------------------------
--Query to get list of cities near each park
create view park_to_city as select distinct(top_50.area_name) as park_name, top_50.city_name,  top_us_cities.city as popular_city, top_50.state_name, top_us_cities.population, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(top_us_cities.lat )))) as distance, top_50.lat as np_lat, top_50.lon as np_lon, top_us_cities.lat as city_lat, top_us_cities.lon as city_lon from dev_project.public.top_50, dev_project.public.top_us_citites_new as top_us_cities where top_50.lat between top_us_cities.lat-1 and top_us_cities.lat+1 and top_50.lon between top_us_cities.lon-1 and top_us_cities.lon+1 order by distance desc WITH NO SCHEMA BINDING;

select * from park_to_city;
------------------------------------------------------------------------------------------
--Query to get list of cities near each park
create view spark_to_city as select distinct(sparks_final.name) as park_name, 
    sparks_final.city_name, 
    sparks_final.state_name, 
    top_us_cities.city as popular_city, 
    top_us_cities.population, 
    (3959 * acos(cos(radians(sparks_final.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(sparks_final.lng)) + sin(radians(sparks_final.lat)) * sin(radians(top_us_cities.lat )))) as distance, 
    sparks_final.lat as sp_lat, 
    sparks_final.lng as sp_lon, 
    top_us_cities.lat as city_lat, 
    top_us_cities.lon as city_lon, 
    sparks_final.composite 
    from dev_project.public.sparks_final, dev_project.public.top_us_citites_new as top_us_cities
    where sparks_final.lat between top_us_cities.lat-1 and top_us_cities.lat+1 
    and sparks_final.lng between top_us_cities.lon-1 and top_us_cities.lon+1 
--    and sparks_final.state_name = 'California'
    order by park_name desc WITH NO SCHEMA BINDING;
    
select * from spark_to_city;
------------------------------------------------------------------------------------------------
--Query that gives list of parks alongwith its nearby popular cities and national park visitations per year change 
select park_to_city.park_name, park_to_city.popular_city, park_to_city.population, park_to_city.distance, visits_final.visitor_usage_2019, visits_final.visitor_usage_2018, visits_final.visitor_usage_2017, visits_final."%increase_2018", visits_final."%increase_2019"  from park_to_city,visits_final where visits_final.name=park_to_city.park_name order by park_to_city.park_name ;
--------------------------------------------------------------------------------------------------
--Query that gives list of parks alongwith its nearby popular cities and national park visitations per year change and each city weather yearly average composite scores
create table dev_project.public.weather_parks_cities as select park_to_city.park_name, park_to_city.popular_city, park_to_city.population, park_to_city.distance, visits_final.visitor_usage_2019, visits_final.visitor_usage_2018, visits_final.visitor_usage_2017, visits_final."%increase_2018", visits_final."%increase_2019",avg_comp_compare.avg_2016,avg_comp_compare.avg_2017,avg_comp_compare.avg_2018,avg_comp_compare.avg_2019  from park_to_city,visits_final,avg_comp_compare where visits_final.name=park_to_city.park_name and park_to_city.popular_city= avg_comp_compare.city order by park_to_city.park_name ;

select * from weather_parks_cities;
select distinct(park_name),* from weather_parks_cities;
------------------------------------------------------------------------------------------------------
select w3.park_name, 
min(T2.avg_2017_visits) as avg_2017_visits, 
min(T2.avg_2018_visits) avg_2018_visits,
min(T2.avg_2019_visits) avg_2019_visits, 
avg(("avg_2017"+"avg_2018"+"avg_2019")/3) as total_avg_comp 
FROM
 ( 
  select w2.park_name, min(distance) as mindistance,
  avg(visitor_usage_2017) as avg_2017_visits, 
  avg(visitor_usage_2018) as avg_2018_visits, 
  avg(visitor_usage_2019) as avg_2019_visits 
  FROM
  (
    select park_name, min(distance) as minDist 
    from weather_parks_cities 
    group by park_name
  ) AS T 
  JOIN weather_parks_cities w2 on T.park_name = w2.park_name and T.mindist = w2.distance  
  group by w2.park_name
) AS T2 
JOIN weather_parks_cities w3 on T2.park_name = w3.park_name and T2.mindistance = w3.distance 
group by w3.park_name;

----------------------------------------------------------------------------------------------------
create view avg_comp_compare as select w_cmp_2016.city, w_cmp_2016.avg_2016, w_cmp_2017.avg_2017,w_cmp_2018.avg_2018, w_cmp_2019.avg_2019 from dev_project.public.w_cmp_2016,dev_project.public.w_cmp_2017,dev_project.public.w_cmp_2018,dev_project.public.w_cmp_2019 where w_cmp_2016.city=w_cmp_2017.city and w_cmp_2017.city=w_cmp_2018.city and w_cmp_2018.city= w_cmp_2019.city with no schema binding;

