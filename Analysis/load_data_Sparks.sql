COPY dev_project.public.sparks_final (objectid, id, slug, name, popularity, type, length, elevation_gain, difficulty_rating, route_type, visitor_usage, avg_rating, area_id, area_name, country_id, country_name, city_id, city_name, state_id, state_name, verified_map_id, features, activities, feature_names, activity_names, overview, num_reviews, units, area_slug, trail_id, city_url, park_slug, profile_photo_url, composite, lat, lng) FROM 's3://project-datasets/Sparks_final.csv' IAM_ROLE 'arn:aws:iam::357782602849:role/AWSGlueServiceRoleMounica_A' FORMAT AS CSV DELIMITER ',' QUOTE '"' REGION AS 'us-east-1' IGNOREHEADER 1;

select * from sparks_final;
select * from visits_final;
select distinct(top_50.area_name) as park_name, top_50.city_name,  top_us_cities.city as popular_city, top_50.state_name, top_us_cities.population, (3959 * acos(cos(radians(top_50.lat)) * cos(radians(top_us_cities.lat)) * cos(radians(top_us_cities.lon) - radians(top_50.lon)) + sin(radians(top_50.lat)) * sin(radians(top_us_cities.lat )))) as distance, top_50.lat as np_lat, top_50.lon as np_lon, top_us_cities.lat as city_lat, top_us_cities.lon as city_lon from dev_project.public.top_50, dev_project.public.top_us_citites_new as top_us_cities where top_50.lat between top_us_cities.lat-1 and top_us_cities.lat+1 and top_50.lon between top_us_cities.lon-1 and top_us_cities.lon+1 order by distance desc
select * from park_to_city;



--Query that gives list of parks alongwith its nearby popular cities and national park visitations per year change and each city weather yearly average composite scores
create table dev_project.public.weather_parks_cities as select park_to_city.park_name, park_to_city.popular_city, park_to_city.population, park_to_city.distance, visits_final.visitor_usage_2019, visits_final.visitor_usage_2018, visits_final.visitor_usage_2017, visits_final."%increase_2018", visits_final."%increase_2019",avg_comp_compare.avg_2016,avg_comp_compare.avg_2017,avg_comp_compare.avg_2018,avg_comp_compare.avg_2019  from park_to_city,visits_final,avg_comp_compare where visits_final.name=park_to_city.park_name and park_to_city.popular_city= avg_comp_compare.city order by park_to_city.park_name ;
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
