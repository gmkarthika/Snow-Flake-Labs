'''
Create a database called MELS_SMOOTHIE_CHALLENGE_DB.

Drop the PUBLIC schema

Add a schema named TRAILS

Add an internal named stage called TRAILS_GEOJSON

Add an internal named stage called TRAILS_PARQUET'''

create database MELS_SMOOTHIE_CHALLENGE_DB;
create schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS ;
drop schema MELS_SMOOTHIE_CHALLENGE_DB.PUBLIC ;

--Create a JSON file format 
create file format FF_JSON
type = JSON ;

--Create a Parquet  file format 
create file format FF_Parquet
type = PARQUET ;

select *
from '@"MELS_SMOOTHIE_CHALLENGE_DB"."TRAILS"."TRAILS_GEOJSON"/Bear_Creek_Trail.geojson'
(file_format =>FF_JSON );

--Query TRAILS_PARQUET Stage
create view CHERRY_CREEK_TRAIL as
(select 
    $1:sequence_1::number as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat  
from '@"MELS_SMOOTHIE_CHALLENGE_DB"."TRAILS"."TRAILS_PARQUET"/cherry_creek_trail.parquet'
(file_format =>FF_Parquet )
order by point_id);

--format to be used in WKT Playground
select top 100
lng || ' '||lat as coord_pair,
'POINT('||coord_pair ||')' as trail_point
from CHERRY_CREEK_TRAIL;

--replace the view by adding a new column
create or replace view CHERRY_CREEK_TRAIL as
(select 
    $1:sequence_1::number as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat,
    lng || ' '||lat as coord_pair,
from '@"MELS_SMOOTHIE_CHALLENGE_DB"."TRAILS"."TRAILS_PARQUET"/cherry_creek_trail.parquet'
(file_format =>FF_Parquet )
order by point_id);

--execute the query and paste the result in https://clydedacruz.github.io/openstreetmap-wkt-playground/ to get the trail plot
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 250
group by trail_name;

--add length of the trail
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring,
st_length(TO_GEOGRAPHY(my_linestring))
from cherry_creek_trail
where point_id <= 250
group by trail_name;

--Explore the geoJSON Files
select $1 
from '@"MELS_SMOOTHIE_CHALLENGE_DB"."TRAILS"."TRAILS_GEOJSON"/Bear_Creek_Trail.geojson'
(file_format =>FF_JSON);

--Normalize the Data Without Loading It!
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

--go to https://geojson.io/ and paste the whole_object column data from the above query

--create view for the normalized data
create view  DENVER_AREA_TRAILS as 
(select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json)
);

--replace view DENVER_AREA_TRAILS to add new column
create or replace view  DENVER_AREA_TRAILS as 
(select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(TO_GEOGRAPHY(geometry)) as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json)
);

select * from DENVER_AREA_TRAILS;

select * from cherry_creek_trail;

-- try to get the data from CHERRY_CREEK_TRAIL and DENVER_AREA_TRAILS to look enough alike that we can run some GeoSpatial functions on all 5 trails at one time

--Create a View on Cherry Creek Data to Mimic the Other Trail Data 'denver area trails'

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--apply to_geography function to both select
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS_2;

--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
create view trails_and_boundaries as 
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

--A Polygon Can be Used to Create a Bounding Box
select
min(min_eastwest) as western_edge
,max(max_eastwest) as eastern_edge
,min(min_northsouth) as southern_edge
,max(max_northsouth) as northern_edge
from trails_and_boundaries;

select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;

--Create a second Schema in Mel's Database and call it LOCATIONS.
create schema MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS;

--create udf DISTANCE_TO_MC (distance to meanies cafe)
create or replace function DISTANCE_TO_MC(LOC_LNG number(38,32), LOC_LAT number(38,32))
    returns float
    as 
    $$
     st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint(LOC_LNG,LOC_LAT)
     )
     $$ 
     ;

--Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng,$tc_lat);

--Create a List of Competing Juice Bars in the Area
create view COMPETITION as 
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

--Which Competitor is Closest to Melanie's?
select
    name
    ,cuisine
    ,st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,coordinates
     ) as distance_to_melanies
    ,*
FROM COMPETITION
order by distance_to_melanies;

--Changing the Function to Accept a GEOGRAPHY Argument
create or replace function DISTANCE_TO_MC(lng_and_lat GEOGRAPHY)
    returns float
    as 
    $$
     st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lng_and_lat
     )
     $$ 
     ;

--Use UDF to select Which Competitor is Closest to Melanie's?
select
    name
    ,cuisine
    ,DISTANCE_TO_MC(coordinates) as distance_to_melanies
    ,*
FROM COMPETITION
order by distance_to_melanies;

-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select DISTANCE_TO_MC($tcb_lng, $tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select DISTANCE_TO_MC(st_makepoint($tcb_lng, $tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select
    name
    ,DISTANCE_TO_MC(coordinates) as distance_to_melanies
    ,ST_ASWKT(coordinates)
FROM OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop = 'books'
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

--Create a View of Bike Shops in the Denver Data for promotion
create view DENVER_BIKE_SHOPS as 
select  
    name
    ,DISTANCE_TO_MC(coordinates) as distance_to_melanies
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop = 'bicycle'
order by distance_to_melanies;

-- Create an External Table
create external table T_CHERRY_CREEK_TRAIL(
my_filename varchar(100) as (metadata$filename::varchar(100))
)
location = @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
auto_refresh = true
file_format = (type = parquet);

-- we get the error 'Cannot use internal stage TRAILS_PARQUET as the location for an external table.'
-- create external stage for an external table

--Let's TRY AGAIN to Create a Super-Simple, Stripped Down External Table
create external table T_CHERRY_CREEK_TRAIL(
my_filename varchar(100) as (metadata$filename::varchar(100))
)
location = @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.EXTERNAL_AWS_DLKW
auto_refresh = true
file_format = (type = parquet);

select * from T_CHERRY_CREEK_TRAIL;

--If we used a regular view, that view would be recalculating that distance over and over each time it was run. With a Materialized view, it ----will only change if the Cherry Creek Trail changes or Melanie's Cafe moves to a different building.
create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
(select 
    value:sequence_1::number as point_id,
    value:trail_name::varchar as trail_name,
    value:latitude::number(11,8) as lng,
    value:longitude::number(11,8) as lat,
    lng || ' '||lat as coord_pair,
    locations.DISTANCE_TO_MC(
        st_makepoint(lng,lat)) as distance_to_melanies
from MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.T_CHERRY_CREEK_TRAIL);

