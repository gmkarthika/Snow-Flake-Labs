--Create a Table Raw JSON Data
// JSON DDL Scripts
use database SOCIAL_MEDIA_FLOODGATES;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create or replace table SOCIAL_MEDIA_FLOODGATES.public.TWEET_INGEST 
(
  RAW_STATUS variant
);

//Create File Format for JSON Data 
create or replace file format SOCIAL_MEDIA_FLOODGATES.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
strip_outer_array = TRUE
enable_octal = FALSE 
allow_duplicate = FALSE 
strip_null_values = FALSE 
ignore_utf8_errors = FALSE; 

--A Copy Into Statement to load author data 
copy into TWEET_INGEST
from @UTIL_DB.public.MY_INTERNAL_STAGE
files = ('nutrition_tweets.json')
file_format = (format_name = SOCIAL_MEDIA_FLOODGATES.public.json_file_format);

select * from TWEET_INGEST ;

//simple select statements -- are you seeing 9 rows?
select RAW_STATUS
from TWEET_INGEST;

select RAW_STATUS:entities
from TWEET_INGEST;

select RAW_STATUS:entities:hashtags
from TWEET_INGEST;

//Explore looking at specific hashtags by adding bracketed numbers
//This query returns just the first hashtag in each tweet
select raw_status:entities:hashtags[0].text
from tweet_ingest;

//This version adds a WHERE clause to get rid of any tweet that 
//doesn't include any hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

//Perform a simple CAST on the created_at key
//Add an ORDER BY clause to sort by the tweet's creation date
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;

//Flatten statements can return nested entities only (and ignore the higher level objects)
select value from 
tweet_ingest,
lateral flatten(input => raw_status:entities:urls);

select value
from tweet_ingest
,table(flatten(raw_status:entities:urls));

--Query the Nested JSON Tweet Data!

//Flatten and return just the hashtag text, CAST the text as VARCHAR
select value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

//Add the Tweet ID and User ID to the returned table so we could join the hashtag back to it's source tweet
select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

--Create a View that Makes the URL Data Appear Normalized
create or replace view social_media_floodgates.public.urls_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id::number as tweet_id
,value:display_url::text as url_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls)
);

select * from urls_normalized;

--Create a View that Makes the Hashtag Data Appear Normalized
create or replace view social_media_floodgates.public.HASHTAGS_NORMALIZED as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags)
);

select * from HASHTAGS_NORMALIZED;
