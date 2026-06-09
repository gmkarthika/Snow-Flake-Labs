--Create the Project Infrastructure
create database AGS_GAME_AUDIENCE;

create schema AGS_GAME_AUDIENCE.RAW;
drop schema AGS_GAME_AUDIENCE.public;

--create table
create table GAME_LOGS (
    RAW_LOG VARIANT
);

--create am external stage
--Test the Stage & Have a Look Around
list @UNI_KISHORE/kickoff;

--Create a File Format
create file format FF_JSON_LOGS
type = JSON
strip_outer_array = true;

-- Exploring the File Before Loading It
select $1 from 
@UNI_KISHORE/kickoff
(file_format => FF_JSON_LOGS);

--Load the File Into The Table
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/kickoff
file_format = (format_name = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

--Build a Select Statement that Separates Every Attribute into Its Own Column
select RAW_LOG:agent::text as AGENT
       ,RAW_LOG:user_event::text as User_event
       ,RAW_LOG:user_login::text as User_login
       ,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
       ,*
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

--Wrapping Selects in Views
create view LOGS  as
select RAW_LOG:agent::text as AGENT
       ,RAW_LOG:user_event::text as User_event
       ,RAW_LOG:user_login::text as User_login
       ,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
       ,*
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

select * from LOGS;

--what time zone is my account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

-- Exploring the File in updated feed
select $1 from 
@UNI_KISHORE/updated_feed
(file_format => FF_JSON_LOGS);

--Load the File Into The Table
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/updated_feed
file_format = (format_name = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

----looking for empty AGENT column
select RAW_LOG:agent::text as agent
       ,RAW_LOG:ip_address::text as ip_address
       ,RAW_LOG:user_event::text as User_event
       ,RAW_LOG:user_login::text as User_login
       ,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
       ,*
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where agent is  null ;

--create a new view with latest data load
create or replace view LOGS  as
select RAW_LOG:ip_address::text as ip_address
       ,RAW_LOG:user_event::text as User_event
       ,RAW_LOG:user_login::text as User_login
       ,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
       ,*
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null;

--Find Prajina's Log Events in log Table
select * from logs where user_login ilike '%prajina%';

--data TRANSFORMATION will be to ENHANCE the log data by adding time zone to each row.
select parse_ip(ip_address,'inet') from
AGS_GAME_AUDIENCE.RAW.logs where user_login ilike '%prajina%';

 --Enhancement Infrastructure
 --Create a new schema in the database and call it ENHANCED
 create schema AGS_GAME_AUDIENCE.ENHANCED;

 --Locate 'IPinfo: IP Geolocation Training/Education Sample' fro marketplace
 --Look Up Kishore & Prajina's Time Zone from IPinfo
select start_ip,
        end_ip,
        start_ip_int,
        end_ip_int,
        city,
        region,
        country,
        timezone    
from
IPINFO_IP_GEOLOC.DEMO.LOCATION
where parse_ip('100.41.16.160','inet'):ipv4
between start_ip_int and end_ip_int
;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
        ,loc.city
        ,loc.country
        ,loc.region
        ,loc.timezone
from AGS_GAME_AUDIENCE.RAW.logs logs
join IPINFO_IP_GEOLOC.DEMO.LOCATION loc 
where parse_ip(logs.ip_address,'inet'):ipv4
between start_ip_int and end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

--current timestamp in logs table is in UTC and we need to convert to local time zone
--Add A Column Called DOW_NAME
--Assigning a Time of Day
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, convert_timezone('UTC', timezone ,logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

--Create the Table and Fill in the Values

-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW
--a Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu
( hour number
  , tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into ags_game_audience.raw.time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from ags_game_audience.raw.time_of_day_lu
group by tod_name;

--Assigning Time of Day
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone ,logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
,TOD_NAME 
--, HOUR(GAME_EVENT_LTZ) as game_event_hr
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
ON tod.hour = HOUR(GAME_EVENT_LTZ) ;

--Wrap any Select in a CTAS statement
create table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone ,logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
,TOD_NAME 
--, HOUR(GAME_EVENT_LTZ) as game_event_hr
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
ON tod.hour = HOUR(GAME_EVENT_LTZ)
);

select * from ags_game_audience.enhanced.logs_enhanced;

--Productionized Loading
--Dump and Refresh
--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone ,logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
,TOD_NAME 
--, HOUR(GAME_EVENT_LTZ) as game_event_hr
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
ON tod.hour = HOUR(GAME_EVENT_LTZ));

--Hey! We should do this every 5 minutes from now until the next millennium - Y3K!!!
--Rebuild and Replace Using Copy/Paste Cloning

--clone the table to save this version as a backup (BU stands for Back Up)
create table ags_game_audience.enhanced.LOGS_ENHANCED_BU 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

--merge the new data based on match
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC
AND r.user_event = e.game_event_name
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

--Build Insert Merge
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone ,logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
,TOD_NAME 
--, HOUR(GAME_EVENT_LTZ) as game_event_hr
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOC.demo.location loc 
ON IPINFO_IP_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
ON tod.hour = HOUR(GAME_EVENT_LTZ)) r --we'll put our select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any! HINT: Probably NONE!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--above task keeps inserting new records irrespective of validating the match for existing records
--so we will use insert merge 

--let's truncate so we can start the load over again
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Edit  Task to Include a MERGE INSERT instead of a SIMPLE INSERT!

--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 


list '@"AGS_GAME_AUDIENCE"."RAW"."UNI_KISHORE"/';