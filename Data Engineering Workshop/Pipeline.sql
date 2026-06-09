--create a new stage for 'Agnie's Files Moved Into the Bucket'
--Step 1 TASK (data loadied in Amazon S3 bucket, but running every 5 minutes)

--Step 2 TASK that will load the new files into the raw table every 5 minutes (as soon as we turn it on).

--Step 3 VIEW that is kind of boring but it does some light transformation (JSON-parsing) work for us.  

--Step 4 TASK  that will load the new rows into the enhanced table every 5 minutes (as soon as we turn it on).

--Create A New Raw Table!this is the PIPELINE version of the GAME_LOGS table.
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS  (
	RAW_LOG VARIANT
);

--Create a File Format
create file format FF_JSON_LOGS
type = JSON
strip_outer_array = true;


--Step2:Load the File Into The Table
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

--create task GET_NEW_FILES Exceute it 
EXECUTE task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

select * from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step3:Create a New JSON-Parsing View
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS (
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as
select RAW_LOG:ip_address::text as ip_address
       ,RAW_LOG:user_event::text as User_event
       ,RAW_LOG:user_login::text as User_login
       ,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
       ,*
from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
where ip_address is not null;

select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step4:
EXECUTE task load_logs_enhanced;

select * from ENHANCED.LOGS_ENHANCED;

truncate ENHANCED.LOGS_ENHANCED;

--Checking Tallies Along the Way

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

SHOW TASKS LIKE 'GET_NEW_FILES' IN SCHEMA AGS_GAME_AUDIENCE.RAW;
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

--Step1: A New Select with Metadata and Pre-Load JSON Parsing
 SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

--step2: Add logs view logic to SELECT
--Step3:Create a New Target Table to Match the Select
create or replace TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS (
	LOG_FILE_NAME VARCHAR(100),
	LOG_FILE_ROW_ID NUMBER(18,0),
	LOAD_LTZ TIMESTAMP_LTZ(0),
	DATETIME_ISO8601 TIMESTAMP_NTZ(9),
	USER_EVENT VARCHAR(25),
	USER_LOGIN VARCHAR(100),
	IP_ADDRESS VARCHAR(100)
) ;
SELECT * FROM ED_PIPELINE_LOGS;

----Step4:reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs'))
;
--Step5: Create event driven pipeline