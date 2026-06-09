create or replace task CDC_LOAD_LOGS_ENHANCED 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '5 Minutes'
--Add A Stream Dependency to the Task Schedule
WHEN 
    system$stream_has_data('ed_cdc_stream')
    AS 
MERGE INTO ENHANCED.LOGS_ENHANCED_BACKUP e
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
from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS  logs
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
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select);

--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;



