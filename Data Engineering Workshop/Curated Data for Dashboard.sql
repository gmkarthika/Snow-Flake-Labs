--Create a CURATED Layer
create schema AGS_GAME_AUDIENCE.CURATED;

--gamer cities 
select distinct gamer_name, city
from ags_game_audience.enhanced.logs_enhanced_backup;

--Add a Time of Day Chart
select tod_name as time_of_day,
    count(*) as tally
from ags_game_audience.enhanced.logs_enhanced_backup
group by tod_name
order by tally desc;

--Aggregating Events by User
--Rolling Up Login and Logout Events with ListAgg

--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

--Windowed Data for Calculating Time in Game Per Player
select GAMER_NAME
       ,GAME_EVENT_LTZ as login 
       ,lead(GAME_EVENT_LTZ) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

select gamer_name,game_event_ltz from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where GAMER_NAME = 'adeighan2yx';

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
limit 5;

-- Put this code into a DASHBOARD TILE QUERY

--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_BACKUP)
where logout is not null;