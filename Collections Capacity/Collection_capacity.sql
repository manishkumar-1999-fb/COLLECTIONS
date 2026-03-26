
---Dialer data
with 
call_data_five_nine as
(
Select 
FBBID,
Date_time_call,
DISPOSITION,
AGENT_NAME,
AGENT_GROUP,
DPD_BUCKET
from analytics.credit.v_five9_call_log
where 
date(date_time_call) >= '2025-01-01'
and (agent_group = 'Collections Group' 
or (campaign in ('Collections - 1-2 OB',
'Collections - 3-12 OB',
'Collections - 14 plus OB',
'Collections - Broken PTP OB',
'Collections - Missed Payment Priority OB')))
and fbbid is not null
qualify row_number() over(partition by fbbid, date_time_call,agent_name,disposition,DPD_bucket order by date_time_call) = 1
)
-- Select * from call_data_five_nine where fbbid = 2713809.00000
-- order by Date_time_call desc;


-- Select top 10* from call_data_five_nine;
,
call_data_salesforce
as 
(
select
case when fundbox_id__c = 'Not Linked' then 0 else TRY_TO_NUMBER(fundbox_id__c) end as Fbbid_2,
lastmodifieddate,
calltype,
calldisposition,
null as DPD_bucket2,
ASSIGNEE_NAME__C as agent_name2,
role_id_name__c as agent_group2
from external_data_sources.salesforce_nova.task
where date(lastmodifieddate) >= '2025-01-01' and TRY_TO_NUMBER(fundbox_id__c) is not null
and ROLE_ID_NAME__C in ('ER agent','Collections Manager')
)
--Select * from call_data_salesforce where fbbid_2 = '2713809' order by lastmodifieddate desc;
--Select * from call_data_salesforce where date(lastmodifieddate) = '2026-02-04';
-- select top 10* from call_data_salesforce;
--select * from external_data_sources.salesforce_nova.task where fundbox_id__c = '2713809'order by lastmodifieddate desc;
,all_calls as
(
select a.*,
b.* 
from call_data_five_nine a
full outer join call_data_salesforce b
on a.fbbid = b.fbbid_2 
and date(a.date_time_call) = date(b.lastmodifieddate)
)
,
dialing_data as
(
select coalesce(fbbid,fbbid_2) as fbbid,
coalesce(date(date_time_call),date(lastmodifieddate)) as call_date,
coalesce(dpd_bucket,dpd_bucket2) as dpd_bucket,
case when (disposition ilike '%Payment%' or calldisposition ilike '%payment%') then 1 else 0 end as payment_flag,
case when (disposition ilike '%Promise to Pay%' or calldisposition ilike '%Promise to Pay%') then 1 else 0 end as PTP_flag,
case when (disposition ilike '%RPC%' or calldisposition ilike '%RPC%') then 1 else 0 end as RPC_flag,
case when (disposition ilike '%Settlement Accepted%' or calldisposition ilike '%Settlement Accepted%') then 1 else 0 end as Settlement_flag,
case when (disposition ilike '%Third Party%' or calldisposition ilike '%Third Party%') then 1 else 0 end as third_party_flag,
case when (agent_name is not null and disposition ilike '%not answer%' and calldisposition not in ('Payment','Promise to Pay','RPC','Settlement Accepted','Third Party')) then 1 else 0 end as not_answer_contacted,
disposition,
calldisposition,
coalesce(agent_name,agent_name2) as agent_name
from all_calls
)
-- select * from dialing_data where call_date ='2026-02-04' 
-- and agent_name is null and rpc_flag = 1;

,agent_level as
(
select 
agent_name,
call_date,
-- DPD_bucket,
count(distinct FBBID) as unique_accnts,
sum(case when agent_name is not null then 1 else 0 end) as total_dials,
sum(payment_flag) as Payment_dispo,
sum(PTP_flag) as PTP_dispo,
sum(RPC_flag) as RPC_dispo,
sum(Settlement_flag) as settlement_dispo,
sum(Third_party_flag) as third_Party_dispo,
sum(not_answer_contacted) as not_answered_dispo
from dialing_data
group by all
)
-- select min(call_date) from agent_level;
-- order by call_date desc,agent_name;

---Agent_time_data
,RawData AS (
    SELECT 
        REPLACE(A._DATA:"AGENT", '"', '') AS AGENT_EMAIL,
        REPLACE(A._DATA:"AGENT GROUP", '"', '') AS AGENT_GROUP,
        REPLACE(A._DATA:"AGENT ID", '"', '') AS AGENT_ID,
        REPLACE(A._DATA:"DATE", '"', '') AS CALL_DATE,
        REPLACE(A._DATA:"TIME", '"', '') AS EVENT_TIME, 
        REPLACE(A._DATA:"STATE", '"', '') AS STATE,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS REASON_CODE,
        REPLACE(A._DATA:"AGENT STATE TIME", '"', '') AS STATE_DURATION_RAW,
        
        -- Use SPLIT_PART to safely grab numbers between colons
        -- Example: '01:30:45' -> Part 1 is '01', Part 2 is '30', Part 3 is '45'
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) AS HOURS,
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) AS MINUTES,
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) AS SECONDS,
        
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin 
            ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS CLEAN_AGENT_NAME,
        -- Safe Time Calculation to decimal minutes
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60) AS DURATION_MINS
        
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
)
Select * from rawdata where call_date = '2026/02/04' and agent_group = 'Collections Group' order by call_date,agent_ID,event_time;
,
agent_data_time as
(
SELECT 
    CLEAN_AGENT_NAME,
    AGENT_EMAIL,
    AGENT_GROUP,
    to_date(CALL_DATE,'YYYY/MM/DD') as call_date,
    EVENT_TIME,
    STATE,
    REASON_CODE,
    STATE_DURATION_RAW,
    DURATION_MINS,
    -- IFNULL handles cases where a part might be missing (like '00:00')
    (IFNULL(HOURS, 0) * 60) + IFNULL(MINUTES, 0) + (IFNULL(SECONDS, 0) / 60) AS TOTAL_MINUTES
FROM RawData
where agent_group = 'Collections Group'
-- ORDER BY CALL_DATE DESC, EVENT_TIME DESC
)
-- Select * from agent_data 
-- where clean_agent_name in ('Anais Orozco') and call_date = '2026/02/04'
-- order by call_date desc,event_time,CLEAN_AGENT_NAME desc, state, reason_code;

,
-- CTE 3: Calculate login duration (first login to last logout)
login_hours AS (
    SELECT 
        CLEAN_AGENT_NAME,
        AGENT_GROUP,
        CALL_DATE,
        MIN(EVENT_TIME) AS FIRST_LOGIN_TIME,
        MAX(CASE WHEN STATE = 'Logout' THEN EVENT_TIME END) AS LAST_LOGOUT_TIME,
        -- Calculate logged in hours (difference between first login and last logout)
        TIMEDIFF(
            'minute',
            TRY_TO_TIME(MIN(EVENT_TIME)),
            TRY_TO_TIME(MAX(CASE WHEN STATE = 'Logout' THEN EVENT_TIME END))
        ) AS LOGGED_IN_MINS
    FROM agent_data_time
    GROUP BY CLEAN_AGENT_NAME, AGENT_GROUP, CALL_DATE
),

-- CTE 4: Calculate activity durations (all categories to sum to total time)
activity_metrics AS (
    SELECT 
        CLEAN_AGENT_NAME,
        AGENT_GROUP,
        CALL_DATE,
        
        -- 1. Call Time: On Call, On Hold, On Park, On Voicemail, DID Ringing, Ringing, OR (Not Ready + Outbound Calls - Manual)
        SUM(CASE 
            WHEN STATE IN ('On Call', /*'On Hold',*/ 'On Park', 'On Voicemail', 'DID Ringing', 'Ringing') 
                 OR (STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual')
            THEN DURATION_MINS ELSE 0 
        END) AS CALL_TIME_MINS,
        
        -- 2. Email Time: Not Ready + Email
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE = 'Email'
            THEN DURATION_MINS ELSE 0 
        END) AS EMAIL_TIME_MINS,
        
        -- 3. SMS Time: Not Ready + SMS
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE = 'SMS'
            THEN DURATION_MINS ELSE 0 
        END) AS SMS_TIME_MINS,
        
        -- 4. Wrap Time: After Call Work + Extended Research + Tech Issue + Wrap-Up + Task Completion + System (aligned with Excel)
        SUM(CASE 
            WHEN STATE = 'After Call Work' 
                 OR (STATE = 'Not Ready' AND REASON_CODE IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
            THEN DURATION_MINS ELSE 0 
        END) AS WRAP_TIME_MINS,
        
        -- 5. Idle Time: Ready state (available but not on call)
        SUM(CASE 
            WHEN STATE = 'Ready'
            THEN DURATION_MINS ELSE 0 
        END) AS IDLE_TIME_MINS,
        
        -- 6. Break/Lunch Time: Not Ready + (Break, Lunch)
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Break', 'Lunch')
            THEN DURATION_MINS ELSE 0 
        END) AS BREAK_LUNCH_MINS,
        
        -- 7. Meeting/Training Time: Not Ready + (Meeting, Team Meeting, Training, Appointment)
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Meeting', 'Team Meeting', 'Training', 'Appointment')
            THEN DURATION_MINS ELSE 0 
        END) AS MEETING_TRAINING_MINS,
        
        -- 10. Other Time: Login, Not Ready with (No Reason, Not Ready, blank)
        SUM(CASE when
                 (STATE = 'Not Ready' AND (REASON_CODE IN ('No Reason', 'Not Ready') OR REASON_CODE IS NULL OR REASON_CODE = ''))
            THEN DURATION_MINS ELSE 0 
        END) AS OTHER_TIME_MINS,
        
        -- Total tracked time (excluding Logout)
        SUM(CASE 
            WHEN STATE not in ('Logout','Login','On Hold')
            THEN DURATION_MINS ELSE 0 
        END) AS TOTAL_TRACKED_MINS
        
    FROM agent_data_time
    GROUP BY CLEAN_AGENT_NAME, AGENT_GROUP, CALL_DATE
)
,agent_data_time_2 as
(SELECT 
    l.CLEAN_AGENT_NAME,
    l.AGENT_GROUP,
    l.CALL_DATE,
    
    -- Login Information
    l.FIRST_LOGIN_TIME,
    l.LAST_LOGOUT_TIME,
    ROUND(l.LOGGED_IN_MINS / 60, 2) AS LOGGED_IN_HOURS,
    ROUND(a.TOTAL_TRACKED_MINS / 60, 2) AS TOTAL_TRACKED_HOURS,
    
    -- Activity Breakdown (in hours) - Wrap now includes Extended Research, Tech Issues, Task Completion, System
    ROUND(a.CALL_TIME_MINS / 60, 2) AS CALL_TIME_HOURS,
    ROUND(a.EMAIL_TIME_MINS / 60, 2) AS EMAIL_TIME_HOURS,
    ROUND(a.SMS_TIME_MINS / 60, 2) AS SMS_TIME_HOURS,
    ROUND(a.WRAP_TIME_MINS / 60, 2) AS WRAP_TIME_HOURS,
    ROUND(a.IDLE_TIME_MINS / 60, 2) AS IDLE_TIME_HOURS,
    ROUND(a.BREAK_LUNCH_MINS / 60, 2) AS BREAK_LUNCH_HOURS,
    ROUND(a.MEETING_TRAINING_MINS / 60, 2) AS MEETING_TRAINING_HOURS,
    ROUND(a.OTHER_TIME_MINS / 60, 2) AS OTHER_TIME_HOURS,
    
    -- Verification: Sum of all activities should equal Total Tracked Time
    ROUND((a.CALL_TIME_MINS + a.EMAIL_TIME_MINS + a.SMS_TIME_MINS + a.WRAP_TIME_MINS + 
           a.IDLE_TIME_MINS + a.BREAK_LUNCH_MINS + a.MEETING_TRAINING_MINS + a.OTHER_TIME_MINS) / 60, 2) AS SUM_ALL_ACTIVITIES_HOURS,
    
    -- Efficiency Percentages (based on total tracked time)
    ROUND((a.CALL_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS CALL_TIME_PCT,
    ROUND((a.EMAIL_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS EMAIL_TIME_PCT,
    ROUND((a.SMS_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS SMS_TIME_PCT,
    ROUND((a.WRAP_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS WRAP_TIME_PCT,
    ROUND((a.IDLE_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS IDLE_TIME_PCT,
    ROUND((a.BREAK_LUNCH_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS BREAK_LUNCH_PCT,
    ROUND((a.MEETING_TRAINING_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS MEETING_TRAINING_PCT,
    ROUND((a.OTHER_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS OTHER_TIME_PCT

FROM login_hours l
JOIN activity_metrics a
    ON l.CLEAN_AGENT_NAME = a.CLEAN_AGENT_NAME
    AND l.CALL_DATE = a.CALL_DATE
ORDER BY l.CALL_DATE DESC, l.CLEAN_AGENT_NAME
)
select a.*,
b.*
from agent_level a
left join agent_data_time_2 b
on a.agent_name = b.CLEAN_AGENT_NAME 
and a.call_date = b.call_date
order by a.call_date desc, agent_name

;

---HOURLY ANALYSIS: Calls and Call Minutes per Hour
---This section breaks down agent activity by hour
with RawData AS (
    SELECT 
        REPLACE(A._DATA:"AGENT", '"', '') AS AGENT_EMAIL,
        REPLACE(A._DATA:"AGENT GROUP", '"', '') AS AGENT_GROUP,
        REPLACE(A._DATA:"AGENT ID", '"', '') AS AGENT_ID,
        REPLACE(A._DATA:"DATE", '"', '') AS CALL_DATE,
        REPLACE(A._DATA:"TIME", '"', '') AS EVENT_TIME, 
        REPLACE(A._DATA:"STATE", '"', '') AS STATE,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS REASON_CODE,
        REPLACE(A._DATA:"AGENT STATE TIME", '"', '') AS STATE_DURATION_RAW,
        
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) AS HOURS,
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) AS MINUTES,
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) AS SECONDS,
        
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin 
            ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS CLEAN_AGENT_NAME,
        
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60) AS DURATION_MINS
        
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
),
hourly_agent_data AS (
    SELECT 
        CLEAN_AGENT_NAME,
        AGENT_EMAIL,
        AGENT_GROUP,
        TO_DATE(CALL_DATE, 'YYYY/MM/DD') AS CALL_DATE_UTC,
        -- Extract hour from EVENT_TIME (format: HH:MM:SS) and convert UTC to CST (UTC-6)
        TRY_CAST(SPLIT_PART(EVENT_TIME, ':', 1) AS INT) AS HOUR_UTC,
        -- Convert to CST: subtract 6 hours, handle negative values (wrap to previous day)
        CASE 
            WHEN TRY_CAST(SPLIT_PART(EVENT_TIME, ':', 1) AS INT) - 6 < 0 
            THEN TRY_CAST(SPLIT_PART(EVENT_TIME, ':', 1) AS INT) - 6 + 24
            ELSE TRY_CAST(SPLIT_PART(EVENT_TIME, ':', 1) AS INT) - 6
        END AS HOUR_OF_DAY_CST,
        -- Adjust date if hour conversion crosses midnight (UTC hour 0-5 becomes previous day in CST)
        CASE 
            WHEN TRY_CAST(SPLIT_PART(EVENT_TIME, ':', 1) AS INT) < 6 
            THEN DATEADD('day', -1, TO_DATE(CALL_DATE, 'YYYY/MM/DD'))
            ELSE TO_DATE(CALL_DATE, 'YYYY/MM/DD')
        END AS CALL_DATE,
        STATE,
        REASON_CODE,
        DURATION_MINS
    FROM RawData
    WHERE AGENT_GROUP = 'Collections Group'
),
hourly_call_metrics AS (
    SELECT 
        CLEAN_AGENT_NAME,
        AGENT_GROUP,
        CALL_DATE,
        HOUR_OF_DAY_CST,
        
        -- Count of calls per hour (On Call events)
        COUNT(CASE WHEN STATE = 'On Call' THEN 1 END) AS CALLS_IN_HOUR,
        
        -- Total minutes on call per hour
        ROUND(SUM(CASE WHEN STATE = 'On Call' THEN DURATION_MINS ELSE 0 END), 2) AS CALL_MINUTES_IN_HOUR,
        
        -- Count of outbound manual calls per hour
        COUNT(CASE WHEN STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual' THEN 1 END) AS OUTBOUND_CALLS_IN_HOUR,
        
        -- Minutes on outbound manual calls per hour
        ROUND(SUM(CASE WHEN STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual' THEN DURATION_MINS ELSE 0 END), 2) AS OUTBOUND_CALL_MINUTES_IN_HOUR,
        
        -- Total talk time (On Call + Outbound Manual)
        ROUND(SUM(CASE WHEN STATE IN ('On Call') OR (STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual') THEN DURATION_MINS ELSE 0 END), 2) AS TOTAL_TALK_TIME_MINS,
        
        -- Wrap time per hour (aligned with Excel: After Call Work + Extended Research + Tech Issues + Wrap-Up + Task Completion + System)
        ROUND(SUM(CASE WHEN STATE = 'After Call Work' OR (STATE = 'Not Ready' AND REASON_CODE IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System')) THEN DURATION_MINS ELSE 0 END), 2) AS WRAP_TIME_IN_HOUR,
        
        -- Idle/Ready time per hour
        ROUND(SUM(CASE WHEN STATE = 'Ready' THEN DURATION_MINS ELSE 0 END), 2) AS IDLE_TIME_IN_HOUR,
        
        -- Break/Lunch time per hour
        ROUND(SUM(CASE WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Break', 'Lunch') THEN DURATION_MINS ELSE 0 END), 2) AS BREAK_LUNCH_IN_HOUR
        
    FROM hourly_agent_data
    GROUP BY CLEAN_AGENT_NAME, AGENT_GROUP, CALL_DATE, HOUR_OF_DAY_CST
)
SELECT 
    CLEAN_AGENT_NAME,
    AGENT_GROUP,
    CALL_DATE,
    HOUR_OF_DAY_CST,
    LPAD(HOUR_OF_DAY_CST, 2, '0') || ':00 - ' || LPAD(HOUR_OF_DAY_CST + 1, 2, '0') || ':00 CST' AS HOUR_RANGE_CST,
    CALLS_IN_HOUR,
    CALL_MINUTES_IN_HOUR,
    OUTBOUND_CALLS_IN_HOUR,
    OUTBOUND_CALL_MINUTES_IN_HOUR,
    TOTAL_TALK_TIME_MINS,
    WRAP_TIME_IN_HOUR,
    IDLE_TIME_IN_HOUR,
    BREAK_LUNCH_IN_HOUR,
    -- Utilization percentage for the hour (talk time / 60 mins)
    ROUND((TOTAL_TALK_TIME_MINS / 60) * 100, 2) AS HOUR_UTILIZATION_PCT
FROM hourly_call_metrics
WHERE CALL_DATE >= '2025-01-01'
ORDER BY CALL_DATE DESC, CLEAN_AGENT_NAME, HOUR_OF_DAY_CST

;

---ENHANCED METRICS: Connect Rate, Calls per Productive Hour, AHT, Agent Ranking
---This query combines dialing data with time data for comprehensive agent performance metrics
with 
call_data_five_nine as
(
Select 
FBBID,
Date_time_call,
DISPOSITION,
AGENT_NAME,
AGENT_GROUP,
DPD_BUCKET
from analytics.credit.v_five9_call_log
where 
date(date_time_call) >= '2025-01-01'
and (agent_group = 'Collections Group' 
or (campaign in ('Collections - 1-2 OB',
'Collections - 3-12 OB',
'Collections - 14 plus OB',
'Collections - Broken PTP OB',
'Collections - Missed Payment Priority OB')))
and fbbid is not null
qualify row_number() over(partition by fbbid, date_time_call,agent_name,disposition,DPD_bucket order by date_time_call) = 1
),
call_data_salesforce as 
(
select
case when fundbox_id__c = 'Not Linked' then 0 else TRY_TO_NUMBER(fundbox_id__c) end as Fbbid_2,
lastmodifieddate,
calltype,
calldisposition,
null as DPD_bucket2,
ASSIGNEE_NAME__C as agent_name2,
role_id_name__c as agent_group2
from external_data_sources.salesforce_nova.task
where date(lastmodifieddate) >= '2025-01-01' and TRY_TO_NUMBER(fundbox_id__c) is not null
and ROLE_ID_NAME__C in ('ER agent','Collections Manager')
),
all_calls as
(
select a.*,
b.* 
from call_data_five_nine a
full outer join call_data_salesforce b
on a.fbbid = b.fbbid_2 
and date(a.date_time_call) = date(b.lastmodifieddate)
),
dialing_data as
(
select coalesce(fbbid,fbbid_2) as fbbid,
coalesce(date(date_time_call),date(lastmodifieddate)) as call_date,
coalesce(dpd_bucket,dpd_bucket2) as dpd_bucket,
case when (disposition ilike '%Payment%' or calldisposition ilike '%payment%') then 1 else 0 end as payment_flag,
case when (disposition ilike '%Promise to Pay%' or calldisposition ilike '%Promise to Pay%') then 1 else 0 end as PTP_flag,
case when (disposition ilike '%RPC%' or calldisposition ilike '%RPC%') then 1 else 0 end as RPC_flag,
case when (disposition ilike '%Settlement Accepted%' or calldisposition ilike '%Settlement Accepted%') then 1 else 0 end as Settlement_flag,
case when (disposition ilike '%Third Party%' or calldisposition ilike '%Third Party%') then 1 else 0 end as third_party_flag,
case when disposition ilike '%Connected%' or calldisposition ilike '%Connected%' 
     or disposition ilike '%RPC%' or calldisposition ilike '%RPC%'
     or disposition ilike '%Payment%' or calldisposition ilike '%Payment%'
     or disposition ilike '%Promise to Pay%' or calldisposition ilike '%Promise to Pay%'
     then 1 else 0 end as connected_flag,
coalesce(agent_name,agent_name2) as agent_name
from all_calls
),
agent_dial_metrics as
(
select 
agent_name,
call_date,
DATE_TRUNC('month', call_date) as call_month,
count(distinct FBBID) as unique_accounts,
count(*) as total_dials,
sum(connected_flag) as total_connected,
sum(payment_flag) as payment_count,
sum(PTP_flag) as ptp_count,
sum(RPC_flag) as rpc_count,
sum(Settlement_flag) as settlement_count
from dialing_data
where agent_name is not null
group by agent_name, call_date, DATE_TRUNC('month', call_date)
),
RawData_metrics AS (
    SELECT 
        REPLACE(A._DATA:"AGENT", '"', '') AS AGENT_EMAIL,
        REPLACE(A._DATA:"AGENT GROUP", '"', '') AS AGENT_GROUP,
        REPLACE(A._DATA:"DATE", '"', '') AS CALL_DATE,
        REPLACE(A._DATA:"STATE", '"', '') AS STATE,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS REASON_CODE,
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin 
            ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS CLEAN_AGENT_NAME,
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60) AS DURATION_MINS
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
),
agent_time_metrics as
(
SELECT 
    CLEAN_AGENT_NAME,
    TO_DATE(CALL_DATE, 'YYYY/MM/DD') AS CALL_DATE,
    DATE_TRUNC('month', TO_DATE(CALL_DATE, 'YYYY/MM/DD')) as call_month,
    
    -- Call Time (On Call + Outbound Manual)
    SUM(CASE WHEN STATE IN ('On Call', 'On Voicemail') OR (STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual') THEN DURATION_MINS ELSE 0 END) AS CALL_TIME_MINS,
    
    -- Wrap Time (aligned with Excel definition)
    SUM(CASE WHEN STATE = 'After Call Work' OR (STATE = 'Not Ready' AND REASON_CODE IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System')) THEN DURATION_MINS ELSE 0 END) AS WRAP_TIME_MINS,
    
    -- Productive Time (Call + Email + SMS + Wrap)
    SUM(CASE 
        WHEN STATE IN ('On Call', 'On Voicemail') 
             OR (STATE = 'Not Ready' AND REASON_CODE IN ('Outbound Calls - Manual', 'Email', 'SMS', 'Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
             OR STATE = 'After Call Work'
        THEN DURATION_MINS ELSE 0 
    END) AS PRODUCTIVE_TIME_MINS,
    
    -- Total Tracked Time
    SUM(CASE WHEN STATE NOT IN ('Logout', 'Login', 'On Hold') THEN DURATION_MINS ELSE 0 END) AS TOTAL_TRACKED_MINS
    
FROM RawData_metrics
GROUP BY CLEAN_AGENT_NAME, TO_DATE(CALL_DATE, 'YYYY/MM/DD'), DATE_TRUNC('month', TO_DATE(CALL_DATE, 'YYYY/MM/DD'))
),
combined_metrics as
(
SELECT 
    COALESCE(d.agent_name, t.CLEAN_AGENT_NAME) as agent_name,
    COALESCE(d.call_date, t.CALL_DATE) as call_date,
    COALESCE(d.call_month, t.call_month) as call_month,
    
    -- Dial Metrics
    COALESCE(d.total_dials, 0) as total_dials,
    COALESCE(d.total_connected, 0) as total_connected,
    COALESCE(d.unique_accounts, 0) as unique_accounts,
    COALESCE(d.payment_count, 0) as payment_count,
    COALESCE(d.ptp_count, 0) as ptp_count,
    COALESCE(d.rpc_count, 0) as rpc_count,
    
    -- Time Metrics (in hours)
    ROUND(COALESCE(t.CALL_TIME_MINS, 0) / 60, 2) as call_hours,
    ROUND(COALESCE(t.WRAP_TIME_MINS, 0) / 60, 2) as wrap_hours,
    ROUND(COALESCE(t.PRODUCTIVE_TIME_MINS, 0) / 60, 2) as productive_hours,
    ROUND(COALESCE(t.TOTAL_TRACKED_MINS, 0) / 60, 2) as total_tracked_hours,
    
    -- Keep raw minutes for AHT calculation
    COALESCE(t.CALL_TIME_MINS, 0) as call_time_mins,
    COALESCE(t.WRAP_TIME_MINS, 0) as wrap_time_mins
    
FROM agent_dial_metrics d
FULL OUTER JOIN agent_time_metrics t
    ON d.agent_name = t.CLEAN_AGENT_NAME
    AND d.call_date = t.CALL_DATE
)
SELECT 
    agent_name,
    call_date,
    call_month,
    total_dials,
    total_connected,
    unique_accounts,
    
    -- Connect Rate %
    ROUND((total_connected / NULLIF(total_dials, 0)) * 100, 2) AS CONNECT_RATE_PCT,
    
    -- Calls per Productive Hour
    ROUND(total_dials / NULLIF(productive_hours, 0), 2) AS DIALS_PER_PRODUCTIVE_HOUR,
    
    -- Average Handle Time (AHT) in minutes = (Call Time + Wrap Time) / Connected Calls
    ROUND((call_time_mins + wrap_time_mins) / NULLIF(total_connected, 0), 2) AS AHT_MINS,
    
    -- Time breakdown
    call_hours,
    wrap_hours,
    productive_hours,
    total_tracked_hours,
    
    -- Outcome metrics
    payment_count,
    ptp_count,
    rpc_count,
    
    -- Payment/PTP/RPC rates per connected call
    ROUND((payment_count / NULLIF(total_connected, 0)) * 100, 2) AS PAYMENT_RATE_PCT,
    ROUND((ptp_count / NULLIF(total_connected, 0)) * 100, 2) AS PTP_RATE_PCT,
    ROUND((rpc_count / NULLIF(total_connected, 0)) * 100, 2) AS RPC_RATE_PCT,
    
    -- Occupancy % (Productive Time / Total Tracked Time)
    ROUND((productive_hours / NULLIF(total_tracked_hours, 0)) * 100, 2) AS OCCUPANCY_PCT,
    
    -- Agent Ranking by Connect Rate (daily)
    RANK() OVER (PARTITION BY call_date ORDER BY (total_connected / NULLIF(total_dials, 0)) DESC) AS DAILY_CONNECT_RANK,
    
    -- Agent Ranking by Dials per Hour (daily)
    RANK() OVER (PARTITION BY call_date ORDER BY (total_dials / NULLIF(productive_hours, 0)) DESC) AS DAILY_PRODUCTIVITY_RANK

FROM combined_metrics
WHERE call_date >= '2025-01-01'
  AND agent_name IS NOT NULL
ORDER BY call_date DESC, agent_name

;

---MONTHLY AGENT SUMMARY WITH RANKINGS
---Aggregated monthly view matching your Excel format with additional metrics
with 
call_data_five_nine as
(
Select 
FBBID,
Date_time_call,
DISPOSITION,
AGENT_NAME,
AGENT_GROUP,
DPD_BUCKET
from analytics.credit.v_five9_call_log
where 
date(date_time_call) >= '2025-01-01'
and (agent_group = 'Collections Group' 
or (campaign in ('Collections - 1-2 OB',
'Collections - 3-12 OB',
'Collections - 14 plus OB',
'Collections - Broken PTP OB',
'Collections - Missed Payment Priority OB')))
and fbbid is not null
qualify row_number() over(partition by fbbid, date_time_call,agent_name,disposition,DPD_bucket order by date_time_call) = 1
),
call_data_salesforce as 
(
select
case when fundbox_id__c = 'Not Linked' then 0 else TRY_TO_NUMBER(fundbox_id__c) end as Fbbid_2,
lastmodifieddate,
calltype,
calldisposition,
null as DPD_bucket2,
ASSIGNEE_NAME__C as agent_name2,
role_id_name__c as agent_group2
from external_data_sources.salesforce_nova.task
where date(lastmodifieddate) >= '2025-01-01' and TRY_TO_NUMBER(fundbox_id__c) is not null
and ROLE_ID_NAME__C in ('ER agent','Collections Manager')
),
all_calls as
(
select a.*,
b.* 
from call_data_five_nine a
full outer join call_data_salesforce b
on a.fbbid = b.fbbid_2 
and date(a.date_time_call) = date(b.lastmodifieddate)
),
dialing_data as
(
select coalesce(fbbid,fbbid_2) as fbbid,
coalesce(date(date_time_call),date(lastmodifieddate)) as call_date,
case when disposition ilike '%Connected%' or calldisposition ilike '%Connected%' 
     or disposition ilike '%RPC%' or calldisposition ilike '%RPC%'
     or disposition ilike '%Payment%' or calldisposition ilike '%Payment%'
     or disposition ilike '%Promise to Pay%' or calldisposition ilike '%Promise to Pay%'
     then 1 else 0 end as connected_flag,
case when (disposition ilike '%Payment%' or calldisposition ilike '%payment%') then 1 else 0 end as payment_flag,
case when (disposition ilike '%Promise to Pay%' or calldisposition ilike '%Promise to Pay%') then 1 else 0 end as PTP_flag,
case when (disposition ilike '%RPC%' or calldisposition ilike '%RPC%') then 1 else 0 end as RPC_flag,
coalesce(agent_name,agent_name2) as agent_name
from all_calls
),
monthly_dial_metrics as
(
select 
agent_name,
DATE_TRUNC('month', call_date) as call_month,
count(*) as total_dials,
sum(connected_flag) as total_connected,
sum(payment_flag) as payment_count,
sum(PTP_flag) as ptp_count,
sum(RPC_flag) as rpc_count
from dialing_data
where agent_name is not null
group by agent_name, DATE_TRUNC('month', call_date)
),
RawData_monthly AS (
    SELECT 
        REPLACE(A._DATA:"DATE", '"', '') AS CALL_DATE,
        REPLACE(A._DATA:"STATE", '"', '') AS STATE,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS REASON_CODE,
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin 
            ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS CLEAN_AGENT_NAME,
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60) AS DURATION_MINS
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
),
monthly_time_metrics as
(
SELECT 
    CLEAN_AGENT_NAME,
    DATE_TRUNC('month', TO_DATE(CALL_DATE, 'YYYY/MM/DD')) as call_month,
    
    -- Call Hrs
    ROUND(SUM(CASE WHEN STATE IN ('On Call', 'On Voicemail') OR (STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual') THEN DURATION_MINS ELSE 0 END) / 60, 2) AS CALL_HRS,
    
    -- Email Hrs
    ROUND(SUM(CASE WHEN STATE = 'Not Ready' AND REASON_CODE = 'Email' THEN DURATION_MINS ELSE 0 END) / 60, 2) AS EMAIL_HRS,
    
    -- Wrap Hrs (aligned with Excel)
    ROUND(SUM(CASE WHEN STATE = 'After Call Work' OR (STATE = 'Not Ready' AND REASON_CODE IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System')) THEN DURATION_MINS ELSE 0 END) / 60, 2) AS WRAP_HRS,
    
    -- Meeting Hrs
    ROUND(SUM(CASE WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Meeting', 'Team Meeting', 'Appointment') THEN DURATION_MINS ELSE 0 END) / 60, 2) AS MEETING_HRS,
    
    -- Break Hrs
    ROUND(SUM(CASE WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Break', 'Lunch') THEN DURATION_MINS ELSE 0 END) / 60, 2) AS BREAK_HRS,
    
    -- Idle Hrs
    ROUND(SUM(CASE WHEN STATE = 'Ready' THEN DURATION_MINS ELSE 0 END) / 60, 2) AS IDLE_HRS,
    
    -- Productive Hrs (Call + Email + Wrap)
    ROUND(SUM(CASE 
        WHEN STATE IN ('On Call', 'On Voicemail') 
             OR STATE = 'After Call Work'
             OR (STATE = 'Not Ready' AND REASON_CODE IN ('Outbound Calls - Manual', 'Email', 'Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
        THEN DURATION_MINS ELSE 0 
    END) / 60, 2) AS AVG_PROD_HRS,
    
    -- Total Tracked Time
    ROUND(SUM(CASE WHEN STATE NOT IN ('Logout', 'Login', 'On Hold') THEN DURATION_MINS ELSE 0 END) / 60, 2) AS TOTAL_TRACKED_HRS,
    
    -- Raw minutes for AHT calc
    SUM(CASE WHEN STATE IN ('On Call', 'On Voicemail') OR (STATE = 'Not Ready' AND REASON_CODE = 'Outbound Calls - Manual') THEN DURATION_MINS ELSE 0 END) AS CALL_TIME_MINS_RAW,
    SUM(CASE WHEN STATE = 'After Call Work' OR (STATE = 'Not Ready' AND REASON_CODE IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System')) THEN DURATION_MINS ELSE 0 END) AS WRAP_TIME_MINS_RAW
    
FROM RawData_monthly
GROUP BY CLEAN_AGENT_NAME, DATE_TRUNC('month', TO_DATE(CALL_DATE, 'YYYY/MM/DD'))
),
monthly_combined as
(
SELECT 
    COALESCE(d.agent_name, t.CLEAN_AGENT_NAME) as AGENT_NAME,
    COALESCE(d.call_month, t.call_month) as CALL_MONTH,
    
    COALESCE(d.total_dials, 0) as TOTAL_DIALS,
    COALESCE(d.total_connected, 0) as TOTAL_CONNECTED,
    
    COALESCE(t.AVG_PROD_HRS, 0) as AVG_PROD_HRS,
    COALESCE(t.CALL_HRS, 0) as CALL_HRS,
    COALESCE(t.EMAIL_HRS, 0) as EMAIL_HRS,
    COALESCE(t.WRAP_HRS, 0) as WRAP_HRS,
    COALESCE(t.MEETING_HRS, 0) as MEETING_HRS,
    COALESCE(t.BREAK_HRS, 0) as BREAK_HRS,
    COALESCE(t.IDLE_HRS, 0) as IDLE_HRS,
    COALESCE(t.TOTAL_TRACKED_HRS, 0) as TOTAL_TRACKED_HRS,
    
    COALESCE(t.CALL_TIME_MINS_RAW, 0) as CALL_TIME_MINS_RAW,
    COALESCE(t.WRAP_TIME_MINS_RAW, 0) as WRAP_TIME_MINS_RAW,
    
    COALESCE(d.payment_count, 0) as PAYMENT_COUNT,
    COALESCE(d.ptp_count, 0) as PTP_COUNT,
    COALESCE(d.rpc_count, 0) as RPC_COUNT
    
FROM monthly_dial_metrics d
FULL OUTER JOIN monthly_time_metrics t
    ON d.agent_name = t.CLEAN_AGENT_NAME
    AND d.call_month = t.call_month
)
SELECT 
    CALL_MONTH,
    AGENT_NAME,
    TOTAL_DIALS,
    TOTAL_CONNECTED,
    AVG_PROD_HRS,
    CALL_HRS,
    EMAIL_HRS,
    WRAP_HRS,
    MEETING_HRS,
    BREAK_HRS,
    IDLE_HRS,
    
    -- Connect Rate %
    ROUND((TOTAL_CONNECTED / NULLIF(TOTAL_DIALS, 0)) * 100, 2) AS CONNECT_RATE_PCT,
    
    -- Calls per Productive Hour
    ROUND(TOTAL_DIALS / NULLIF(AVG_PROD_HRS, 0), 2) AS DIALS_PER_PROD_HOUR,
    
    -- Average Handle Time (AHT) in minutes
    ROUND((CALL_TIME_MINS_RAW + WRAP_TIME_MINS_RAW) / NULLIF(TOTAL_CONNECTED, 0), 2) AS AHT_MINS,
    
    -- Occupancy % (Productive / Total Tracked)
    ROUND((AVG_PROD_HRS / NULLIF(TOTAL_TRACKED_HRS, 0)) * 100, 2) AS OCCUPANCY_PCT,
    
    -- Idle %
    ROUND((IDLE_HRS / NULLIF(TOTAL_TRACKED_HRS, 0)) * 100, 2) AS IDLE_PCT,
    
    -- Break %
    ROUND((BREAK_HRS / NULLIF(TOTAL_TRACKED_HRS, 0)) * 100, 2) AS BREAK_PCT,
    
    -- Payment/PTP/RPC rates
    ROUND((PAYMENT_COUNT / NULLIF(TOTAL_CONNECTED, 0)) * 100, 2) AS PAYMENT_RATE_PCT,
    ROUND((PTP_COUNT / NULLIF(TOTAL_CONNECTED, 0)) * 100, 2) AS PTP_RATE_PCT,
    ROUND((RPC_COUNT / NULLIF(TOTAL_CONNECTED, 0)) * 100, 2) AS RPC_RATE_PCT,
    
    -- Monthly Rankings
    RANK() OVER (PARTITION BY CALL_MONTH ORDER BY (TOTAL_CONNECTED / NULLIF(TOTAL_DIALS, 0)) DESC) AS CONNECT_RATE_RANK,
    RANK() OVER (PARTITION BY CALL_MONTH ORDER BY (TOTAL_DIALS / NULLIF(AVG_PROD_HRS, 0)) DESC) AS PRODUCTIVITY_RANK,
    RANK() OVER (PARTITION BY CALL_MONTH ORDER BY (AVG_PROD_HRS / NULLIF(TOTAL_TRACKED_HRS, 0)) DESC) AS OCCUPANCY_RANK

FROM monthly_combined
WHERE CALL_MONTH >= '2025-01-01'
  AND AGENT_NAME IS NOT NULL
ORDER BY CALL_MONTH DESC, AGENT_NAME

