WITH RawData AS (
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
),
agent_data as
(
SELECT 
    CLEAN_AGENT_NAME,
    AGENT_EMAIL,
    AGENT_GROUP,
    CALL_DATE,
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
),
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
    FROM agent_data
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
            WHEN STATE IN ('On Call', 'On Hold', 'On Park', 'On Voicemail', 'DID Ringing', 'Ringing') 
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
        
        -- 4. Wrap Time: After Call Work OR (Not Ready + Wrap-Up)
        SUM(CASE 
            WHEN STATE = 'After Call Work' 
                 OR (STATE = 'Not Ready' AND REASON_CODE = 'Wrap-Up')
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
        
        -- 8. Research/Task Time: Not Ready + (Extended Research, Task Completion)
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Extended Research', 'Task Completion')
            THEN DURATION_MINS ELSE 0 
        END) AS RESEARCH_TASK_MINS,
        
        -- 9. Tech Issues/System Time: Not Ready + (Tech Issues, System, Forced)
        SUM(CASE 
            WHEN STATE = 'Not Ready' AND REASON_CODE IN ('Tech Issues', 'System', 'Forced')
            THEN DURATION_MINS ELSE 0 
        END) AS TECH_SYSTEM_MINS,
        
        -- 10. Other Time: Login, Not Ready with (No Reason, Not Ready, blank)
        SUM(CASE 
            WHEN STATE = 'Login'
                 OR (STATE = 'Not Ready' AND (REASON_CODE IN ('No Reason', 'Not Ready') OR REASON_CODE IS NULL OR REASON_CODE = ''))
            THEN DURATION_MINS ELSE 0 
        END) AS OTHER_TIME_MINS,
        
        -- Total tracked time (excluding Logout)
        SUM(CASE 
            WHEN STATE <> 'Logout'
            THEN DURATION_MINS ELSE 0 
        END) AS TOTAL_TRACKED_MINS
        
    FROM agent_data
    GROUP BY CLEAN_AGENT_NAME, AGENT_GROUP, CALL_DATE
)

-- Final Output: Agent Efficiency Report
SELECT 
    l.CLEAN_AGENT_NAME,
    l.AGENT_GROUP,
    l.CALL_DATE,
    
    -- Login Information
    l.FIRST_LOGIN_TIME,
    l.LAST_LOGOUT_TIME,
    ROUND(l.LOGGED_IN_MINS / 60, 2) AS LOGGED_IN_HOURS,
    ROUND(a.TOTAL_TRACKED_MINS / 60, 2) AS TOTAL_TRACKED_HOURS,
    
    -- Activity Breakdown (in hours)
    ROUND(a.CALL_TIME_MINS / 60, 2) AS CALL_TIME_HOURS,
    ROUND(a.EMAIL_TIME_MINS / 60, 2) AS EMAIL_TIME_HOURS,
    ROUND(a.SMS_TIME_MINS / 60, 2) AS SMS_TIME_HOURS,
    ROUND(a.WRAP_TIME_MINS / 60, 2) AS WRAP_TIME_HOURS,
    ROUND(a.IDLE_TIME_MINS / 60, 2) AS IDLE_TIME_HOURS,
    ROUND(a.BREAK_LUNCH_MINS / 60, 2) AS BREAK_LUNCH_HOURS,
    ROUND(a.MEETING_TRAINING_MINS / 60, 2) AS MEETING_TRAINING_HOURS,
    ROUND(a.RESEARCH_TASK_MINS / 60, 2) AS RESEARCH_TASK_HOURS,
    ROUND(a.TECH_SYSTEM_MINS / 60, 2) AS TECH_SYSTEM_HOURS,
    ROUND(a.OTHER_TIME_MINS / 60, 2) AS OTHER_TIME_HOURS,
    
    -- Verification: Sum of all activities should equal Total Tracked Time
    ROUND((a.CALL_TIME_MINS + a.EMAIL_TIME_MINS + a.SMS_TIME_MINS + a.WRAP_TIME_MINS + 
           a.IDLE_TIME_MINS + a.BREAK_LUNCH_MINS + a.MEETING_TRAINING_MINS + 
           a.RESEARCH_TASK_MINS + a.TECH_SYSTEM_MINS + a.OTHER_TIME_MINS) / 60, 2) AS SUM_ALL_ACTIVITIES_HOURS,
    
    -- Efficiency Percentages (based on total tracked time)
    ROUND((a.CALL_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS CALL_TIME_PCT,
    ROUND((a.EMAIL_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS EMAIL_TIME_PCT,
    ROUND((a.SMS_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS SMS_TIME_PCT,
    ROUND((a.WRAP_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS WRAP_TIME_PCT,
    ROUND((a.IDLE_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS IDLE_TIME_PCT,
    ROUND((a.BREAK_LUNCH_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS BREAK_LUNCH_PCT,
    ROUND((a.MEETING_TRAINING_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS MEETING_TRAINING_PCT,
    ROUND((a.RESEARCH_TASK_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS RESEARCH_TASK_PCT,
    ROUND((a.TECH_SYSTEM_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS TECH_SYSTEM_PCT,
    ROUND((a.OTHER_TIME_MINS / NULLIF(a.TOTAL_TRACKED_MINS, 0)) * 100, 2) AS OTHER_TIME_PCT

FROM login_hours l
JOIN activity_metrics a
    ON l.CLEAN_AGENT_NAME = a.CLEAN_AGENT_NAME
    AND l.CALL_DATE = a.CALL_DATE
ORDER BY l.CALL_DATE DESC, l.CLEAN_AGENT_NAME;