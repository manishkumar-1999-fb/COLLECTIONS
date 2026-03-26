-- =====================================================
-- COLLECTIONS HEADCOUNT PROJECTION FRAMEWORK
-- =====================================================
-- This framework provides SQL views for calculating headcount requirements
-- Covers: Pre-CO Collections Group + Post-CO Internal Late Recovery (ILR)
-- 
-- Components:
-- 1. Agent Productivity Benchmarks (by team/month)
-- 2. Inventory Trends with Projections (by DPD bucket)
-- 3. Headcount Calculator (demand vs supply)
-- =====================================================


-- =====================================================
-- VIEW 1: AGENT PRODUCTIVITY BENCHMARKS BY TEAM
-- =====================================================
-- Calculates monthly productivity metrics with 3-month rolling averages
-- Teams: Collections Group (Pre-CO), ILR (Post-CO Internal Recovery)

CREATE OR REPLACE VIEW analytics.credit.v_headcount_productivity_benchmarks AS

WITH 
-- Pre-CO: Five9 Call Data for Collections Group
call_data_five_nine AS (
    SELECT 
        FBBID,
        Date_time_call,
        DISPOSITION,
        AGENT_NAME,
        AGENT_GROUP,
        DPD_BUCKET,
        'Collections' AS team_type
    FROM analytics.credit.v_five9_call_log
    WHERE date(date_time_call) >= DATEADD('month', -6, CURRENT_DATE)
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB',
                           'Collections - 3-12 OB',
                           'Collections - 14 plus OB',
                           'Collections - Broken PTP OB',
                           'Collections - Missed Payment Priority OB'))
      AND fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid, date_time_call, agent_name, disposition, DPD_bucket ORDER BY date_time_call) = 1
),

-- Post-CO ILR: Salesforce Task Data for Internal Late Recovery
call_data_ilr AS (
    SELECT
        CASE WHEN fundbox_id__c = 'Not Linked' THEN 0 ELSE TRY_TO_NUMBER(fundbox_id__c) END AS fbbid,
        lastmodifieddate AS date_time_call,
        calldisposition AS disposition,
        ASSIGNEE_NAME__C AS agent_name,
        role_id_name__c AS agent_group,
        NULL AS dpd_bucket,
        'ILR' AS team_type
    FROM external_data_sources.salesforce_nova.task t
    WHERE date(lastmodifieddate) >= DATEADD('month', -6, CURRENT_DATE)
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
      AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent')
      AND EXISTS (
          SELECT 1 FROM bi.public.daily_approved_customers_data d
          WHERE TRY_TO_NUMBER(t.fundbox_id__c) = d.fbbid
            AND d.recovery_suggested_state = 'ILR'
      )
),

-- Combine all call data
all_calls AS (
    SELECT fbbid, date_time_call, disposition, agent_name, agent_group, dpd_bucket, team_type
    FROM call_data_five_nine
    UNION ALL
    SELECT fbbid, date_time_call, disposition, agent_name, agent_group, dpd_bucket, team_type
    FROM call_data_ilr
),

-- Dialing metrics by agent/day
dialing_data AS (
    SELECT 
        team_type,
        agent_name,
        DATE(date_time_call) AS call_date,
        DATE_TRUNC('month', date_time_call) AS call_month,
        fbbid,
        CASE WHEN disposition ILIKE '%Connected%' 
              OR disposition ILIKE '%RPC%'
              OR disposition ILIKE '%Payment%'
              OR disposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END AS connected_flag
    FROM all_calls
    WHERE agent_name IS NOT NULL
),

-- Agent daily dial metrics
agent_dial_metrics AS (
    SELECT 
        team_type,
        agent_name,
        call_date,
        call_month,
        COUNT(DISTINCT fbbid) AS unique_accounts,
        COUNT(*) AS total_dials,
        SUM(connected_flag) AS total_connected
    FROM dialing_data
    GROUP BY team_type, agent_name, call_date, call_month
),

-- Agent time data from Five9 (Collections Group)
RawData_time AS (
    SELECT 
        REPLACE(A._DATA:"AGENT GROUP", '"', '') AS agent_group,
        TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') AS call_date,
        REPLACE(A._DATA:"STATE", '"', '') AS state,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS reason_code,
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS agent_name,
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0) AS duration_mins
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
      AND TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') >= DATEADD('month', -6, CURRENT_DATE)
),

-- Agent time metrics
agent_time_metrics AS (
    SELECT 
        agent_name,
        call_date,
        DATE_TRUNC('month', call_date) AS call_month,
        'Collections' AS team_type,
        
        -- Call Time (On Call + Outbound Manual)
        SUM(CASE WHEN state IN ('On Call', 'On Voicemail') 
                  OR (state = 'Not Ready' AND reason_code = 'Outbound Calls - Manual') 
             THEN duration_mins ELSE 0 END) AS call_time_mins,
        
        -- Wrap Time
        SUM(CASE WHEN state = 'After Call Work' 
                  OR (state = 'Not Ready' AND reason_code IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System')) 
             THEN duration_mins ELSE 0 END) AS wrap_time_mins,
        
        -- Productive Time (Call + Email + SMS + Wrap)
        SUM(CASE WHEN state IN ('On Call', 'On Voicemail') 
                  OR state = 'After Call Work'
                  OR (state = 'Not Ready' AND reason_code IN ('Outbound Calls - Manual', 'Email', 'SMS', 'Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
             THEN duration_mins ELSE 0 END) AS productive_time_mins,
        
        -- Total Tracked Time
        SUM(CASE WHEN state NOT IN ('Logout', 'Login', 'On Hold') 
             THEN duration_mins ELSE 0 END) AS total_tracked_mins
             
    FROM RawData_time
    GROUP BY agent_name, call_date, DATE_TRUNC('month', call_date)
),

-- Combine dial and time metrics at daily level
daily_combined AS (
    SELECT 
        COALESCE(d.team_type, t.team_type) AS team_type,
        COALESCE(d.agent_name, t.agent_name) AS agent_name,
        COALESCE(d.call_date, t.call_date) AS call_date,
        COALESCE(d.call_month, t.call_month) AS call_month,
        COALESCE(d.total_dials, 0) AS total_dials,
        COALESCE(d.total_connected, 0) AS total_connected,
        COALESCE(t.call_time_mins, 0) AS call_time_mins,
        COALESCE(t.wrap_time_mins, 0) AS wrap_time_mins,
        COALESCE(t.productive_time_mins, 0) AS productive_time_mins,
        COALESCE(t.total_tracked_mins, 0) AS total_tracked_mins
    FROM agent_dial_metrics d
    FULL OUTER JOIN agent_time_metrics t
        ON d.agent_name = t.agent_name
        AND d.call_date = t.call_date
        AND d.team_type = t.team_type
),

-- Monthly agent aggregates
monthly_agent AS (
    SELECT 
        team_type,
        agent_name,
        call_month,
        COUNT(DISTINCT call_date) AS working_days,
        SUM(total_dials) AS total_dials,
        SUM(total_connected) AS total_connected,
        SUM(productive_time_mins) / 60.0 AS productive_hours,
        SUM(total_tracked_mins) / 60.0 AS total_tracked_hours,
        SUM(call_time_mins) AS call_time_mins,
        SUM(wrap_time_mins) AS wrap_time_mins
    FROM daily_combined
    WHERE agent_name IS NOT NULL
    GROUP BY team_type, agent_name, call_month
),

-- Monthly team benchmarks
monthly_benchmarks AS (
    SELECT 
        team_type,
        call_month,
        
        -- Agent count
        COUNT(DISTINCT agent_name) AS agent_count,
        
        -- Averages per agent
        AVG(productive_hours / NULLIF(working_days, 0)) AS avg_productive_hrs_per_day,
        AVG(total_dials / NULLIF(productive_hours, 0)) AS avg_dials_per_productive_hour,
        AVG((call_time_mins + wrap_time_mins) / NULLIF(total_connected, 0)) AS avg_aht_mins,
        AVG(total_connected * 100.0 / NULLIF(total_dials, 0)) AS avg_connect_rate_pct,
        AVG(productive_hours * 100.0 / NULLIF(total_tracked_hours, 0)) AS avg_occupancy_pct,
        
        -- Totals for team
        SUM(total_dials) AS team_total_dials,
        SUM(total_connected) AS team_total_connected,
        SUM(productive_hours) AS team_productive_hours
        
    FROM monthly_agent
    GROUP BY team_type, call_month
)

SELECT 
    team_type,
    call_month,
    agent_count,
    
    -- Daily productivity
    ROUND(avg_productive_hrs_per_day, 2) AS avg_productive_hrs_per_day,
    ROUND(avg_dials_per_productive_hour, 2) AS avg_dials_per_productive_hour,
    ROUND(avg_aht_mins, 2) AS avg_aht_mins,
    ROUND(avg_connect_rate_pct, 2) AS avg_connect_rate_pct,
    ROUND(avg_occupancy_pct, 2) AS avg_occupancy_pct,
    
    -- Team totals
    team_total_dials,
    team_total_connected,
    ROUND(team_productive_hours, 2) AS team_productive_hours,
    
    -- 3-month rolling averages for stability
    ROUND(AVG(avg_productive_hrs_per_day) OVER (PARTITION BY team_type ORDER BY call_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_productive_hrs,
    ROUND(AVG(avg_dials_per_productive_hour) OVER (PARTITION BY team_type ORDER BY call_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_dials_per_hour,
    ROUND(AVG(avg_aht_mins) OVER (PARTITION BY team_type ORDER BY call_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_aht_mins,
    ROUND(AVG(avg_connect_rate_pct) OVER (PARTITION BY team_type ORDER BY call_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_connect_rate,
    ROUND(AVG(avg_occupancy_pct) OVER (PARTITION BY team_type ORDER BY call_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_occupancy_pct

FROM monthly_benchmarks
ORDER BY team_type, call_month DESC;


-- =====================================================
-- VIEW 2: INVENTORY TRENDS WITH PROJECTIONS
-- =====================================================
-- Weekly inventory by DPD bucket with 4-week averages and trend projections
-- Buckets: 1-2, 3-8, 9-13 (Pre-CO), ILR (Post-CO Internal)

CREATE OR REPLACE VIEW analytics.credit.v_headcount_inventory_trends AS

WITH 
-- Pre-CO Inventory from existing snapshot metrics
preco_inventory AS (
    SELECT 
        week_end_date,
        '1-2' AS dpd_bucket,
        num_cust_1_2 AS account_count,
        sum_os_1_2_snapshot AS outstanding_balance
    FROM analytics.credit.km_snapshot_preco_metrics
    WHERE week_end_date >= DATEADD('month', -6, CURRENT_DATE)
    
    UNION ALL
    
    SELECT 
        week_end_date,
        '3-8' AS dpd_bucket,
        num_cust_3_8 AS account_count,
        sum_os_3_8_snapshot AS outstanding_balance
    FROM analytics.credit.km_snapshot_preco_metrics
    WHERE week_end_date >= DATEADD('month', -6, CURRENT_DATE)
    
    UNION ALL
    
    SELECT 
        week_end_date,
        '9-13' AS dpd_bucket,
        num_cust_9_13 AS account_count,
        sum_os_9_13_snapshot AS outstanding_balance
    FROM analytics.credit.km_snapshot_preco_metrics
    WHERE week_end_date >= DATEADD('month', -6, CURRENT_DATE)
),

-- Post-CO ILR Inventory
ilr_inventory AS (
    SELECT 
        DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS week_end_date,
        'ILR' AS dpd_bucket,
        COUNT(DISTINCT fbbid) AS account_count,
        SUM(outstanding_principal) AS outstanding_balance
    FROM bi.public.daily_approved_customers_data
    WHERE recovery_suggested_state = 'ILR'
      AND DAYOFWEEK(edate) = 3
      AND edate >= DATEADD('month', -6, CURRENT_DATE)
    GROUP BY DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2
),

-- Combine all inventory
all_inventory AS (
    SELECT * FROM preco_inventory
    UNION ALL
    SELECT * FROM ilr_inventory
),

-- Calculate 4-week averages and trends
inventory_with_trends AS (
    SELECT 
        week_end_date,
        dpd_bucket,
        account_count,
        outstanding_balance,
        
        -- 4-week rolling average
        AVG(account_count) OVER (
            PARTITION BY dpd_bucket 
            ORDER BY week_end_date 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS avg_4_week_accounts,
        
        -- Prior 4-week average (for trend calculation)
        AVG(account_count) OVER (
            PARTITION BY dpd_bucket 
            ORDER BY week_end_date 
            ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING
        ) AS prior_4_week_avg,
        
        -- Week-over-week change
        account_count - LAG(account_count, 1) OVER (PARTITION BY dpd_bucket ORDER BY week_end_date) AS wow_change,
        
        -- 4-week trend (slope) - calculated manually since REGR_SLOPE doesn't support sliding windows
        -- Using: (current_avg - prior_week_avg) as weekly trend indicator
        (AVG(account_count) OVER (
            PARTITION BY dpd_bucket 
            ORDER BY week_end_date 
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) - AVG(account_count) OVER (
            PARTITION BY dpd_bucket 
            ORDER BY week_end_date 
            ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING
        )) AS trend_slope
        
    FROM all_inventory
)

SELECT 
    week_end_date,
    dpd_bucket,
    account_count,
    ROUND(outstanding_balance, 2) AS outstanding_balance,
    ROUND(avg_4_week_accounts, 0) AS avg_4_week_accounts,
    wow_change,
    
    -- Trend direction
    CASE 
        WHEN trend_slope > 5 THEN 'Increasing'
        WHEN trend_slope < -5 THEN 'Decreasing'
        ELSE 'Stable'
    END AS trend_direction,
    
    -- Monthly growth rate
    ROUND((avg_4_week_accounts - prior_4_week_avg) * 100.0 / NULLIF(prior_4_week_avg, 0), 2) AS monthly_growth_rate_pct,
    
    -- Projected next month (simple trend-based)
    ROUND(avg_4_week_accounts * (1 + COALESCE((avg_4_week_accounts - prior_4_week_avg) / NULLIF(prior_4_week_avg, 0), 0)), 0) AS projected_next_month,
    
    -- Assign team for headcount calculation
    CASE 
        WHEN dpd_bucket IN ('1-2', '3-8', '9-13') THEN 'Collections'
        WHEN dpd_bucket = 'ILR' THEN 'ILR'
    END AS team_type

FROM inventory_with_trends
ORDER BY dpd_bucket, week_end_date DESC;


-- =====================================================
-- VIEW 3: HEADCOUNT CALCULATOR
-- =====================================================
-- Combines demand (workload) and supply (capacity) to calculate headcount
-- Assumes configurable contact policy via parameters

CREATE OR REPLACE VIEW analytics.credit.v_headcount_calculator AS

WITH 
-- Get latest inventory by bucket
latest_inventory AS (
    SELECT 
        dpd_bucket,
        team_type,
        account_count,
        avg_4_week_accounts,
        projected_next_month
    FROM analytics.credit.v_headcount_inventory_trends
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dpd_bucket ORDER BY week_end_date DESC) = 1
),

-- Get latest productivity benchmarks
latest_benchmarks AS (
    SELECT 
        team_type,
        agent_count AS current_headcount,
        rolling_3m_productive_hrs AS productive_hrs_per_day,
        rolling_3m_dials_per_hour AS dials_per_productive_hour,
        rolling_3m_aht_mins AS aht_mins,
        rolling_3m_connect_rate AS connect_rate_pct,
        rolling_3m_occupancy_pct AS occupancy_pct
    FROM analytics.credit.v_headcount_productivity_benchmarks
    QUALIFY ROW_NUMBER() OVER (PARTITION BY team_type ORDER BY call_month DESC) = 1
),

-- Default contact policy assumptions (adjust these as needed)
contact_policy AS (
    SELECT '1-2' AS dpd_bucket, 4 AS contact_attempts_per_month, 'Collections' AS team_type
    UNION ALL SELECT '3-8', 6, 'Collections'
    UNION ALL SELECT '9-13', 8, 'Collections'
    UNION ALL SELECT 'ILR', 5, 'ILR'
),

-- Calculate demand (required work hours)
demand_calc AS (
    SELECT 
        i.team_type,
        i.dpd_bucket,
        i.account_count AS current_accounts,
        i.projected_next_month AS projected_accounts,
        cp.contact_attempts_per_month,
        b.aht_mins,
        b.connect_rate_pct,
        
        -- Current workload
        i.account_count * cp.contact_attempts_per_month AS required_dials_current,
        
        -- Projected workload
        i.projected_next_month * cp.contact_attempts_per_month AS required_dials_projected,
        
        -- Required work hours (current)
        (i.account_count * cp.contact_attempts_per_month * b.aht_mins) / (60.0 * (b.connect_rate_pct / 100.0)) AS required_work_hours_current,
        
        -- Required work hours (projected)
        (i.projected_next_month * cp.contact_attempts_per_month * b.aht_mins) / (60.0 * (b.connect_rate_pct / 100.0)) AS required_work_hours_projected
        
    FROM latest_inventory i
    JOIN contact_policy cp ON i.dpd_bucket = cp.dpd_bucket
    JOIN latest_benchmarks b ON i.team_type = b.team_type
),

-- Aggregate demand by team
team_demand AS (
    SELECT 
        team_type,
        SUM(current_accounts) AS total_current_accounts,
        SUM(projected_accounts) AS total_projected_accounts,
        SUM(required_dials_current) AS total_required_dials_current,
        SUM(required_dials_projected) AS total_required_dials_projected,
        SUM(required_work_hours_current) AS total_work_hours_current,
        SUM(required_work_hours_projected) AS total_work_hours_projected
    FROM demand_calc
    GROUP BY team_type
),

-- Calculate supply (capacity per agent)
supply_calc AS (
    SELECT 
        b.team_type,
        b.current_headcount,
        b.productive_hrs_per_day,
        b.occupancy_pct,
        22 AS working_days_per_month,
        0.15 AS buffer_pct,
        
        -- Monthly capacity per agent
        b.productive_hrs_per_day * 22 * (b.occupancy_pct / 100.0) AS monthly_capacity_per_agent,
        
        -- Total team capacity
        b.current_headcount * b.productive_hrs_per_day * 22 * (b.occupancy_pct / 100.0) AS total_team_capacity
        
    FROM latest_benchmarks b
)

-- Final headcount calculation
SELECT 
    d.team_type,
    
    -- Current State
    d.total_current_accounts,
    ROUND(d.total_work_hours_current, 2) AS required_work_hours_current,
    s.current_headcount,
    ROUND(s.total_team_capacity, 2) AS current_team_capacity_hours,
    
    -- Headcount needed (current)
    CEIL(d.total_work_hours_current / NULLIF(s.monthly_capacity_per_agent, 0)) AS headcount_needed_current,
    CEIL(d.total_work_hours_current / NULLIF(s.monthly_capacity_per_agent, 0) * (1 + s.buffer_pct)) AS headcount_with_buffer_current,
    
    -- Gap analysis (current)
    s.current_headcount - CEIL(d.total_work_hours_current / NULLIF(s.monthly_capacity_per_agent, 0)) AS headcount_gap_current,
    
    -- Projected State
    d.total_projected_accounts,
    ROUND(d.total_work_hours_projected, 2) AS required_work_hours_projected,
    
    -- Headcount needed (projected)
    CEIL(d.total_work_hours_projected / NULLIF(s.monthly_capacity_per_agent, 0)) AS headcount_needed_projected,
    CEIL(d.total_work_hours_projected / NULLIF(s.monthly_capacity_per_agent, 0) * (1 + s.buffer_pct)) AS headcount_with_buffer_projected,
    
    -- Gap analysis (projected)
    s.current_headcount - CEIL(d.total_work_hours_projected / NULLIF(s.monthly_capacity_per_agent, 0)) AS headcount_gap_projected,
    
    -- Capacity metrics
    ROUND(s.monthly_capacity_per_agent, 2) AS monthly_capacity_per_agent_hrs,
    ROUND(s.productive_hrs_per_day, 2) AS productive_hrs_per_day,
    ROUND(s.occupancy_pct, 2) AS occupancy_pct,
    ROUND(s.buffer_pct * 100, 0) AS buffer_pct,
    
    -- Utilization
    ROUND((d.total_work_hours_current / NULLIF(s.total_team_capacity, 0)) * 100, 2) AS current_utilization_pct

FROM team_demand d
JOIN supply_calc s ON d.team_type = s.team_type
ORDER BY d.team_type;


-- =====================================================
-- VIEW 4: DETAILED HEADCOUNT BY DPD BUCKET
-- =====================================================
-- Provides granular headcount breakdown by DPD bucket for planning

CREATE OR REPLACE VIEW analytics.credit.v_headcount_by_bucket AS

WITH 
-- Get latest inventory by bucket
latest_inventory AS (
    SELECT 
        dpd_bucket,
        team_type,
        account_count,
        avg_4_week_accounts,
        projected_next_month,
        trend_direction,
        monthly_growth_rate_pct
    FROM analytics.credit.v_headcount_inventory_trends
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dpd_bucket ORDER BY week_end_date DESC) = 1
),

-- Get latest productivity benchmarks
latest_benchmarks AS (
    SELECT 
        team_type,
        rolling_3m_aht_mins AS aht_mins,
        rolling_3m_connect_rate AS connect_rate_pct,
        rolling_3m_productive_hrs AS productive_hrs_per_day,
        rolling_3m_occupancy_pct AS occupancy_pct
    FROM analytics.credit.v_headcount_productivity_benchmarks
    QUALIFY ROW_NUMBER() OVER (PARTITION BY team_type ORDER BY call_month DESC) = 1
),

-- Contact policy
contact_policy AS (
    SELECT '1-2' AS dpd_bucket, 4 AS contact_attempts_per_month
    UNION ALL SELECT '3-8', 6
    UNION ALL SELECT '9-13', 8
    UNION ALL SELECT 'ILR', 5
)

SELECT 
    i.dpd_bucket,
    i.team_type,
    i.account_count AS current_accounts,
    i.projected_next_month AS projected_accounts,
    i.trend_direction,
    ROUND(i.monthly_growth_rate_pct, 2) AS growth_rate_pct,
    
    cp.contact_attempts_per_month,
    ROUND(b.aht_mins, 2) AS aht_mins,
    ROUND(b.connect_rate_pct, 2) AS connect_rate_pct,
    
    -- Required dials
    i.account_count * cp.contact_attempts_per_month AS required_dials_current,
    i.projected_next_month * cp.contact_attempts_per_month AS required_dials_projected,
    
    -- Required work hours
    ROUND((i.account_count * cp.contact_attempts_per_month * b.aht_mins) / (60.0 * (b.connect_rate_pct / 100.0)), 2) AS work_hours_current,
    ROUND((i.projected_next_month * cp.contact_attempts_per_month * b.aht_mins) / (60.0 * (b.connect_rate_pct / 100.0)), 2) AS work_hours_projected,
    
    -- FTE equivalent (based on 22 days, productive hours, occupancy)
    ROUND(
        (i.account_count * cp.contact_attempts_per_month * b.aht_mins) / 
        (60.0 * (b.connect_rate_pct / 100.0)) / 
        (b.productive_hrs_per_day * 22 * (b.occupancy_pct / 100.0))
    , 2) AS fte_needed_current,
    
    ROUND(
        (i.projected_next_month * cp.contact_attempts_per_month * b.aht_mins) / 
        (60.0 * (b.connect_rate_pct / 100.0)) / 
        (b.productive_hrs_per_day * 22 * (b.occupancy_pct / 100.0))
    , 2) AS fte_needed_projected

FROM latest_inventory i
JOIN contact_policy cp ON i.dpd_bucket = cp.dpd_bucket
JOIN latest_benchmarks b ON i.team_type = b.team_type
ORDER BY 
    CASE i.dpd_bucket 
        WHEN '1-2' THEN 1 
        WHEN '3-8' THEN 2 
        WHEN '9-13' THEN 3 
        WHEN 'ILR' THEN 4 
    END;


-- =====================================================
-- QUERY: SCENARIO ANALYSIS
-- =====================================================
-- Use this query to model different scenarios
-- Adjust the parameters in the WITH clause

/*
-- Example: Run scenario analysis
WITH scenario_params AS (
    SELECT 
        -- Scenario 1: Optimistic (lower volume, higher productivity)
        0.90 AS volume_multiplier_optimistic,
        1.10 AS productivity_multiplier_optimistic,
        
        -- Scenario 2: Base (current trends)
        1.00 AS volume_multiplier_base,
        1.00 AS productivity_multiplier_base,
        
        -- Scenario 3: Pessimistic (higher volume, lower productivity)
        1.15 AS volume_multiplier_pessimistic,
        0.90 AS productivity_multiplier_pessimistic,
        
        -- Buffer percentage
        0.15 AS buffer_pct
)
SELECT 
    h.team_type,
    h.total_projected_accounts,
    h.required_work_hours_projected,
    h.headcount_needed_projected AS headcount_base,
    
    -- Optimistic scenario
    CEIL(h.required_work_hours_projected * s.volume_multiplier_optimistic / 
         (h.monthly_capacity_per_agent_hrs * s.productivity_multiplier_optimistic) * 
         (1 + s.buffer_pct)) AS headcount_optimistic,
    
    -- Pessimistic scenario
    CEIL(h.required_work_hours_projected * s.volume_multiplier_pessimistic / 
         (h.monthly_capacity_per_agent_hrs * s.productivity_multiplier_pessimistic) * 
         (1 + s.buffer_pct)) AS headcount_pessimistic

FROM analytics.credit.v_headcount_calculator h
CROSS JOIN scenario_params s;
*/


-- =====================================================
-- SUMMARY QUERIES FOR EXCEL EXPORT
-- =====================================================

-- Query 1: Productivity Benchmarks (for Excel Data Import tab)
-- SELECT * FROM analytics.credit.v_headcount_productivity_benchmarks ORDER BY team_type, call_month DESC;

-- Query 2: Inventory Trends (for Excel Data Import tab)
-- SELECT * FROM analytics.credit.v_headcount_inventory_trends ORDER BY dpd_bucket, week_end_date DESC;

-- Query 3: Headcount Calculator (for Excel Capacity Calculator tab)
-- SELECT * FROM analytics.credit.v_headcount_calculator;

-- Query 4: Detailed by Bucket (for Excel granular analysis)
-- SELECT * FROM analytics.credit.v_headcount_by_bucket;
