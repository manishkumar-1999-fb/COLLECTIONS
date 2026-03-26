-- =====================================================
-- HEADCOUNT FRAMEWORK - EXCEL OUTPUT FORMAT
-- =====================================================
-- This code generates output matching the Excel template structure
-- Run each section separately and paste into Excel
-- =====================================================


-- =====================================================
-- QUERY 1: TOTAL DELINQUENT ACCOUNTS BY MONTH
-- =====================================================
-- Paste into Excel row: "Total Delinquent Accounts"

WITH delinquent_accounts AS (
    SELECT 
        fmd.edate,
        DATE_TRUNC('month', fmd.edate) AS report_month,
        fmd.fbbid,
        fmd.dpd_days,
        fmd.is_charged_off,
        
        CASE 
            WHEN fmd.is_charged_off = 1 THEN 'Charged Off'
            WHEN fmd.dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category,
        
        dacd.charge_off_date,
        YEAR(dacd.charge_off_date) AS charge_off_year
        
    FROM bi.finance.finance_metrics_daily fmd
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON fmd.fbbid = dacd.fbbid AND fmd.edate = dacd.edate
    WHERE fmd.edate >= '2025-01-01'
      AND (fmd.dpd_days >= 1 OR fmd.is_charged_off = 1)
)

SELECT 
    report_month,
    account_category,
    charge_off_year,
    COUNT(DISTINCT fbbid) AS total_delinquent_accounts
FROM delinquent_accounts
WHERE account_category IN ('DPD 1-13', 'Charged Off')
  AND EXTRACT(DAY FROM edate) = EXTRACT(DAY FROM LAST_DAY(edate))
GROUP BY report_month, account_category, charge_off_year
ORDER BY report_month, account_category, charge_off_year;


-- =====================================================
-- QUERY 2: DIALABLE ACCOUNTS (Unique Accounts Dialed)
-- =====================================================
-- Paste into Excel row: "Dialable Accounts"

WITH call_data_five_nine AS (
    SELECT 
        fbbid,
        DATE_TRUNC('month', date_time_call) AS call_month
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB', 'Collections - 3-12 OB', 
                          'Collections - 14 plus OB', 'Collections - Broken PTP OB',
                          'Collections - Missed Payment Priority OB'))
      AND fbbid IS NOT NULL
),

call_data_salesforce AS (
    SELECT
        TRY_TO_NUMBER(fundbox_id__c) AS fbbid,
        DATE_TRUNC('month', lastmodifieddate) AS call_month
    FROM external_data_sources.salesforce_nova.task
    WHERE DATE(lastmodifieddate) >= '2025-01-01' 
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
      AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent', 'ER agent', 'Collections Manager')
),

all_dialed AS (
    SELECT fbbid, call_month FROM call_data_five_nine
    UNION
    SELECT fbbid, call_month FROM call_data_salesforce
),

fmd_category AS (
    SELECT DISTINCT
        fbbid,
        DATE_TRUNC('month', edate) AS report_month,
        CASE 
            WHEN is_charged_off = 1 THEN 'Charged Off'
            WHEN dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category
    FROM bi.finance.finance_metrics_daily
    WHERE edate >= '2025-01-01'
)

SELECT 
    d.call_month AS report_month,
    COALESCE(f.account_category, 'Unknown') AS account_category,
    COUNT(DISTINCT d.fbbid) AS dialable_accounts
FROM all_dialed d
LEFT JOIN fmd_category f ON d.fbbid = f.fbbid AND d.call_month = f.report_month
WHERE COALESCE(f.account_category, 'Unknown') IN ('DPD 1-13', 'Charged Off')
GROUP BY d.call_month, f.account_category
ORDER BY d.call_month, f.account_category;


-- =====================================================
-- QUERY 3: UNIQUE DIALS & TOTAL DIALS
-- =====================================================
-- Paste into Excel rows: "Unique Dials" and "Total Dials"

WITH call_data_five_nine AS (
    SELECT 
        fbbid,
        date_time_call,
        DATE_TRUNC('month', date_time_call) AS call_month,
        disposition
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB', 'Collections - 3-12 OB', 
                          'Collections - 14 plus OB', 'Collections - Broken PTP OB',
                          'Collections - Missed Payment Priority OB'))
      AND fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid, date_time_call, disposition ORDER BY date_time_call) = 1
),

call_data_salesforce AS (
    SELECT
        TRY_TO_NUMBER(fundbox_id__c) AS fbbid,
        lastmodifieddate AS date_time_call,
        DATE_TRUNC('month', lastmodifieddate) AS call_month,
        calldisposition AS disposition
    FROM external_data_sources.salesforce_nova.task
    WHERE DATE(lastmodifieddate) >= '2025-01-01' 
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
      AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent', 'ER agent', 'Collections Manager')
),

all_dials AS (
    SELECT fbbid, call_month, disposition, 'Five9' AS source FROM call_data_five_nine
    UNION ALL
    SELECT fbbid, call_month, disposition, 'Salesforce' AS source FROM call_data_salesforce
),

fmd_category AS (
    SELECT DISTINCT
        fbbid,
        DATE_TRUNC('month', edate) AS report_month,
        CASE 
            WHEN is_charged_off = 1 THEN 'Charged Off'
            WHEN dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category
    FROM bi.finance.finance_metrics_daily
    WHERE edate >= '2025-01-01'
)

SELECT 
    d.call_month AS report_month,
    COALESCE(f.account_category, 
             CASE WHEN d.source = 'Salesforce' THEN 'Charged Off' ELSE 'Unknown' END
            ) AS account_category,
    COUNT(DISTINCT d.fbbid) AS unique_dials,
    COUNT(*) AS total_dials
FROM all_dials d
LEFT JOIN fmd_category f ON d.fbbid = f.fbbid AND d.call_month = f.report_month
WHERE COALESCE(f.account_category, 
               CASE WHEN d.source = 'Salesforce' THEN 'Charged Off' ELSE 'Unknown' END
              ) IN ('DPD 1-13', 'Charged Off')
GROUP BY d.call_month, COALESCE(f.account_category, 
                                 CASE WHEN d.source = 'Salesforce' THEN 'Charged Off' ELSE 'Unknown' END)
ORDER BY d.call_month, account_category;


-- =====================================================
-- QUERY 4: PENETRATION RATE & INTENSITY RATE
-- =====================================================
-- Paste into Excel rows: "Penetration Rate" and "Intensity Rate"
-- Penetration Rate = Unique Dials / Delinquent Accounts * 100
-- Intensity Rate = Total Dials / Unique Dials

WITH delinquent_accounts AS (
    SELECT 
        DATE_TRUNC('month', edate) AS report_month,
        CASE 
            WHEN is_charged_off = 1 THEN 'Charged Off'
            WHEN dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category,
        COUNT(DISTINCT fbbid) AS total_accounts
    FROM bi.finance.finance_metrics_daily
    WHERE edate >= '2025-01-01'
      AND EXTRACT(DAY FROM edate) = EXTRACT(DAY FROM LAST_DAY(edate))
      AND (dpd_days >= 1 OR is_charged_off = 1)
    GROUP BY report_month, account_category
),

dial_metrics AS (
    -- Use query 3 logic here
    SELECT 
        call_month AS report_month,
        account_category,
        unique_dials,
        total_dials
    FROM (
        -- Simplified: combining Five9 + Salesforce
        SELECT 
            DATE_TRUNC('month', COALESCE(f.date_time_call, s.lastmodifieddate)) AS call_month,
            CASE 
                WHEN fmd.is_charged_off = 1 THEN 'Charged Off'
                WHEN fmd.dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
                WHEN s.fbbid IS NOT NULL THEN 'Charged Off'
                ELSE 'Other'
            END AS account_category,
            COUNT(DISTINCT COALESCE(f.fbbid, s.fbbid)) AS unique_dials,
            COUNT(*) AS total_dials
        FROM analytics.credit.v_five9_call_log f
        FULL OUTER JOIN (
            SELECT TRY_TO_NUMBER(fundbox_id__c) AS fbbid, lastmodifieddate
            FROM external_data_sources.salesforce_nova.task
            WHERE DATE(lastmodifieddate) >= '2025-01-01' 
              AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
              AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent', 'ER agent', 'Collections Manager')
        ) s ON f.fbbid = s.fbbid AND DATE(f.date_time_call) = DATE(s.lastmodifieddate)
        LEFT JOIN bi.finance.finance_metrics_daily fmd 
            ON COALESCE(f.fbbid, s.fbbid) = fmd.fbbid 
            AND DATE(COALESCE(f.date_time_call, s.lastmodifieddate)) = fmd.edate
        WHERE DATE(COALESCE(f.date_time_call, s.lastmodifieddate)) >= '2025-01-01'
        GROUP BY call_month, account_category
    )
    WHERE account_category IN ('DPD 1-13', 'Charged Off')
)

SELECT 
    d.report_month,
    d.account_category,
    a.total_accounts AS delinquent_accounts,
    d.unique_dials,
    d.total_dials,
    ROUND(d.unique_dials * 100.0 / NULLIF(a.total_accounts, 0), 2) AS penetration_rate_pct,
    ROUND(d.total_dials * 1.0 / NULLIF(d.unique_dials, 0), 2) AS intensity_rate
FROM dial_metrics d
LEFT JOIN delinquent_accounts a 
    ON d.report_month = a.report_month 
    AND d.account_category = a.account_category
ORDER BY d.report_month, d.account_category;


-- =====================================================
-- QUERY 5: TOTAL CALLS (RPC) & DISPOSITIONS
-- =====================================================
-- Paste into Excel row: "Total Calls"

WITH call_data_five_nine AS (
    SELECT 
        fbbid,
        DATE_TRUNC('month', date_time_call) AS call_month,
        disposition,
        CASE WHEN disposition ILIKE '%Connected%' OR disposition ILIKE '%RPC%'
              OR disposition ILIKE '%Payment%' OR disposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END AS connected_flag,
        CASE WHEN disposition ILIKE '%RPC%' THEN 1 ELSE 0 END AS rpc_flag,
        CASE WHEN disposition ILIKE '%Payment%' THEN 1 ELSE 0 END AS payment_flag,
        CASE WHEN disposition ILIKE '%Promise to Pay%' THEN 1 ELSE 0 END AS ptp_flag
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB', 'Collections - 3-12 OB', 
                          'Collections - 14 plus OB', 'Collections - Broken PTP OB',
                          'Collections - Missed Payment Priority OB'))
      AND fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid, date_time_call, disposition ORDER BY date_time_call) = 1
),

call_data_salesforce AS (
    SELECT
        TRY_TO_NUMBER(fundbox_id__c) AS fbbid,
        DATE_TRUNC('month', lastmodifieddate) AS call_month,
        calldisposition AS disposition,
        CASE WHEN calldisposition ILIKE '%Connected%' OR calldisposition ILIKE '%RPC%'
              OR calldisposition ILIKE '%Payment%' OR calldisposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END AS connected_flag,
        CASE WHEN calldisposition ILIKE '%RPC%' THEN 1 ELSE 0 END AS rpc_flag,
        CASE WHEN calldisposition ILIKE '%Payment%' THEN 1 ELSE 0 END AS payment_flag,
        CASE WHEN calldisposition ILIKE '%Promise to Pay%' THEN 1 ELSE 0 END AS ptp_flag
    FROM external_data_sources.salesforce_nova.task
    WHERE DATE(lastmodifieddate) >= '2025-01-01' 
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
      AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent', 'ER agent', 'Collections Manager')
),

fmd_category AS (
    SELECT DISTINCT
        fbbid,
        DATE_TRUNC('month', edate) AS report_month,
        CASE 
            WHEN is_charged_off = 1 THEN 'Charged Off'
            WHEN dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category
    FROM bi.finance.finance_metrics_daily
    WHERE edate >= '2025-01-01'
)

SELECT 
    call_month AS report_month,
    account_category,
    SUM(connected_flag) AS total_calls_connected,
    SUM(rpc_flag) AS total_rpc,
    SUM(payment_flag) AS total_payments,
    SUM(ptp_flag) AS total_ptp
FROM (
    SELECT c.*, COALESCE(f.account_category, 'DPD 1-13') AS account_category
    FROM call_data_five_nine c
    LEFT JOIN fmd_category f ON c.fbbid = f.fbbid AND c.call_month = f.report_month
    UNION ALL
    SELECT c.*, COALESCE(f.account_category, 'Charged Off') AS account_category
    FROM call_data_salesforce c
    LEFT JOIN fmd_category f ON c.fbbid = f.fbbid AND c.call_month = f.report_month
)
WHERE account_category IN ('DPD 1-13', 'Charged Off')
GROUP BY call_month, account_category
ORDER BY call_month, account_category;


-- =====================================================
-- QUERY 6: AHT (Average Handling Time) FROM FIVE9
-- =====================================================
-- Paste into Excel row: "Average Handling Time"

WITH five9_time AS (
    SELECT 
        DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD')) AS call_month,
        REPLACE(A._DATA:"STATE", '"', '') AS state,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS reason_code,
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
        TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
        (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0) AS duration_mins
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
      AND TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') >= '2025-01-01'
),

time_metrics AS (
    SELECT 
        call_month,
        SUM(CASE WHEN state IN ('On Call', 'On Voicemail') 
                  OR (state = 'Not Ready' AND reason_code = 'Outbound Calls - Manual')
             THEN duration_mins ELSE 0 END) AS call_time_mins,
        SUM(CASE WHEN state = 'After Call Work' 
                  OR (state = 'Not Ready' AND reason_code IN ('Wrap-Up', 'Extended Research', 'Tech Issues'))
             THEN duration_mins ELSE 0 END) AS wrap_time_mins
    FROM five9_time
    GROUP BY call_month
),

connected_calls AS (
    SELECT 
        DATE_TRUNC('month', date_time_call) AS call_month,
        SUM(CASE WHEN disposition ILIKE '%Connected%' OR disposition ILIKE '%RPC%'
                  OR disposition ILIKE '%Payment%' OR disposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END) AS total_connected
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND agent_group = 'Collections Group'
      AND fbbid IS NOT NULL
    GROUP BY DATE_TRUNC('month', date_time_call)
)

SELECT 
    t.call_month AS report_month,
    'DPD 1-13' AS account_category,
    ROUND(t.call_time_mins, 0) AS total_call_time_mins,
    ROUND(t.wrap_time_mins, 0) AS total_wrap_time_mins,
    c.total_connected,
    ROUND((t.call_time_mins + t.wrap_time_mins) / NULLIF(c.total_connected, 0), 2) AS aht_mins
FROM time_metrics t
LEFT JOIN connected_calls c ON t.call_month = c.call_month
ORDER BY t.call_month;


-- =====================================================
-- QUERY 7: WORKING DAYS & PRODUCTION HOURS
-- =====================================================
-- Paste into Excel rows: "Working Days" and "Production Hours"

WITH working_days AS (
    SELECT 
        DATE_TRUNC('month', DATE(date_time_call)) AS call_month,
        COUNT(DISTINCT DATE(date_time_call)) AS working_days
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND agent_group = 'Collections Group'
    GROUP BY DATE_TRUNC('month', DATE(date_time_call))
),

agent_productive_time AS (
    SELECT 
        DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD')) AS call_month,
        COUNT(DISTINCT B.AGENT_NAME) AS agent_count,
        SUM(CASE 
            WHEN REPLACE(A._DATA:"STATE", '"', '') IN ('On Call', 'On Voicemail', 'After Call Work')
                 OR (REPLACE(A._DATA:"STATE", '"', '') = 'Not Ready' 
                     AND REPLACE(A._DATA:"REASON CODE", '"', '') IN ('Outbound Calls - Manual', 'Email', 'SMS', 'Wrap-Up'))
            THEN (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
                 TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
                 (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0)
            ELSE 0 
        END) / 60.0 AS total_productive_hours
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
      AND TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') >= '2025-01-01'
    GROUP BY DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD'))
)

SELECT 
    w.call_month AS report_month,
    'DPD 1-13' AS account_category,
    w.working_days,
    a.agent_count,
    ROUND(a.total_productive_hours, 0) AS total_productive_hours,
    ROUND(a.total_productive_hours / NULLIF(a.agent_count, 0), 0) AS production_hours_per_agent,
    ROUND(a.total_productive_hours / NULLIF(a.agent_count * w.working_days, 0), 2) AS productive_hrs_per_day
FROM working_days w
LEFT JOIN agent_productive_time a ON w.call_month = a.call_month
ORDER BY w.call_month;


-- =====================================================
-- QUERY 8: HEADCOUNT CALCULATION
-- =====================================================
-- Paste into Excel row: "Headcount Required"

WITH dial_metrics AS (
    SELECT 
        DATE_TRUNC('month', date_time_call) AS call_month,
        COUNT(*) AS total_dials,
        SUM(CASE WHEN disposition ILIKE '%Connected%' OR disposition ILIKE '%RPC%'
                  OR disposition ILIKE '%Payment%' OR disposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END) AS total_connected
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB', 'Collections - 3-12 OB', 
                          'Collections - 14 plus OB', 'Collections - Broken PTP OB'))
      AND fbbid IS NOT NULL
    GROUP BY DATE_TRUNC('month', date_time_call)
),

time_metrics AS (
    SELECT 
        DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD')) AS call_month,
        COUNT(DISTINCT B.AGENT_NAME) AS agent_count,
        COUNT(DISTINCT TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD')) AS working_days,
        SUM(CASE WHEN REPLACE(A._DATA:"STATE", '"', '') IN ('On Call', 'On Voicemail') 
                  OR (REPLACE(A._DATA:"STATE", '"', '') = 'Not Ready' 
                      AND REPLACE(A._DATA:"REASON CODE", '"', '') = 'Outbound Calls - Manual')
             THEN (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
                  TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
                  (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0)
             ELSE 0 END) AS call_time_mins,
        SUM(CASE WHEN REPLACE(A._DATA:"STATE", '"', '') = 'After Call Work' 
                  OR (REPLACE(A._DATA:"STATE", '"', '') = 'Not Ready' 
                      AND REPLACE(A._DATA:"REASON CODE", '"', '') IN ('Wrap-Up', 'Extended Research', 'Tech Issues'))
             THEN (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
                  TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
                  (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0)
             ELSE 0 END) AS wrap_time_mins,
        SUM(CASE WHEN REPLACE(A._DATA:"STATE", '"', '') NOT IN ('Logout', 'Login', 'On Hold')
             THEN (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 1) AS INT) * 60) +
                  TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 2) AS INT) +
                  (TRY_CAST(SPLIT_PART(REPLACE(A._DATA:"AGENT STATE TIME", '"', ''), ':', 3) AS INT) / 60.0)
             ELSE 0 END) AS total_tracked_mins
    FROM ANALYTICS.CREDIT.FIVE9_AGENT_DETAILS_REPORT A
    JOIN ANALYTICS.CREDIT.V_FIVE9_AGENT_INFO B
        ON REPLACE(A._DATA:"AGENT ID", '"', '') = B.AGENT_ID
    WHERE REPLACE(A._DATA:"TIME", '"', '') <> '00:00:00'
      AND REPLACE(A._DATA:"AGENT GROUP", '"', '') = 'Collections Group'
      AND TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') >= '2025-01-01'
    GROUP BY DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD'))
)

SELECT 
    d.call_month AS report_month,
    'DPD 1-13' AS account_category,
    
    -- Metrics
    d.total_dials,
    d.total_connected,
    t.agent_count AS current_headcount,
    t.working_days,
    
    -- AHT
    ROUND((t.call_time_mins + t.wrap_time_mins) / NULLIF(d.total_connected, 0), 2) AS aht_mins,
    
    -- Occupancy
    ROUND((t.call_time_mins + t.wrap_time_mins) * 100.0 / NULLIF(t.total_tracked_mins, 0), 2) AS occupancy_pct,
    
    -- Productive hours per day per agent
    ROUND(((t.call_time_mins + t.wrap_time_mins) / 60.0) / NULLIF(t.agent_count * t.working_days, 0), 2) AS productive_hrs_per_day,
    
    -- Call hours required (using 25% connect rate assumption)
    ROUND((d.total_dials * COALESCE((t.call_time_mins + t.wrap_time_mins) / NULLIF(d.total_connected, 0), 8)) / (60.0 * 0.25), 0) AS call_hours_required,
    
    -- Monthly capacity per agent
    ROUND(
        (((t.call_time_mins + t.wrap_time_mins) / 60.0) / NULLIF(t.agent_count * t.working_days, 0)) * t.working_days * 
        ((t.call_time_mins + t.wrap_time_mins) / NULLIF(t.total_tracked_mins, 0))
    , 0) AS monthly_capacity_per_agent,
    
    -- Headcount Required
    ROUND(
        ((d.total_dials * COALESCE((t.call_time_mins + t.wrap_time_mins) / NULLIF(d.total_connected, 0), 8)) / (60.0 * 0.25)) /
        NULLIF(
            (((t.call_time_mins + t.wrap_time_mins) / 60.0) / NULLIF(t.agent_count * t.working_days, 0)) * t.working_days * 
            ((t.call_time_mins + t.wrap_time_mins) / NULLIF(t.total_tracked_mins, 0))
        , 0)
    , 1) AS headcount_required

FROM dial_metrics d
LEFT JOIN time_metrics t ON d.call_month = t.call_month
ORDER BY d.call_month;


-- =====================================================
-- QUERY 9: CHARGED OFF ACCOUNTS BY CO YEAR
-- =====================================================
-- For detailed CO breakdown

WITH co_accounts AS (
    SELECT 
        DATE_TRUNC('month', edate) AS report_month,
        fbbid,
        YEAR(dacd.charge_off_date) AS charge_off_year
    FROM bi.finance.finance_metrics_daily fmd
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON fmd.fbbid = dacd.fbbid AND fmd.edate = dacd.edate
    WHERE fmd.edate >= '2025-01-01'
      AND fmd.is_charged_off = 1
      AND EXTRACT(DAY FROM fmd.edate) = EXTRACT(DAY FROM LAST_DAY(fmd.edate))
)

SELECT 
    report_month,
    charge_off_year,
    COUNT(DISTINCT fbbid) AS total_co_accounts
FROM co_accounts
GROUP BY report_month, charge_off_year
ORDER BY report_month, charge_off_year DESC;
