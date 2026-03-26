-- =====================================================
-- HEADCOUNT FRAMEWORK - MONTHLY METRICS
-- =====================================================
-- This code creates a comprehensive monthly view for headcount planning
-- 
-- Data Sources:
--   1. FMD (Finance Metrics Daily) - Delinquent accounts by DPD bucket
--   2. Five9 Call Log - Dialer data for Pre-CO Collections
--   3. Salesforce Nova Task - Dialer data for ILR/Post-CO
--   4. Five9 Agent Details - AHT calculations
--
-- Output Categories:
--   - DPD 1-13 (Pre-Charge-Off)
--   - Charged-Off (Post-CO with CO date/year)
-- =====================================================


-- =====================================================
-- SECTION 1: DELINQUENT ACCOUNTS FROM FMD
-- =====================================================
-- Get unique accounts categorized by DPD bucket and Charge-off status

WITH delinquent_accounts AS (
    SELECT 
        fmd.edate,
        DATE_TRUNC('month', fmd.edate) AS report_month,
        fmd.fbbid,
        fmd.loan_key,
        fmd.dpd_days,
        fmd.dpd_bucket,
        fmd.outstanding_balance_due,
        fmd.is_charged_off,
        
        -- Categorize accounts
        CASE 
            WHEN fmd.is_charged_off = 1 THEN 'Charged Off'
            WHEN fmd.dpd_days BETWEEN 1 AND 13 THEN 'DPD 1-13'
            ELSE 'Other'
        END AS account_category,
        
        -- For charged-off accounts, get CO date from daily_approved_customers_data
        dacd.charge_off_date,
        YEAR(dacd.charge_off_date) AS charge_off_year,
        dacd.recovery_suggested_state
        
    FROM bi.finance.finance_metrics_daily fmd
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON fmd.fbbid = dacd.fbbid
        AND fmd.edate = dacd.edate
    WHERE fmd.edate >= '2025-01-01'
      AND (fmd.dpd_days >= 1 OR fmd.is_charged_off = 1)
),

-- Monthly snapshot of delinquent accounts (end of month)
monthly_delinquent_snapshot AS (
    SELECT 
        report_month,
        account_category,
        charge_off_year,
        COUNT(DISTINCT fbbid) AS total_delinquent_accounts,
        SUM(outstanding_balance_due) AS total_outstanding_balance
    FROM delinquent_accounts
    WHERE DAYOFMONTH(edate) = EXTRACT(DAY FROM LAST_DAY(edate))  -- Last day of month
       OR edate = (SELECT MAX(edate) FROM delinquent_accounts)   -- Or latest available date
    GROUP BY report_month, account_category, charge_off_year
),

-- =====================================================
-- SECTION 2: DIALER DATA - FIVE9 (Pre-CO Collections)
-- =====================================================
call_data_five_nine AS (
    SELECT 
        fbbid,
        date_time_call,
        DATE(date_time_call) AS call_date,
        DATE_TRUNC('month', date_time_call) AS call_month,
        disposition,
        agent_name,
        agent_group,
        dpd_bucket,
        campaign,
        
        -- Disposition flags
        CASE WHEN disposition ILIKE '%Connected%' 
              OR disposition ILIKE '%RPC%'
              OR disposition ILIKE '%Payment%'
              OR disposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END AS connected_flag,
        CASE WHEN disposition ILIKE '%Payment%' THEN 1 ELSE 0 END AS payment_flag,
        CASE WHEN disposition ILIKE '%Promise to Pay%' THEN 1 ELSE 0 END AS ptp_flag,
        CASE WHEN disposition ILIKE '%RPC%' THEN 1 ELSE 0 END AS rpc_flag,
        CASE WHEN disposition ILIKE '%Settlement%' THEN 1 ELSE 0 END AS settlement_flag
        
    FROM analytics.credit.v_five9_call_log
    WHERE DATE(date_time_call) >= '2025-01-01'
      AND (agent_group = 'Collections Group' 
           OR campaign IN ('Collections - 1-2 OB',
                          'Collections - 3-12 OB',
                          'Collections - 14 plus OB',
                          'Collections - Broken PTP OB',
                          'Collections - Missed Payment Priority OB'))
      AND fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbbid, date_time_call, agent_name, disposition, dpd_bucket 
        ORDER BY date_time_call
    ) = 1
),

-- =====================================================
-- SECTION 3: DIALER DATA - SALESFORCE NOVA (Post-CO/ILR)
-- =====================================================
call_data_salesforce AS (
    SELECT
        CASE 
            WHEN fundbox_id__c = 'Not Linked' THEN NULL 
            ELSE TRY_TO_NUMBER(fundbox_id__c) 
        END AS fbbid,
        lastmodifieddate AS date_time_call,
        DATE(lastmodifieddate) AS call_date,
        DATE_TRUNC('month', lastmodifieddate) AS call_month,
        calldisposition AS disposition,
        ASSIGNEE_NAME__C AS agent_name,
        role_id_name__c AS agent_group,
        NULL AS dpd_bucket,
        NULL AS campaign,
        
        -- Disposition flags
        CASE WHEN calldisposition ILIKE '%Connected%' 
              OR calldisposition ILIKE '%RPC%'
              OR calldisposition ILIKE '%Payment%'
              OR calldisposition ILIKE '%Promise to Pay%'
             THEN 1 ELSE 0 END AS connected_flag,
        CASE WHEN calldisposition ILIKE '%Payment%' THEN 1 ELSE 0 END AS payment_flag,
        CASE WHEN calldisposition ILIKE '%Promise to Pay%' THEN 1 ELSE 0 END AS ptp_flag,
        CASE WHEN calldisposition ILIKE '%RPC%' THEN 1 ELSE 0 END AS rpc_flag,
        CASE WHEN calldisposition ILIKE '%Settlement%' THEN 1 ELSE 0 END AS settlement_flag
        
    FROM external_data_sources.salesforce_nova.task
    WHERE DATE(lastmodifieddate) >= '2025-01-01' 
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
      AND ROLE_ID_NAME__C IN ('LR Agent', 'Late Recovery Agent', 'ER agent', 'Collections Manager')
),

-- =====================================================
-- SECTION 4: COMBINE ALL DIALER DATA WITH FMD
-- =====================================================
-- Map dialer data with delinquent account status

all_dials AS (
    SELECT 
        'Five9' AS source,
        f.fbbid,
        f.call_date,
        f.call_month,
        f.disposition,
        f.agent_name,
        f.connected_flag,
        f.payment_flag,
        f.ptp_flag,
        f.rpc_flag,
        f.settlement_flag,
        
        -- Get account category from FMD
        COALESCE(d.account_category, 'Unknown') AS account_category,
        d.charge_off_year
        
    FROM call_data_five_nine f
    LEFT JOIN (
        SELECT DISTINCT fbbid, edate, account_category, charge_off_year
        FROM delinquent_accounts
    ) d ON f.fbbid = d.fbbid AND f.call_date = d.edate
    
    UNION ALL
    
    SELECT 
        'Salesforce' AS source,
        s.fbbid,
        s.call_date,
        s.call_month,
        s.disposition,
        s.agent_name,
        s.connected_flag,
        s.payment_flag,
        s.ptp_flag,
        s.rpc_flag,
        s.settlement_flag,
        
        -- Post-CO accounts from Salesforce are Charged Off
        COALESCE(d.account_category, 'Charged Off') AS account_category,
        d.charge_off_year
        
    FROM call_data_salesforce s
    LEFT JOIN (
        SELECT DISTINCT fbbid, edate, account_category, charge_off_year
        FROM delinquent_accounts
    ) d ON s.fbbid = d.fbbid AND s.call_date = d.edate
),

-- =====================================================
-- SECTION 5: MONTHLY DIAL METRICS BY CATEGORY
-- =====================================================
monthly_dial_metrics AS (
    SELECT 
        call_month,
        account_category,
        charge_off_year,
        
        -- Unique dials (unique accounts dialed)
        COUNT(DISTINCT fbbid) AS unique_dials,
        
        -- Total dials (all dial attempts)
        COUNT(*) AS total_dials,
        
        -- Connected calls (RPC)
        SUM(connected_flag) AS total_connected,
        SUM(rpc_flag) AS total_rpc,
        SUM(payment_flag) AS total_payments,
        SUM(ptp_flag) AS total_ptp,
        SUM(settlement_flag) AS total_settlements,
        
        -- Working days (distinct call dates)
        COUNT(DISTINCT call_date) AS working_days
        
    FROM all_dials
    WHERE fbbid IS NOT NULL
    GROUP BY call_month, account_category, charge_off_year
),

-- =====================================================
-- SECTION 6: AHT FROM FIVE9 AGENT DETAILS
-- =====================================================
five9_agent_time AS (
    SELECT 
        TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD') AS call_date,
        DATE_TRUNC('month', TO_DATE(REPLACE(A._DATA:"DATE", '"', ''), 'YYYY/MM/DD')) AS call_month,
        REPLACE(A._DATA:"AGENT GROUP", '"', '') AS agent_group,
        REPLACE(A._DATA:"STATE", '"', '') AS state,
        REPLACE(A._DATA:"REASON CODE", '"', '') AS reason_code,
        
        CASE 
            WHEN B.AGENT_NAME IN ('Mohsin Bin ', 'Mohsin Bin Moosa') THEN 'Mohsin bin Moosa'
            WHEN B.AGENT_NAME = 'Tiffany Lewis' THEN 'Bahari Lewis'
            ELSE B.AGENT_NAME 
        END AS agent_name,
        
        -- Duration in minutes
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

monthly_aht AS (
    SELECT 
        call_month,
        
        -- Call Time (On Call + Outbound Manual)
        SUM(CASE 
            WHEN state IN ('On Call', 'On Voicemail') 
                 OR (state = 'Not Ready' AND reason_code = 'Outbound Calls - Manual')
            THEN duration_mins ELSE 0 
        END) AS total_call_time_mins,
        
        -- Wrap Time (After Call Work + Extended Research + Tech Issues + Wrap-Up)
        SUM(CASE 
            WHEN state = 'After Call Work' 
                 OR (state = 'Not Ready' AND reason_code IN ('Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
            THEN duration_mins ELSE 0 
        END) AS total_wrap_time_mins,
        
        -- Productive Time
        SUM(CASE 
            WHEN state IN ('On Call', 'On Voicemail', 'After Call Work')
                 OR (state = 'Not Ready' AND reason_code IN ('Outbound Calls - Manual', 'Email', 'SMS', 'Wrap-Up', 'Extended Research', 'Tech Issues', 'Task Completion', 'System'))
            THEN duration_mins ELSE 0 
        END) AS total_productive_mins,
        
        -- Total Tracked Time
        SUM(CASE 
            WHEN state NOT IN ('Logout', 'Login', 'On Hold')
            THEN duration_mins ELSE 0 
        END) AS total_tracked_mins,
        
        -- Agent count
        COUNT(DISTINCT agent_name) AS agent_count,
        
        -- Working days
        COUNT(DISTINCT call_date) AS working_days
        
    FROM five9_agent_time
    GROUP BY call_month
),

-- =====================================================
-- SECTION 7: CALCULATE AHT PER CONNECTED CALL
-- =====================================================
monthly_aht_calculated AS (
    SELECT 
        a.call_month,
        a.total_call_time_mins,
        a.total_wrap_time_mins,
        a.total_productive_mins,
        a.total_tracked_mins,
        a.agent_count,
        a.working_days,
        
        -- Get total connected calls for AHT calculation
        SUM(d.total_connected) AS total_connected_calls,
        
        -- AHT = (Call Time + Wrap Time) / Connected Calls
        ROUND((a.total_call_time_mins + a.total_wrap_time_mins) / NULLIF(SUM(d.total_connected), 0), 2) AS aht_mins,
        
        -- Productive hours per day per agent
        ROUND((a.total_productive_mins / 60.0) / NULLIF(a.agent_count * a.working_days, 0), 2) AS productive_hrs_per_day,
        
        -- Occupancy Rate
        ROUND((a.total_productive_mins / NULLIF(a.total_tracked_mins, 0)) * 100, 2) AS occupancy_pct
        
    FROM monthly_aht a
    LEFT JOIN monthly_dial_metrics d ON a.call_month = d.call_month
    GROUP BY a.call_month, a.total_call_time_mins, a.total_wrap_time_mins, 
             a.total_productive_mins, a.total_tracked_mins, a.agent_count, a.working_days
)

-- =====================================================
-- FINAL OUTPUT: HEADCOUNT FRAMEWORK METRICS
-- =====================================================
SELECT 
    -- Time dimension
    d.call_month AS report_month,
    TO_CHAR(d.call_month, 'YYYY-MM') AS month_label,
    
    -- Account category
    d.account_category,
    d.charge_off_year,
    
    -- Delinquent Accounts (from snapshot)
    COALESCE(s.total_delinquent_accounts, 0) AS total_delinquent_accounts,
    
    -- Dialable Accounts (accounts that were dialed = unique dials as proxy)
    d.unique_dials AS dialable_accounts,
    
    -- Unique Dials
    d.unique_dials,
    
    -- Total Dials
    d.total_dials,
    
    -- Penetration Rate (Unique Dials / Delinquent Accounts)
    ROUND((d.unique_dials * 100.0) / NULLIF(s.total_delinquent_accounts, 0), 2) AS penetration_rate_pct,
    
    -- Intensity Rate (Total Dials / Unique Dials)
    ROUND(d.total_dials * 1.0 / NULLIF(d.unique_dials, 0), 2) AS intensity_rate,
    
    -- Total Calls (Connected/RPC)
    d.total_connected AS total_calls,
    d.total_rpc,
    d.total_payments,
    d.total_ptp,
    
    -- AHT (from Five9)
    COALESCE(a.aht_mins, 8) AS aht_mins,  -- Default to 8 if no data
    
    -- Call Hours Required (simplified: Total Dials * AHT / 60 / Connect Rate)
    -- Assuming 25% connect rate for estimation
    ROUND((d.total_dials * COALESCE(a.aht_mins, 8)) / (60.0 * 0.25), 0) AS call_hours_required,
    
    -- Occupancy Rate
    COALESCE(a.occupancy_pct, 75) AS occupancy_rate_pct,  -- Default to 75%
    
    -- Total Hours Required (Call Hours / Occupancy)
    ROUND((d.total_dials * COALESCE(a.aht_mins, 8)) / (60.0 * 0.25) / (COALESCE(a.occupancy_pct, 75) / 100.0), 0) AS total_hours_required,
    
    -- Production Hours per Agent
    COALESCE(a.productive_hrs_per_day, 5.5) * d.working_days AS production_hours_per_agent,
    
    -- Working Days
    d.working_days,
    
    -- Agent Count (from Five9)
    COALESCE(a.agent_count, 0) AS current_agent_count,
    
    -- Headcount Required
    ROUND(
        ((d.total_dials * COALESCE(a.aht_mins, 8)) / (60.0 * 0.25) / (COALESCE(a.occupancy_pct, 75) / 100.0)) /
        NULLIF(COALESCE(a.productive_hrs_per_day, 5.5) * d.working_days, 0)
    , 1) AS headcount_required

FROM monthly_dial_metrics d

LEFT JOIN monthly_delinquent_snapshot s 
    ON d.call_month = s.report_month 
    AND d.account_category = s.account_category
    AND COALESCE(d.charge_off_year, 0) = COALESCE(s.charge_off_year, 0)

LEFT JOIN monthly_aht_calculated a 
    ON d.call_month = a.call_month

WHERE d.account_category IN ('DPD 1-13', 'Charged Off')

ORDER BY d.call_month DESC, d.account_category, d.charge_off_year;


-- =====================================================
-- SUMMARY VIEW: AGGREGATED BY CATEGORY
-- =====================================================
/*
-- Run this separately for a summary view
SELECT 
    call_month,
    account_category,
    SUM(total_delinquent_accounts) AS total_delinquent_accounts,
    SUM(unique_dials) AS unique_dials,
    SUM(total_dials) AS total_dials,
    ROUND(SUM(unique_dials) * 100.0 / NULLIF(SUM(total_delinquent_accounts), 0), 2) AS penetration_rate,
    ROUND(SUM(total_dials) * 1.0 / NULLIF(SUM(unique_dials), 0), 2) AS intensity_rate,
    SUM(total_calls) AS total_rpc,
    MAX(aht_mins) AS aht_mins,
    SUM(headcount_required) AS headcount_required
FROM (
    -- Insert the main query here
)
GROUP BY call_month, account_category
ORDER BY call_month DESC, account_category;
*/


-- =====================================================
-- CHARGED OFF BREAKDOWN BY YEAR
-- =====================================================
/*
-- Run this separately for CO breakdown by year
SELECT 
    call_month,
    charge_off_year,
    SUM(total_delinquent_accounts) AS total_co_accounts,
    SUM(unique_dials) AS unique_dials,
    SUM(total_dials) AS total_dials,
    ROUND(SUM(unique_dials) * 100.0 / NULLIF(SUM(total_delinquent_accounts), 0), 2) AS penetration_rate
FROM (
    -- Insert the main query here
)
WHERE account_category = 'Charged Off'
GROUP BY call_month, charge_off_year
ORDER BY call_month DESC, charge_off_year DESC;
*/
