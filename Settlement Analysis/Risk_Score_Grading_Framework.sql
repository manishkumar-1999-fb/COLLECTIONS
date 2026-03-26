-- =====================================================
-- RISK SCORE & GRADING FRAMEWORK
-- =====================================================
-- Purpose: Generate risk scores and grades for delinquent accounts
--          using feature store data and track charge-off outcomes
--
-- Source Table: DATA_SCIENCE.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_ALL_FEATURES
-- Base Date: Delinquent non-charged-off accounts as of Jan 1st 2025
-- =====================================================

-- Set database context
USE DATABASE ANALYTICS;
USE SCHEMA CREDIT;

-- =====================================================
-- STEP 1: IDENTIFY DELINQUENT NON-CHARGED-OFF ACCOUNTS AS OF JAN 1, 2025
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE delinquent_accounts_jan2025 AS
SELECT DISTINCT
    fbbid::VARCHAR AS fbbid,
    dpd_days::INT AS dpd_at_snapshot,
    outstanding_principal_due,
    is_charged_off
FROM bi.finance.finance_metrics_daily
WHERE edate = '2025-01-01'
  AND product_type <> 'Flexpay'
  AND dpd_days::INT > 0                    -- Delinquent (DPD > 0)
  AND is_charged_off = 0                   -- Not charged off yet
;


-- =====================================================
-- STEP 2: PIVOT FEATURE STORE TO WIDE FORMAT
-- =====================================================
-- Get the relevant features for each fbbid from the feature store
-- Using features closest to Jan 1, 2025

CREATE OR REPLACE TEMPORARY TABLE feature_store_pivoted AS
WITH feature_latest AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        UPPER(feature_name) AS feature_name,
        feature_value,
        cutoff_time,
        ROW_NUMBER() OVER (
            PARTITION BY fbbid, UPPER(feature_name)
            ORDER BY cutoff_time DESC
        ) AS rn
    FROM DATA_SCIENCE.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_ALL_FEATURES
    WHERE cutoff_time <= '2025-01-01'
      AND fbbid IN (SELECT fbbid FROM delinquent_accounts_jan2025)
)
SELECT
    fbbid,
    
    -- Vantage Score
    MAX(CASE WHEN feature_name = 'CR_V2_VANTAGE_UPGRADED' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS vantage_score,
    
    -- Vantage Change (% change)
    MAX(CASE WHEN feature_name = 'FP_PCT_CHANGE_VANTAGE_SCORE_BEFORE_DPD_TO_CURRENT' AND rn = 1 
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END) AS vantage_pct_change,
    
    -- Accounts in Collection (2 year)
    MAX(CASE WHEN feature_name = 'CR_V1_TL_2Y_NUM_COLLECTION' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS accounts_in_collection,
    
    -- Past Due Accounts (90 day)
    MAX(CASE WHEN feature_name = 'CR_V1_TL_90D_NUM_PAST_DUES' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS past_due_accounts,
    
    -- Historical Bankruptcies (5 year)
    MAX(CASE WHEN feature_name = 'CR_V1_PR_5Y_NUM_BANKRUPTCIES' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS historical_bankruptcies,
    
    -- Inquiries (past 12 months)
    MAX(CASE WHEN feature_name = 'CR_V2_NUMBER_OF_ALL_INQUIRIES_1Y' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS inquiries_12m,
    
    -- Bank Balance (3 month average)
    MAX(CASE WHEN feature_name = 'FI_V13_MULTIPLE_ACCOUNT_M3_AVG_BALANCE' AND rn = 1 
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END) AS bank_balance_m3_avg,
    
    -- Monthly Income (for revenue trend calculation)
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M1_AVG_MONTHLY_INCOME' AND rn = 1 
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END) AS monthly_income_m1,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_AVG_MONTHLY_INCOME' AND rn = 1 
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END) AS monthly_income_m3,
    
    -- Alt Lenders (for new debt detection)
    MAX(CASE WHEN feature_name = 'NUM_NEW_ALT_LENDERS_LAST_12_WEEKS' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS new_alt_lenders_12w,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 
        THEN feature_value::VARCHAR END) AS alt_lenders_m3,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M6_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 
        THEN feature_value::VARCHAR END) AS alt_lenders_m6,
    
    -- Days Since Approval (for FBX DOB in years)
    MAX(CASE WHEN feature_name = 'DAYS_SINCE_APPROVAL_INCLUDING_APPROVAL_OFFER' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS days_since_approval,
    
    -- Custom Plan History
    MAX(CASE WHEN feature_name = 'ACTUAL_PAYMENTS_METRICS_HAD_CUSTOM_PLAN_EVER' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS had_custom_plan_ever,
    
    -- Industry Type
    MAX(CASE WHEN feature_name = 'INDUSTRY_TYPE' AND rn = 1 
        THEN feature_value::VARCHAR END) AS industry_type,
    
    -- Broken Settlement History
    MAX(CASE WHEN feature_name = 'FP_HAS_BROKEN_SETTLEMENT' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS has_broken_settlement,
    
    -- Deferred Payment
    MAX(CASE WHEN feature_name = 'FP_HAS_DEFERRED_PAYMENT' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS has_deferred_payment,
    
    -- OG Bucket (ongoing model bucket)
    MAX(CASE WHEN feature_name = 'LAST_RELEVANT_ONGOING_MODEL_BUCKET' AND rn = 1 
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END) AS og_bucket

FROM feature_latest
GROUP BY fbbid
;


-- =====================================================
-- STEP 3: CALCULATE COMPONENT SCORES BASED ON SCORING GRID
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE scored_accounts AS
SELECT
    f.fbbid,
    d.dpd_at_snapshot,
    d.outstanding_principal_due,
    
    -- Raw Feature Values
    f.vantage_score,
    f.vantage_pct_change,
    f.accounts_in_collection,
    f.past_due_accounts,
    f.historical_bankruptcies,
    f.inquiries_12m,
    f.bank_balance_m3_avg,
    f.monthly_income_m1,
    f.monthly_income_m3,
    f.new_alt_lenders_12w,
    f.days_since_approval,
    f.had_custom_plan_ever,
    f.industry_type,
    f.has_broken_settlement,
    f.has_deferred_payment,
    f.og_bucket,
    
    -- Derived: FBX DOB in years
    ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2) AS fbx_dob_years,
    
    -- Derived: 3M Revenue Trend (% decline in income)
    CASE 
        WHEN f.monthly_income_m3 > 0 AND f.monthly_income_m1 IS NOT NULL
        THEN ROUND(((f.monthly_income_m3 - f.monthly_income_m1) / f.monthly_income_m3) * 100, 2)
        ELSE 0 
    END AS revenue_trend_pct,
    
    -- Derived: Has new debt from other lenders
    CASE 
        WHEN COALESCE(f.new_alt_lenders_12w, 0) > 0 THEN 1
        WHEN f.alt_lenders_m3 IS NOT NULL AND f.alt_lenders_m6 IS NULL THEN 1
        WHEN f.alt_lenders_m3 IS NOT NULL AND f.alt_lenders_m3 <> f.alt_lenders_m6 THEN 1
        ELSE 0 
    END AS has_new_debt_other_lenders,
    
    -- ═══════════════════════════════════════════════════
    -- COMPONENT SCORES
    -- ═══════════════════════════════════════════════════
    
    -- 1. Vantage Score
    CASE 
        WHEN f.vantage_score > 700 THEN 0
        WHEN f.vantage_score BETWEEN 600 AND 700 THEN 11.5
        WHEN f.vantage_score < 600 THEN 20
        ELSE 0  -- Default if NULL
    END AS score_vantage,
    
    -- 2. Vantage Change (% decline triggers score)
    CASE 
        WHEN f.vantage_pct_change IS NULL THEN 0
        WHEN f.vantage_pct_change >= -0.15 THEN 0           -- 0 through -15%
        WHEN f.vantage_pct_change >= -0.25 THEN 5           -- -15% through -25%
        ELSE 11.5                                            -- -25%+
    END AS score_vantage_change,
    
    -- 3. Accounts in Collection
    CASE 
        WHEN COALESCE(f.accounts_in_collection, 0) = 0 THEN 0
        WHEN f.accounts_in_collection BETWEEN 1 AND 2 THEN 1
        WHEN f.accounts_in_collection BETWEEN 3 AND 4 THEN 7.5
        ELSE 15  -- 5+
    END AS score_accounts_collection,
    
    -- 4. Past Due Accounts
    CASE 
        WHEN COALESCE(f.past_due_accounts, 0) <= 2 THEN 0
        WHEN f.past_due_accounts BETWEEN 3 AND 5 THEN 2
        ELSE 5  -- 5+
    END AS score_past_due,
    
    -- 5. Historical Bankruptcies (prior years)
    CASE 
        WHEN COALESCE(f.historical_bankruptcies, 0) = 0 THEN 0
        ELSE 7.5  -- 1+
    END AS score_bankruptcies,
    
    -- 6. Inquiries (past 12 months)
    CASE 
        WHEN COALESCE(f.inquiries_12m, 0) <= 1 THEN 0
        WHEN f.inquiries_12m BETWEEN 2 AND 4 THEN 5
        ELSE 10  -- 5+
    END AS score_inquiries,
    
    -- 7. Bank Balances (EOM)
    CASE 
        WHEN COALESCE(f.bank_balance_m3_avg, 0) > 20000 THEN 0
        WHEN f.bank_balance_m3_avg > 10000 THEN 4
        ELSE 7.5  -- < $10,000
    END AS score_bank_balance,
    
    -- 8. 3M Revenue Trends (% decline)
    CASE 
        WHEN CASE 
                WHEN f.monthly_income_m3 > 0 AND f.monthly_income_m1 IS NOT NULL
                THEN ABS((f.monthly_income_m3 - f.monthly_income_m1) / f.monthly_income_m3) * 100
                ELSE 0 
             END < 15 THEN 0
        WHEN CASE 
                WHEN f.monthly_income_m3 > 0 AND f.monthly_income_m1 IS NOT NULL
                THEN ABS((f.monthly_income_m3 - f.monthly_income_m1) / f.monthly_income_m3) * 100
                ELSE 0 
             END BETWEEN 15 AND 35 THEN 5
        ELSE 15  -- > 35%
    END AS score_revenue_trend,
    
    -- 9. Deferred Payments from Other Lenders (past 3M)
    CASE 
        WHEN COALESCE(f.has_deferred_payment, 0) = 0 THEN 0
        ELSE 4
    END AS score_deferred_payment,
    
    -- 10. New Debt from Other Lenders (past 3M)
    CASE 
        WHEN COALESCE(f.new_alt_lenders_12w, 0) = 0 
         AND (f.alt_lenders_m3 IS NULL OR f.alt_lenders_m3 = f.alt_lenders_m6) THEN 0
        ELSE 7.5
    END AS score_new_debt,
    
    -- 11. FBX DOB (years)
    CASE 
        WHEN ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2) > 3 THEN 11.5
        WHEN ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2) BETWEEN 1 AND 3 THEN 7.5
        ELSE 0  -- 0-1 years
    END AS score_fbx_dob,
    
    -- 12. Previous Custom Plan
    CASE 
        WHEN COALESCE(f.had_custom_plan_ever, 0) = 0 THEN 0
        ELSE 7.5
    END AS score_custom_plan,
    
    -- 13. Industry (transportation, restaurant, construction, trade)
    CASE 
        WHEN LOWER(COALESCE(f.industry_type, '')) IN (
            'transportation', 'restaurant', 'construction', 'trade',
            'transportation & warehousing', 'retail & wholesale trade',
            'food service', 'trucking'
        ) THEN 2.5
        ELSE 0
    END AS score_industry,
    
    -- 14. Prior Broken Settlement Agreement (with FBX)
    CASE 
        WHEN COALESCE(f.has_broken_settlement, 0) = 0 THEN 0
        ELSE 7.5
    END AS score_broken_settlement,
    
    -- 15. OG Bucket Group (negative scores for early buckets)
    CASE 
        WHEN COALESCE(f.og_bucket, 0) BETWEEN 1 AND 6 THEN -20
        WHEN f.og_bucket BETWEEN 7 AND 8 THEN -15
        WHEN f.og_bucket BETWEEN 9 AND 12 THEN 0
        WHEN f.og_bucket > 12 THEN 25
        ELSE 0  -- Default
    END AS score_og_bucket

FROM feature_store_pivoted f
INNER JOIN delinquent_accounts_jan2025 d
    ON d.fbbid = f.fbbid
;


-- =====================================================
-- STEP 4: CALCULATE TOTAL SCORE AND ASSIGN GRADE
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE graded_accounts AS
SELECT
    *,
    
    -- Total Score (sum of all component scores)
    (
        score_vantage +
        score_vantage_change +
        score_accounts_collection +
        score_past_due +
        score_bankruptcies +
        score_inquiries +
        score_bank_balance +
        score_revenue_trend +
        score_deferred_payment +
        score_new_debt +
        score_fbx_dob +
        score_custom_plan +
        score_industry +
        score_broken_settlement +
        score_og_bucket
    ) AS total_score,
    
    -- Risk Grade based on total score
    CASE 
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) >= 0 AND (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 30 THEN 'A'
        
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) > 30 AND (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 40 THEN 'B'
        
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) > 40 AND (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 55 THEN 'C'
        
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) > 55 AND (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 70 THEN 'D'
        
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) > 70 AND (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 85 THEN 'E'
        
        ELSE 'F'  -- total_score > 85
    END AS risk_grade,
    
    -- Expected CO Rate based on grade
    CASE 
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 30 THEN 0.26
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 40 THEN 0.36
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 55 THEN 0.45
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 70 THEN 0.52
        WHEN (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) <= 85 THEN 0.56
        ELSE 0.62
    END AS expected_co_rate

FROM scored_accounts
;


-- =====================================================
-- STEP 5: JOIN WITH CHARGE-OFF DATA TO TRACK OUTCOMES
-- =====================================================

CREATE OR REPLACE TABLE analytics.credit.risk_grade_chargeoff_analysis AS
WITH charge_off_events AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(outstanding_principal_due) AS co_principal
    FROM bi.finance.finance_metrics_daily
    WHERE is_charged_off = 1
      AND product_type <> 'Flexpay'
      AND charge_off_date >= '2025-01-01'  -- CO after our snapshot date
    GROUP BY fbbid::VARCHAR
)
SELECT
    -- Account Identifiers
    g.fbbid,
    g.dpd_at_snapshot AS dpd_jan2025,
    g.outstanding_principal_due AS principal_jan2025,
    
    -- Risk Score & Grade
    g.total_score,
    g.risk_grade,
    g.expected_co_rate,
    
    -- Component Scores (for analysis)
    g.score_vantage,
    g.score_vantage_change,
    g.score_accounts_collection,
    g.score_past_due,
    g.score_bankruptcies,
    g.score_inquiries,
    g.score_bank_balance,
    g.score_revenue_trend,
    g.score_deferred_payment,
    g.score_new_debt,
    g.score_fbx_dob,
    g.score_custom_plan,
    g.score_industry,
    g.score_broken_settlement,
    g.score_og_bucket,
    
    -- Raw Feature Values
    g.vantage_score,
    g.vantage_pct_change,
    g.accounts_in_collection,
    g.past_due_accounts,
    g.historical_bankruptcies,
    g.inquiries_12m,
    g.bank_balance_m3_avg,
    g.fbx_dob_years,
    g.revenue_trend_pct,
    g.had_custom_plan_ever,
    g.industry_type,
    g.has_broken_settlement,
    g.has_deferred_payment,
    g.og_bucket,
    
    -- Charge-Off Outcome
    CASE WHEN co.charge_off_date IS NOT NULL THEN 1 ELSE 0 END AS did_charge_off,
    co.charge_off_date,
    co.co_principal AS chargeoff_principal,
    
    -- Time to Charge-Off (from Jan 1, 2025)
    DATEDIFF('day', '2025-01-01', co.charge_off_date) AS days_to_chargeoff,
    DATEDIFF('month', '2025-01-01', co.charge_off_date) AS months_to_chargeoff,
    
    -- Charge-Off Month/Quarter for cohort analysis
    DATE_TRUNC('month', co.charge_off_date) AS chargeoff_month,
    DATE_TRUNC('quarter', co.charge_off_date) AS chargeoff_quarter

FROM graded_accounts g
LEFT JOIN charge_off_events co
    ON co.fbbid = g.fbbid
;


-- =====================================================
-- SUMMARY ANALYSIS QUERIES
-- =====================================================

-- Query 1: Grade Distribution with Charge-Off Rates
SELECT
    risk_grade,
    expected_co_rate AS expected_co_pct,
    COUNT(*) AS total_accounts,
    SUM(principal_jan2025) AS total_principal,
    SUM(did_charge_off) AS chargeoff_count,
    ROUND(SUM(did_charge_off) * 100.0 / COUNT(*), 2) AS actual_co_pct,
    ROUND(AVG(total_score), 2) AS avg_score,
    ROUND(AVG(days_to_chargeoff), 0) AS avg_days_to_co
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY risk_grade, expected_co_rate
ORDER BY risk_grade
;


-- Query 2: Monthly Charge-Off Tracking by Grade
SELECT
    risk_grade,
    chargeoff_month,
    COUNT(*) AS chargeoff_count,
    SUM(chargeoff_principal) AS chargeoff_principal,
    ROUND(AVG(days_to_chargeoff), 0) AS avg_days_to_co
FROM analytics.credit.risk_grade_chargeoff_analysis
WHERE did_charge_off = 1
GROUP BY risk_grade, chargeoff_month
ORDER BY risk_grade, chargeoff_month
;


-- Query 3: Score Distribution Analysis
SELECT
    CASE 
        WHEN total_score < 0 THEN '< 0'
        WHEN total_score BETWEEN 0 AND 10 THEN '0-10'
        WHEN total_score BETWEEN 10 AND 20 THEN '10-20'
        WHEN total_score BETWEEN 20 AND 30 THEN '20-30'
        WHEN total_score BETWEEN 30 AND 40 THEN '30-40'
        WHEN total_score BETWEEN 40 AND 55 THEN '40-55'
        WHEN total_score BETWEEN 55 AND 70 THEN '55-70'
        WHEN total_score BETWEEN 70 AND 85 THEN '70-85'
        ELSE '85+'
    END AS score_bucket,
    risk_grade,
    COUNT(*) AS account_count,
    SUM(did_charge_off) AS co_count,
    ROUND(SUM(did_charge_off) * 100.0 / COUNT(*), 2) AS co_rate_pct
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY 1, 2
ORDER BY 
    CASE 
        WHEN total_score < 0 THEN 1
        WHEN total_score BETWEEN 0 AND 10 THEN 2
        WHEN total_score BETWEEN 10 AND 20 THEN 3
        WHEN total_score BETWEEN 20 AND 30 THEN 4
        WHEN total_score BETWEEN 30 AND 40 THEN 5
        WHEN total_score BETWEEN 40 AND 55 THEN 6
        WHEN total_score BETWEEN 55 AND 70 THEN 7
        WHEN total_score BETWEEN 70 AND 85 THEN 8
        ELSE 9
    END
;


-- Query 4: Component Score Impact Analysis
SELECT
    'Vantage Score' AS component,
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_vantage END), 2) AS avg_score_co,
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_vantage END), 2) AS avg_score_non_co
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Vantage Change', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_vantage_change END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_vantage_change END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Accounts in Collection', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_accounts_collection END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_accounts_collection END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Past Due Accounts', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_past_due END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_past_due END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Bankruptcies', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_bankruptcies END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_bankruptcies END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Inquiries', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_inquiries END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_inquiries END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Bank Balance', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_bank_balance END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_bank_balance END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Revenue Trend', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_revenue_trend END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_revenue_trend END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Deferred Payment', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_deferred_payment END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_deferred_payment END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'New Debt', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_new_debt END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_new_debt END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'FBX DOB', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_fbx_dob END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_fbx_dob END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Custom Plan', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_custom_plan END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_custom_plan END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Industry', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_industry END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_industry END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'Broken Settlement', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_broken_settlement END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_broken_settlement END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
UNION ALL
SELECT 'OG Bucket', 
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_og_bucket END), 2),
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_og_bucket END), 2)
FROM analytics.credit.risk_grade_chargeoff_analysis
;


-- Query 5: Full Detail Export
SELECT * 
FROM analytics.credit.risk_grade_chargeoff_analysis
ORDER BY risk_grade, total_score DESC
;
