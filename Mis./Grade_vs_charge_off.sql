-- =====================================================
-- RISK SCORE & GRADING FRAMEWORK — MULTI-SNAPSHOT
-- =====================================================
-- Purpose: Generate risk scores and grades for delinquent accounts
--          using feature store data and track charge-off outcomes
--
-- Source Table: DATA_SCIENCE.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_ALL_FEATURES
-- Snapshots:    Delinquent non-charged-off accounts as of:
--               Jan 1, 2025 | Feb 1, 2025 | Mar 1, 2025
--
-- Feature Lookup Window: Latest feature value within 15 days PRIOR to each snapshot date
-- Output:  One row per snapshot per account
-- =====================================================

-- Set database context
USE DATABASE ANALYTICS;
USE SCHEMA CREDIT;


-- =====================================================
-- STEP 1: IDENTIFY DELINQUENT NON-CHARGED-OFF ACCOUNTS
--         FOR EACH OF THE THREE SNAPSHOT DATES
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE delinquent_accounts_snapshots AS

SELECT DISTINCT
    fbbid::VARCHAR                      AS fbbid,
    '2025-01-01'::DATE                  AS snapshot_date,
    MAX(dpd_days)::INT                  AS dpd_at_snapshot,
    SUM(outstanding_principal_due)      AS outstanding_principal_due,
    MAX(is_charged_off)                 AS is_charged_off
FROM bi.finance.finance_metrics_daily
WHERE edate = '2025-01-01'
  AND product_type <> 'Flexpay'
  AND dpd_days::INT > 0
  AND is_charged_off = 0
GROUP BY fbbid

UNION ALL

SELECT DISTINCT
    fbbid::VARCHAR                      AS fbbid,
    '2025-02-01'::DATE                  AS snapshot_date,
    MAX(dpd_days)::INT                  AS dpd_at_snapshot,
    SUM(outstanding_principal_due)      AS outstanding_principal_due,
    MAX(is_charged_off)                 AS is_charged_off
FROM bi.finance.finance_metrics_daily
WHERE edate = '2025-02-01'
  AND product_type <> 'Flexpay'
  AND dpd_days::INT > 0
  AND is_charged_off = 0
GROUP BY fbbid

UNION ALL

SELECT DISTINCT
    fbbid::VARCHAR                      AS fbbid,
    '2025-03-01'::DATE                  AS snapshot_date,
    MAX(dpd_days)::INT                  AS dpd_at_snapshot,
    SUM(outstanding_principal_due)      AS outstanding_principal_due,
    MAX(is_charged_off)                 AS is_charged_off
FROM bi.finance.finance_metrics_daily
WHERE edate = '2025-03-01'
  AND product_type <> 'Flexpay'
  AND dpd_days::INT > 0
  AND is_charged_off = 0
GROUP BY fbbid
;


-- =====================================================
-- STEP 2: PIVOT FEATURE STORE TO WIDE FORMAT
--         Using latest feature value within 60-day window
--         Preference: 15-day data preferred, fallback to 60-day
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE feature_store_pivoted AS
WITH

-- Define lookup windows: 15-day, 60-day, and max 365-day for performance
account_windows AS (
    SELECT DISTINCT
        fbbid,
        snapshot_date,
        DATEADD('day', -15, snapshot_date)  AS window_15d_start,   -- 15-day window
        DATEADD('day', -60, snapshot_date)  AS window_60d_start,   -- 60-day window
        DATEADD('day', -365, snapshot_date) AS window_max_start,   -- Max lookback for performance
        snapshot_date                        AS window_end
    FROM delinquent_accounts_snapshots
),

-- Rank feature values with three separate rankings for different windows
feature_ranked AS (
    SELECT
        aw.fbbid,
        aw.snapshot_date,
        UPPER(fs.feature_name)              AS feature_name,
        fs.feature_value,
        fs.cutoff_time,
        
        -- Freshness Classification Flags
        -- Flag: 1 if within 15-day window (0-15 days before snapshot)
        CASE WHEN fs.cutoff_time >= aw.window_15d_start THEN 1 ELSE 0 END AS is_within_15d,
        -- Flag: 1 if within 60-day window (0-60 days before snapshot)
        CASE WHEN fs.cutoff_time >= aw.window_60d_start THEN 1 ELSE 0 END AS is_within_60d,
        
        -- Single ranking: Get latest feature value per feature
        ROW_NUMBER() OVER (
            PARTITION BY aw.fbbid, aw.snapshot_date, UPPER(fs.feature_name)
            ORDER BY fs.cutoff_time DESC
        ) AS rn
        
    FROM account_windows aw
    LEFT JOIN DATA_SCIENCE.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_ALL_FEATURES fs
        ON  fs.fbbid::VARCHAR = aw.fbbid
        AND fs.cutoff_time   >= aw.window_max_start   -- Max 365-day lookback for performance
        AND fs.cutoff_time   <= aw.window_end
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY aw.fbbid, aw.snapshot_date, UPPER(fs.feature_name) 
        ORDER BY fs.cutoff_time DESC
    ) = 1    -- Keep only the latest row per feature
)

SELECT
    fbbid,
    snapshot_date,
    
    -- Cutoff times for each window
    MAX(CASE WHEN rn = 1 AND is_within_15d = 1 THEN cutoff_time END)               AS cutoff_time_15d,
    MAX(CASE WHEN rn = 1 AND is_within_60d = 1 THEN cutoff_time END)               AS cutoff_time_60d,
    MAX(CASE WHEN rn = 1 THEN cutoff_time END)                                      AS cutoff_time_latest,
    
    -- Data Availability Flags
    MAX(CASE WHEN rn = 1 AND is_within_15d = 1 AND feature_value IS NOT NULL THEN 1 ELSE 0 END)  AS has_15d_data,
    MAX(CASE WHEN rn = 1 AND is_within_60d = 1 AND feature_value IS NOT NULL THEN 1 ELSE 0 END)  AS has_60d_data,
    MAX(CASE WHEN rn = 1 AND feature_value IS NOT NULL THEN 1 ELSE 0 END)                        AS has_feature_data,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- FEATURES FROM 15-DAY WINDOW (0-15 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    MAX(CASE WHEN feature_name = 'CR_V2_VANTAGE_UPGRADED' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS vantage_score_15d,
    MAX(CASE WHEN feature_name = 'FP_PCT_CHANGE_VANTAGE_SCORE_BEFORE_DPD_TO_CURRENT' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS vantage_pct_change_15d,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_2Y_NUM_COLLECTION' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS accounts_in_collection_15d,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_90D_NUM_PAST_DUES' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS past_due_accounts_15d,
    MAX(CASE WHEN feature_name = 'CR_V1_PR_5Y_NUM_BANKRUPTCIES' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS historical_bankruptcies_15d,
    MAX(CASE WHEN feature_name = 'CR_V2_NUMBER_OF_ALL_INQUIRIES_1Y' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS inquiries_12m_15d,
    MAX(CASE WHEN feature_name = 'FI_V13_MULTIPLE_ACCOUNT_M3_AVG_BALANCE' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS bank_balance_m3_avg_15d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M1_AVG_MONTHLY_INCOME' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m1_15d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_AVG_MONTHLY_INCOME' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m3_15d,
    MAX(CASE WHEN feature_name = 'NUM_NEW_ALT_LENDERS_LAST_12_WEEKS' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS new_alt_lenders_12w_15d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 AND is_within_15d = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m3_15d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M6_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 AND is_within_15d = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m6_15d,
    MAX(CASE WHEN feature_name = 'DAYS_SINCE_APPROVAL_INCLUDING_APPROVAL_OFFER' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS days_since_approval_15d,
    MAX(CASE WHEN feature_name = 'ACTUAL_PAYMENTS_METRICS_HAD_CUSTOM_PLAN_EVER' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS had_custom_plan_ever_15d,
    MAX(CASE WHEN feature_name = 'INDUSTRY_TYPE' AND rn = 1 AND is_within_15d = 1
        THEN feature_value::VARCHAR END)                                             AS industry_type_15d,
    MAX(CASE WHEN feature_name = 'FP_HAS_BROKEN_SETTLEMENT' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_broken_settlement_15d,
    MAX(CASE WHEN feature_name = 'FP_HAS_DEFERRED_PAYMENT' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_deferred_payment_15d,
    MAX(CASE WHEN feature_name = 'LAST_RELEVANT_ONGOING_MODEL_BUCKET' AND rn = 1 AND is_within_15d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS og_bucket_15d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- FEATURES FROM 60-DAY WINDOW (0-60 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    MAX(CASE WHEN feature_name = 'CR_V2_VANTAGE_UPGRADED' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS vantage_score_60d,
    MAX(CASE WHEN feature_name = 'FP_PCT_CHANGE_VANTAGE_SCORE_BEFORE_DPD_TO_CURRENT' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS vantage_pct_change_60d,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_2Y_NUM_COLLECTION' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS accounts_in_collection_60d,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_90D_NUM_PAST_DUES' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS past_due_accounts_60d,
    MAX(CASE WHEN feature_name = 'CR_V1_PR_5Y_NUM_BANKRUPTCIES' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS historical_bankruptcies_60d,
    MAX(CASE WHEN feature_name = 'CR_V2_NUMBER_OF_ALL_INQUIRIES_1Y' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS inquiries_12m_60d,
    MAX(CASE WHEN feature_name = 'FI_V13_MULTIPLE_ACCOUNT_M3_AVG_BALANCE' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS bank_balance_m3_avg_60d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M1_AVG_MONTHLY_INCOME' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m1_60d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_AVG_MONTHLY_INCOME' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m3_60d,
    MAX(CASE WHEN feature_name = 'NUM_NEW_ALT_LENDERS_LAST_12_WEEKS' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS new_alt_lenders_12w_60d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 AND is_within_60d = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m3_60d,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M6_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1 AND is_within_60d = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m6_60d,
    MAX(CASE WHEN feature_name = 'DAYS_SINCE_APPROVAL_INCLUDING_APPROVAL_OFFER' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS days_since_approval_60d,
    MAX(CASE WHEN feature_name = 'ACTUAL_PAYMENTS_METRICS_HAD_CUSTOM_PLAN_EVER' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS had_custom_plan_ever_60d,
    MAX(CASE WHEN feature_name = 'INDUSTRY_TYPE' AND rn = 1 AND is_within_60d = 1
        THEN feature_value::VARCHAR END)                                             AS industry_type_60d,
    MAX(CASE WHEN feature_name = 'FP_HAS_BROKEN_SETTLEMENT' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_broken_settlement_60d,
    MAX(CASE WHEN feature_name = 'FP_HAS_DEFERRED_PAYMENT' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_deferred_payment_60d,
    MAX(CASE WHEN feature_name = 'LAST_RELEVANT_ONGOING_MODEL_BUCKET' AND rn = 1 AND is_within_60d = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS og_bucket_60d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- FEATURES FROM LATEST AVAILABLE (no time limit)
    -- ═══════════════════════════════════════════════════════════════════════════
    MAX(CASE WHEN feature_name = 'CR_V2_VANTAGE_UPGRADED' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS vantage_score,
    MAX(CASE WHEN feature_name = 'FP_PCT_CHANGE_VANTAGE_SCORE_BEFORE_DPD_TO_CURRENT' AND rn = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS vantage_pct_change,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_2Y_NUM_COLLECTION' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS accounts_in_collection,
    MAX(CASE WHEN feature_name = 'CR_V1_TL_90D_NUM_PAST_DUES' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS past_due_accounts,
    MAX(CASE WHEN feature_name = 'CR_V1_PR_5Y_NUM_BANKRUPTCIES' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS historical_bankruptcies,
    MAX(CASE WHEN feature_name = 'CR_V2_NUMBER_OF_ALL_INQUIRIES_1Y' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS inquiries_12m,
    MAX(CASE WHEN feature_name = 'FI_V13_MULTIPLE_ACCOUNT_M3_AVG_BALANCE' AND rn = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS bank_balance_m3_avg,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M1_AVG_MONTHLY_INCOME' AND rn = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m1,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_AVG_MONTHLY_INCOME' AND rn = 1
        THEN TRY_TO_DOUBLE(feature_value::VARCHAR) END)                             AS monthly_income_m3,
    MAX(CASE WHEN feature_name = 'NUM_NEW_ALT_LENDERS_LAST_12_WEEKS' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS new_alt_lenders_12w,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M3_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m3,
    MAX(CASE WHEN feature_name = 'FI_V12_MULTIPLE_ACCOUNT_M6_NAMES_OF_ALTERNATIVE_LENDERS' AND rn = 1
        THEN feature_value::VARCHAR END)                                             AS alt_lenders_m6,
    MAX(CASE WHEN feature_name = 'DAYS_SINCE_APPROVAL_INCLUDING_APPROVAL_OFFER' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS days_since_approval,
    MAX(CASE WHEN feature_name = 'ACTUAL_PAYMENTS_METRICS_HAD_CUSTOM_PLAN_EVER' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS had_custom_plan_ever,
    MAX(CASE WHEN feature_name = 'INDUSTRY_TYPE' AND rn = 1
        THEN feature_value::VARCHAR END)                                             AS industry_type,
    MAX(CASE WHEN feature_name = 'FP_HAS_BROKEN_SETTLEMENT' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_broken_settlement,
    MAX(CASE WHEN feature_name = 'FP_HAS_DEFERRED_PAYMENT' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS has_deferred_payment,
    MAX(CASE WHEN feature_name = 'LAST_RELEVANT_ONGOING_MODEL_BUCKET' AND rn = 1
        THEN TRY_TO_NUMBER(feature_value::VARCHAR) END)                             AS og_bucket

FROM feature_ranked
GROUP BY fbbid, snapshot_date
;


-- =====================================================
-- STEP 3: CALCULATE COMPONENT SCORES FOR ALL THREE WINDOWS
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE scored_accounts AS
SELECT
    d.fbbid,
    d.snapshot_date,
    d.dpd_at_snapshot,
    d.outstanding_principal_due,

    -- Cutoff Times
    f.cutoff_time_15d,
    f.cutoff_time_60d,
    f.cutoff_time_latest,

    -- Data Availability Flags
    COALESCE(f.has_15d_data, 0)                                                    AS has_15d_data,
    COALESCE(f.has_60d_data, 0)                                                    AS has_60d_data,
    COALESCE(f.has_feature_data, 0)                                                AS has_feature_data,

    -- Raw Feature Values (Latest - for reference)
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
    f.alt_lenders_m3,
    f.alt_lenders_m6,
    f.days_since_approval,
    f.had_custom_plan_ever,
    f.industry_type,
    f.has_broken_settlement,
    f.has_deferred_payment,
    f.og_bucket,

    -- Derived: FBX DOB in years (Latest)
    ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2)                           AS fbx_dob_years,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- COMPONENT SCORES - 15 DAY WINDOW
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE WHEN f.vantage_score_15d > 700 THEN 0 WHEN f.vantage_score_15d BETWEEN 600 AND 700 THEN 11.5 WHEN f.vantage_score_15d < 600 THEN 20 ELSE 0 END AS score_vantage_15d,
    CASE WHEN f.vantage_pct_change_15d IS NULL THEN 0 WHEN f.vantage_pct_change_15d >= -0.15 THEN 0 WHEN f.vantage_pct_change_15d >= -0.25 THEN 5 ELSE 11.5 END AS score_vantage_change_15d,
    CASE WHEN COALESCE(f.accounts_in_collection_15d, 0) = 0 THEN 0 WHEN f.accounts_in_collection_15d BETWEEN 1 AND 2 THEN 1 WHEN f.accounts_in_collection_15d BETWEEN 3 AND 4 THEN 7.5 ELSE 15 END AS score_accounts_collection_15d,
    CASE WHEN COALESCE(f.past_due_accounts_15d, 0) <= 2 THEN 0 WHEN f.past_due_accounts_15d BETWEEN 3 AND 5 THEN 2 ELSE 5 END AS score_past_due_15d,
    CASE WHEN COALESCE(f.historical_bankruptcies_15d, 0) = 0 THEN 0 ELSE 7.5 END AS score_bankruptcies_15d,
    CASE WHEN COALESCE(f.inquiries_12m_15d, 0) <= 1 THEN 0 WHEN f.inquiries_12m_15d BETWEEN 2 AND 4 THEN 5 ELSE 10 END AS score_inquiries_15d,
    CASE WHEN COALESCE(f.bank_balance_m3_avg_15d, 0) > 20000 THEN 0 WHEN f.bank_balance_m3_avg_15d > 10000 THEN 4 ELSE 7.5 END AS score_bank_balance_15d,
    CASE WHEN CASE WHEN f.monthly_income_m3_15d > 0 AND f.monthly_income_m1_15d IS NOT NULL THEN ABS((f.monthly_income_m3_15d - f.monthly_income_m1_15d) / f.monthly_income_m3_15d) * 100 ELSE 0 END < 15 THEN 0
         WHEN CASE WHEN f.monthly_income_m3_15d > 0 AND f.monthly_income_m1_15d IS NOT NULL THEN ABS((f.monthly_income_m3_15d - f.monthly_income_m1_15d) / f.monthly_income_m3_15d) * 100 ELSE 0 END BETWEEN 15 AND 35 THEN 5 ELSE 15 END AS score_revenue_trend_15d,
    CASE WHEN COALESCE(f.has_deferred_payment_15d, 0) = 0 THEN 0 ELSE 4 END AS score_deferred_payment_15d,
    CASE WHEN COALESCE(f.new_alt_lenders_12w_15d, 0) = 0 AND (f.alt_lenders_m3_15d IS NULL OR f.alt_lenders_m3_15d = f.alt_lenders_m6_15d) THEN 0 ELSE 7.5 END AS score_new_debt_15d,
    CASE WHEN ROUND(COALESCE(f.days_since_approval_15d, 0) / 365.0, 2) > 3 THEN 11.5 WHEN ROUND(COALESCE(f.days_since_approval_15d, 0) / 365.0, 2) BETWEEN 1 AND 3 THEN 7.5 ELSE 0 END AS score_fbx_dob_15d,
    CASE WHEN COALESCE(f.had_custom_plan_ever_15d, 0) = 0 THEN 0 ELSE 7.5 END AS score_custom_plan_15d,
    CASE WHEN LOWER(COALESCE(f.industry_type_15d, '')) IN ('transportation', 'restaurant', 'construction', 'trade', 'transportation & warehousing', 'retail & wholesale trade', 'food service', 'trucking') THEN 2.5 ELSE 0 END AS score_industry_15d,
    CASE WHEN COALESCE(f.has_broken_settlement_15d, 0) = 0 THEN 0 ELSE 7.5 END AS score_broken_settlement_15d,
    CASE WHEN COALESCE(f.og_bucket_15d, 0) BETWEEN 1 AND 6 THEN -20 WHEN f.og_bucket_15d BETWEEN 7 AND 8 THEN -15 WHEN f.og_bucket_15d BETWEEN 9 AND 12 THEN 0 WHEN f.og_bucket_15d > 12 THEN 25 ELSE 0 END AS score_og_bucket_15d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- COMPONENT SCORES - 60 DAY WINDOW (0-60 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE WHEN f.vantage_score_60d > 700 THEN 0 WHEN f.vantage_score_60d BETWEEN 600 AND 700 THEN 11.5 WHEN f.vantage_score_60d < 600 THEN 20 ELSE 0 END AS score_vantage_60d,
    CASE WHEN f.vantage_pct_change_60d IS NULL THEN 0 WHEN f.vantage_pct_change_60d >= -0.15 THEN 0 WHEN f.vantage_pct_change_60d >= -0.25 THEN 5 ELSE 11.5 END AS score_vantage_change_60d,
    CASE WHEN COALESCE(f.accounts_in_collection_60d, 0) = 0 THEN 0 WHEN f.accounts_in_collection_60d BETWEEN 1 AND 2 THEN 1 WHEN f.accounts_in_collection_60d BETWEEN 3 AND 4 THEN 7.5 ELSE 15 END AS score_accounts_collection_60d,
    CASE WHEN COALESCE(f.past_due_accounts_60d, 0) <= 2 THEN 0 WHEN f.past_due_accounts_60d BETWEEN 3 AND 5 THEN 2 ELSE 5 END AS score_past_due_60d,
    CASE WHEN COALESCE(f.historical_bankruptcies_60d, 0) = 0 THEN 0 ELSE 7.5 END AS score_bankruptcies_60d,
    CASE WHEN COALESCE(f.inquiries_12m_60d, 0) <= 1 THEN 0 WHEN f.inquiries_12m_60d BETWEEN 2 AND 4 THEN 5 ELSE 10 END AS score_inquiries_60d,
    CASE WHEN COALESCE(f.bank_balance_m3_avg_60d, 0) > 20000 THEN 0 WHEN f.bank_balance_m3_avg_60d > 10000 THEN 4 ELSE 7.5 END AS score_bank_balance_60d,
    CASE WHEN CASE WHEN f.monthly_income_m3_60d > 0 AND f.monthly_income_m1_60d IS NOT NULL THEN ABS((f.monthly_income_m3_60d - f.monthly_income_m1_60d) / f.monthly_income_m3_60d) * 100 ELSE 0 END < 15 THEN 0
         WHEN CASE WHEN f.monthly_income_m3_60d > 0 AND f.monthly_income_m1_60d IS NOT NULL THEN ABS((f.monthly_income_m3_60d - f.monthly_income_m1_60d) / f.monthly_income_m3_60d) * 100 ELSE 0 END BETWEEN 15 AND 35 THEN 5 ELSE 15 END AS score_revenue_trend_60d,
    CASE WHEN COALESCE(f.has_deferred_payment_60d, 0) = 0 THEN 0 ELSE 4 END AS score_deferred_payment_60d,
    CASE WHEN COALESCE(f.new_alt_lenders_12w_60d, 0) = 0 AND (f.alt_lenders_m3_60d IS NULL OR f.alt_lenders_m3_60d = f.alt_lenders_m6_60d) THEN 0 ELSE 7.5 END AS score_new_debt_60d,
    CASE WHEN ROUND(COALESCE(f.days_since_approval_60d, 0) / 365.0, 2) > 3 THEN 11.5 WHEN ROUND(COALESCE(f.days_since_approval_60d, 0) / 365.0, 2) BETWEEN 1 AND 3 THEN 7.5 ELSE 0 END AS score_fbx_dob_60d,
    CASE WHEN COALESCE(f.had_custom_plan_ever_60d, 0) = 0 THEN 0 ELSE 7.5 END AS score_custom_plan_60d,
    CASE WHEN LOWER(COALESCE(f.industry_type_60d, '')) IN ('transportation', 'restaurant', 'construction', 'trade', 'transportation & warehousing', 'retail & wholesale trade', 'food service', 'trucking') THEN 2.5 ELSE 0 END AS score_industry_60d,
    CASE WHEN COALESCE(f.has_broken_settlement_60d, 0) = 0 THEN 0 ELSE 7.5 END AS score_broken_settlement_60d,
    CASE WHEN COALESCE(f.og_bucket_60d, 0) BETWEEN 1 AND 6 THEN -20 WHEN f.og_bucket_60d BETWEEN 7 AND 8 THEN -15 WHEN f.og_bucket_60d BETWEEN 9 AND 12 THEN 0 WHEN f.og_bucket_60d > 12 THEN 25 ELSE 0 END AS score_og_bucket_60d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- COMPONENT SCORES - LATEST AVAILABLE (15d preferred, fallback to 16-60d)
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE WHEN f.vantage_score > 700 THEN 0 WHEN f.vantage_score BETWEEN 600 AND 700 THEN 11.5 WHEN f.vantage_score < 600 THEN 20 ELSE 0 END AS score_vantage,
    CASE WHEN f.vantage_pct_change IS NULL THEN 0 WHEN f.vantage_pct_change >= -0.15 THEN 0 WHEN f.vantage_pct_change >= -0.25 THEN 5 ELSE 11.5 END AS score_vantage_change,
    CASE WHEN COALESCE(f.accounts_in_collection, 0) = 0 THEN 0 WHEN f.accounts_in_collection BETWEEN 1 AND 2 THEN 1 WHEN f.accounts_in_collection BETWEEN 3 AND 4 THEN 7.5 ELSE 15 END AS score_accounts_collection,
    CASE WHEN COALESCE(f.past_due_accounts, 0) <= 2 THEN 0 WHEN f.past_due_accounts BETWEEN 3 AND 5 THEN 2 ELSE 5 END AS score_past_due,
    CASE WHEN COALESCE(f.historical_bankruptcies, 0) = 0 THEN 0 ELSE 7.5 END AS score_bankruptcies,
    CASE WHEN COALESCE(f.inquiries_12m, 0) <= 1 THEN 0 WHEN f.inquiries_12m BETWEEN 2 AND 4 THEN 5 ELSE 10 END AS score_inquiries,
    CASE WHEN COALESCE(f.bank_balance_m3_avg, 0) > 20000 THEN 0 WHEN f.bank_balance_m3_avg > 10000 THEN 4 ELSE 7.5 END AS score_bank_balance,
    CASE WHEN CASE WHEN f.monthly_income_m3 > 0 AND f.monthly_income_m1 IS NOT NULL THEN ABS((f.monthly_income_m3 - f.monthly_income_m1) / f.monthly_income_m3) * 100 ELSE 0 END < 15 THEN 0
         WHEN CASE WHEN f.monthly_income_m3 > 0 AND f.monthly_income_m1 IS NOT NULL THEN ABS((f.monthly_income_m3 - f.monthly_income_m1) / f.monthly_income_m3) * 100 ELSE 0 END BETWEEN 15 AND 35 THEN 5 ELSE 15 END AS score_revenue_trend,
    CASE WHEN COALESCE(f.has_deferred_payment, 0) = 0 THEN 0 ELSE 4 END AS score_deferred_payment,
    CASE WHEN COALESCE(f.new_alt_lenders_12w, 0) = 0 AND (f.alt_lenders_m3 IS NULL OR f.alt_lenders_m3 = f.alt_lenders_m6) THEN 0 ELSE 7.5 END AS score_new_debt,
    CASE WHEN ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2) > 3 THEN 11.5 WHEN ROUND(COALESCE(f.days_since_approval, 0) / 365.0, 2) BETWEEN 1 AND 3 THEN 7.5 ELSE 0 END AS score_fbx_dob,
    CASE WHEN COALESCE(f.had_custom_plan_ever, 0) = 0 THEN 0 ELSE 7.5 END AS score_custom_plan,
    CASE WHEN LOWER(COALESCE(f.industry_type, '')) IN ('transportation', 'restaurant', 'construction', 'trade', 'transportation & warehousing', 'retail & wholesale trade', 'food service', 'trucking') THEN 2.5 ELSE 0 END AS score_industry,
    CASE WHEN COALESCE(f.has_broken_settlement, 0) = 0 THEN 0 ELSE 7.5 END AS score_broken_settlement,
    CASE WHEN COALESCE(f.og_bucket, 0) BETWEEN 1 AND 6 THEN -20 WHEN f.og_bucket BETWEEN 7 AND 8 THEN -15 WHEN f.og_bucket BETWEEN 9 AND 12 THEN 0 WHEN f.og_bucket > 12 THEN 25 ELSE 0 END AS score_og_bucket

FROM delinquent_accounts_snapshots d
LEFT JOIN feature_store_pivoted f
    ON  f.fbbid         = d.fbbid
    AND f.snapshot_date = d.snapshot_date
;


-- =====================================================
-- STEP 4: CALCULATE TOTAL SCORES AND ASSIGN THREE GRADES
-- =====================================================

CREATE OR REPLACE TEMPORARY TABLE graded_accounts AS
WITH scored AS (
    SELECT
        *,
        -- Total Score from 15-day window only
        (
            score_vantage_15d + score_vantage_change_15d + score_accounts_collection_15d +
            score_past_due_15d + score_bankruptcies_15d + score_inquiries_15d +
            score_bank_balance_15d + score_revenue_trend_15d + score_deferred_payment_15d +
            score_new_debt_15d + score_fbx_dob_15d + score_custom_plan_15d +
            score_industry_15d + score_broken_settlement_15d + score_og_bucket_15d
        ) AS total_score_15d,
        
        -- Total Score from 60-day window (0-60 days)
        (
            score_vantage_60d + score_vantage_change_60d + score_accounts_collection_60d +
            score_past_due_60d + score_bankruptcies_60d + score_inquiries_60d +
            score_bank_balance_60d + score_revenue_trend_60d + score_deferred_payment_60d +
            score_new_debt_60d + score_fbx_dob_60d + score_custom_plan_60d +
            score_industry_60d + score_broken_settlement_60d + score_og_bucket_60d
        ) AS total_score_60d,
        
        -- Total Score from Latest available (15d preferred, fallback to 16-60d)
        (
            score_vantage + score_vantage_change + score_accounts_collection +
            score_past_due + score_bankruptcies + score_inquiries +
            score_bank_balance + score_revenue_trend + score_deferred_payment +
            score_new_debt + score_fbx_dob + score_custom_plan +
            score_industry + score_broken_settlement + score_og_bucket
        ) AS total_score_latest
    FROM scored_accounts
)
SELECT
    *,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- RISK GRADE 1: 15-DAY WINDOW ONLY
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE
        WHEN has_15d_data = 0                              THEN 'NG'  -- No 15-day data available
        WHEN total_score_15d >= 0  AND total_score_15d <= 30 THEN 'A'
        WHEN total_score_15d > 30  AND total_score_15d <= 40 THEN 'B'
        WHEN total_score_15d > 40  AND total_score_15d <= 55 THEN 'C'
        WHEN total_score_15d > 55  AND total_score_15d <= 70 THEN 'D'
        WHEN total_score_15d > 70  AND total_score_15d <= 85 THEN 'E'
        ELSE 'F'
    END AS risk_grade_15d,
    
    CASE
        WHEN has_15d_data = 0      THEN NULL
        WHEN total_score_15d <= 30 THEN 0.26
        WHEN total_score_15d <= 40 THEN 0.36
        WHEN total_score_15d <= 55 THEN 0.45
        WHEN total_score_15d <= 70 THEN 0.52
        WHEN total_score_15d <= 85 THEN 0.56
        ELSE 0.62
    END AS expected_co_rate_15d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- RISK GRADE 2: 60-DAY WINDOW (0-60 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE
        WHEN has_60d_data = 0                                THEN 'NG'  -- No 60-day data available
        WHEN total_score_60d >= 0  AND total_score_60d <= 30 THEN 'A'
        WHEN total_score_60d > 30  AND total_score_60d <= 40 THEN 'B'
        WHEN total_score_60d > 40  AND total_score_60d <= 55 THEN 'C'
        WHEN total_score_60d > 55  AND total_score_60d <= 70 THEN 'D'
        WHEN total_score_60d > 70  AND total_score_60d <= 85 THEN 'E'
        ELSE 'F'
    END AS risk_grade_60d,
    
    CASE
        WHEN has_60d_data = 0      THEN NULL
        WHEN total_score_60d <= 30 THEN 0.26
        WHEN total_score_60d <= 40 THEN 0.36
        WHEN total_score_60d <= 55 THEN 0.45
        WHEN total_score_60d <= 70 THEN 0.52
        WHEN total_score_60d <= 85 THEN 0.56
        ELSE 0.62
    END AS expected_co_rate_60d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- RISK GRADE 3: LATEST AVAILABLE (15d preferred, fallback to 16-60d)
    -- ═══════════════════════════════════════════════════════════════════════════
    CASE
        WHEN has_feature_data = 0                                THEN 'NG'  -- No feature data at all
        WHEN total_score_latest >= 0  AND total_score_latest <= 30 THEN 'A'
        WHEN total_score_latest > 30  AND total_score_latest <= 40 THEN 'B'
        WHEN total_score_latest > 40  AND total_score_latest <= 55 THEN 'C'
        WHEN total_score_latest > 55  AND total_score_latest <= 70 THEN 'D'
        WHEN total_score_latest > 70  AND total_score_latest <= 85 THEN 'E'
        ELSE 'F'
    END AS risk_grade_latest,
    
    CASE
        WHEN has_feature_data = 0      THEN NULL
        WHEN total_score_latest <= 30 THEN 0.26
        WHEN total_score_latest <= 40 THEN 0.36
        WHEN total_score_latest <= 55 THEN 0.45
        WHEN total_score_latest <= 70 THEN 0.52
        WHEN total_score_latest <= 85 THEN 0.56
        ELSE 0.62
    END AS expected_co_rate_latest

FROM scored
;


-- =====================================================
-- STEP 5: JOIN WITH CHARGE-OFF DATA TO TRACK OUTCOMES
--         Charge-offs must occur ON OR AFTER the snapshot date
-- =====================================================

CREATE OR REPLACE TABLE analytics.credit.risk_grade_chargeoff_analysis AS
WITH charge_off_moments AS (
    SELECT
        fbbid::VARCHAR  AS fbbid,
        charge_off_date,
        outstanding_principal_due,
        ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate ASC) AS entry_num
    FROM bi.finance.finance_metrics_daily
    WHERE is_charged_off    = 1
      AND product_type      <> 'Flexpay'
      AND charge_off_date   >= '2025-01-01'
),
charge_off_events AS (
    SELECT
        fbbid,
        charge_off_date,
        outstanding_principal_due AS co_principal
    FROM charge_off_moments
    WHERE entry_num = 1
)
SELECT
    -- Snapshot Identifiers
    g.fbbid,
    g.snapshot_date,
    g.dpd_at_snapshot,
    g.outstanding_principal_due                             AS principal_at_snapshot,

    -- Cutoff Times for each window
    g.cutoff_time_15d,
    g.cutoff_time_60d,
    g.cutoff_time_latest,

    -- Data Availability Flags
    g.has_15d_data,                                         -- 1 = Has data within 15 days
    g.has_60d_data,                                         -- 1 = Has data within 60 days
    g.has_feature_data,                                     -- 1 = Has any feature data

    -- ═══════════════════════════════════════════════════════════════════════════
    -- GRADE 1: 15-DAY WINDOW (0-15 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    g.total_score_15d,
    g.risk_grade_15d,
    g.expected_co_rate_15d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- GRADE 2: 60-DAY WINDOW (0-60 days)
    -- ═══════════════════════════════════════════════════════════════════════════
    g.total_score_60d,
    g.risk_grade_60d,
    g.expected_co_rate_60d,

    -- ═══════════════════════════════════════════════════════════════════════════
    -- GRADE 3: LATEST AVAILABLE (15d preferred, fallback to 16-60d)
    -- ═══════════════════════════════════════════════════════════════════════════
    g.total_score_latest,
    g.risk_grade_latest,
    g.expected_co_rate_latest,

    -- Component Scores (Latest)
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

    -- Raw Feature Values (Latest)
    g.vantage_score,
    g.vantage_pct_change,
    g.accounts_in_collection,
    g.past_due_accounts,
    g.historical_bankruptcies,
    g.inquiries_12m,
    g.bank_balance_m3_avg,
    g.fbx_dob_years,
    g.had_custom_plan_ever,
    g.industry_type,
    g.has_broken_settlement,
    g.has_deferred_payment,
    g.og_bucket,

    -- Charge-Off Outcome
    -- Only count charge-offs that happened AFTER this snapshot date
    CASE WHEN co.charge_off_date >= g.snapshot_date THEN 1 ELSE 0 END  AS did_charge_off,
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN co.charge_off_date END                                    AS charge_off_date,
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN co.co_principal END                                        AS chargeoff_principal,

    -- Time to Charge-Off (from snapshot date)
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN DATEDIFF('day',   g.snapshot_date, co.charge_off_date) END AS days_to_chargeoff,
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN DATEDIFF('month', g.snapshot_date, co.charge_off_date) END AS months_to_chargeoff,

    -- Charge-Off Month/Quarter for cohort analysis
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN DATE_TRUNC('month',   co.charge_off_date) END              AS chargeoff_month,
    CASE WHEN co.charge_off_date >= g.snapshot_date
         THEN DATE_TRUNC('quarter', co.charge_off_date) END              AS chargeoff_quarter

FROM graded_accounts g
LEFT JOIN charge_off_events co
    ON co.fbbid = g.fbbid
;


-- =====================================================
-- VALIDATION: CHECK FOR DUPLICATE KEYS
-- =====================================================
-- Each (fbbid, snapshot_date) should be unique in the final table

SELECT fbbid, snapshot_date, COUNT(*) AS cnt
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY fbbid, snapshot_date
HAVING COUNT(*) > 1
ORDER BY fbbid, snapshot_date
;

-- Check for accounts with no feature data in any window
SELECT * 
FROM analytics.credit.risk_grade_chargeoff_analysis
WHERE has_feature_data = 0;
-- =====================================================
-- SUMMARY ANALYSIS QUERIES
-- =====================================================

-- Query 1a: Grade Distribution — 15-DAY WINDOW
SELECT
    snapshot_date,
    risk_grade_15d                                                  AS risk_grade,
    '15d'                                                           AS data_window,
    expected_co_rate_15d                                            AS expected_co_pct,
    COUNT(*)                                                        AS total_accounts,
    SUM(principal_at_snapshot)                                      AS total_principal,
    SUM(did_charge_off)                                             AS chargeoff_count,
    ROUND(SUM(did_charge_off) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS actual_co_pct,
    ROUND(AVG(total_score_15d), 2)                                  AS avg_score,
    ROUND(AVG(days_to_chargeoff), 0)                                AS avg_days_to_co
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY snapshot_date, risk_grade_15d, expected_co_rate_15d
ORDER BY snapshot_date, risk_grade_15d
;

-- Query 1b: Grade Distribution — 60-DAY WINDOW (0-60 days)
SELECT
    snapshot_date,
    risk_grade_60d                                                  AS risk_grade,
    '60d'                                                           AS data_window,
    expected_co_rate_60d                                            AS expected_co_pct,
    COUNT(*)                                                        AS total_accounts,
    SUM(principal_at_snapshot)                                      AS total_principal,
    SUM(did_charge_off)                                             AS chargeoff_count,
    ROUND(SUM(did_charge_off) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS actual_co_pct,
    ROUND(AVG(total_score_60d), 2)                                  AS avg_score,
    ROUND(AVG(days_to_chargeoff), 0)                                AS avg_days_to_co
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY snapshot_date, risk_grade_60d, expected_co_rate_60d
ORDER BY snapshot_date, risk_grade_60d
;

-- Query 1c: Grade Distribution — LATEST AVAILABLE (Combined)
SELECT
    snapshot_date,
    risk_grade_latest                                               AS risk_grade,
    'latest'                                                        AS data_window,
    expected_co_rate_latest                                         AS expected_co_pct,
    COUNT(*)                                                        AS total_accounts,
    SUM(principal_at_snapshot)                                      AS total_principal,
    SUM(did_charge_off)                                             AS chargeoff_count,
    ROUND(SUM(did_charge_off) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS actual_co_pct,
    ROUND(AVG(total_score_latest), 2)                               AS avg_score,
    ROUND(AVG(days_to_chargeoff), 0)                                AS avg_days_to_co
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY snapshot_date, risk_grade_latest, expected_co_rate_latest
ORDER BY snapshot_date, risk_grade_latest
;


-- Query 2: Grade Comparison — All Three Windows Side by Side
SELECT
    snapshot_date,
    risk_grade_15d,
    risk_grade_60d,
    risk_grade_latest,
    COUNT(*)                                                        AS total_accounts,
    SUM(did_charge_off)                                             AS chargeoff_count,
    ROUND(SUM(did_charge_off) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS actual_co_pct
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY snapshot_date, risk_grade_15d, risk_grade_60d, risk_grade_latest
ORDER BY snapshot_date, risk_grade_latest
;


-- Query 3: Score Distribution Analysis — LATEST (by Snapshot)
SELECT
    snapshot_date,
    CASE
        WHEN total_score_latest < 0                    THEN '< 0'
        WHEN total_score_latest BETWEEN 0  AND 10      THEN '0-10'
        WHEN total_score_latest BETWEEN 10 AND 20      THEN '10-20'
        WHEN total_score_latest BETWEEN 20 AND 30      THEN '20-30'
        WHEN total_score_latest BETWEEN 30 AND 40      THEN '30-40'
        WHEN total_score_latest BETWEEN 40 AND 55      THEN '40-55'
        WHEN total_score_latest BETWEEN 55 AND 70      THEN '55-70'
        WHEN total_score_latest BETWEEN 70 AND 85      THEN '70-85'
        ELSE '85+'
    END                                                             AS score_bucket,
    risk_grade_latest                                               AS risk_grade,
    COUNT(*)                                                        AS account_count,
    SUM(did_charge_off)                                             AS co_count,
    ROUND(SUM(did_charge_off) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS co_rate_pct
FROM analytics.credit.risk_grade_chargeoff_analysis
GROUP BY 1, 2, 3
ORDER BY
    snapshot_date,
    CASE
        WHEN total_score_latest < 0                    THEN 1
        WHEN total_score_latest BETWEEN 0  AND 10      THEN 2
        WHEN total_score_latest BETWEEN 10 AND 20      THEN 3
        WHEN total_score_latest BETWEEN 20 AND 30      THEN 4
        WHEN total_score_latest BETWEEN 30 AND 40      THEN 5
        WHEN total_score_latest BETWEEN 40 AND 55      THEN 6
        WHEN total_score_latest BETWEEN 55 AND 70      THEN 7
        WHEN total_score_latest BETWEEN 70 AND 85      THEN 8
        ELSE 9
    END
;


-- Query 4: Component Score Impact Analysis — across all snapshots
SELECT
    'Vantage Score'             AS component,
    ROUND(AVG(CASE WHEN did_charge_off = 1 THEN score_vantage END), 2)              AS avg_score_co,
    ROUND(AVG(CASE WHEN did_charge_off = 0 THEN score_vantage END), 2)              AS avg_score_non_co
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
ORDER BY snapshot_date, risk_grade_latest, total_score_latest DESC
;