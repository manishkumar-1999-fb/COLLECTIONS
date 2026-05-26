/*
  Pre-aggregated datasets for MUW TL Risk Factors Dashboard (HTML v3)
  Run AFTER refreshing ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD (TL Dashboard.sql)

  Fixes addressed:
    - IN_T4W: Wed-to-Wed weeks via is_in_t4w (last 4 complete weeks)
    - IN_3MO / IN_6MO / IN_12MO: is_in_* flags on base table
    - Filters: program_name, uw_type, program_group, tl_name, uw_name
    - DQ charts: optional is_mature_6mo = 1 to avoid right-censoring
    - Override matrix lookback: review_complete_date + review-level grades
    - Calibration heal: high DQ% minus low DQ% by UW and risk factor
*/

-- ---------------------------------------------------------------------------
-- 0) DATA COVERAGE — run first to confirm 4W / 3M rows exist
-- ---------------------------------------------------------------------------
/*
SELECT
    'IN_T4W' AS period_key,
    COUNT(*) AS review_rows,
    COUNT(DISTINCT review_week_start) AS distinct_weeks,
    MIN(review_week_start) AS min_week,
    MAX(review_week_start) AS max_week
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE is_in_t4w = 1

UNION ALL

SELECT
    'IN_3MO',
    COUNT(*),
    COUNT(DISTINCT review_month),
    MIN(review_complete_date),
    MAX(review_complete_date)
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE is_in_3mo = 1

UNION ALL

SELECT
    'IN_12MO',
    COUNT(*),
    COUNT(DISTINCT review_month),
    MIN(review_complete_date),
    MAX(review_complete_date)
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE is_in_12mo = 1;
*/


-- ---------------------------------------------------------------------------
-- 1) DQ BY RATING — weekly (T4W) + monthly (3M/6M/12M)
--    Dashboard: Module B.2, Team Calibration, DQ by rating charts
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RF_DQ_BY_RATING AS

WITH base AS (
    SELECT
        review_complete_date,
        review_week_start,
        review_week_label,
        review_month,
        is_in_t4w,
        is_in_3mo,
        is_in_6mo,
        is_in_12mo,
        program_name,
        program_group,
        uw_type,
        is_complete,
        is_mature_6mo,
        tl_name,
        manager_name,
        uw_name,
        udr35,
        rf_bank_balance,
        rf_quality_of_revenue,
        rf_personal_credit,
        rf_industry,
        rf_debt_service,
        rf_residual
    FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
    WHERE uw_name IS NOT NULL
      AND is_complete = 1
),

long_rf AS (
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Bank Balance' AS risk_factor_name, rf_bank_balance AS risk_factor_rating
    FROM base WHERE rf_bank_balance IN ('Low', 'Medium', 'High')
    UNION ALL
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Quality of Revenue', rf_quality_of_revenue
    FROM base WHERE rf_quality_of_revenue IN ('Low', 'Medium', 'High')
    UNION ALL
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Personal Credit', rf_personal_credit
    FROM base WHERE rf_personal_credit IN ('Low', 'Medium', 'High')
    UNION ALL
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Industry', rf_industry
    FROM base WHERE rf_industry IN ('Low', 'Medium', 'High')
    UNION ALL
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Existing Debt Service', rf_debt_service
    FROM base WHERE rf_debt_service IN ('Low', 'Medium', 'High')
    UNION ALL
    SELECT review_complete_date, review_week_start, review_week_label, review_month,
           is_in_t4w, is_in_3mo, is_in_6mo, is_in_12mo,
           program_name, program_group, uw_type, tl_name, manager_name, uw_name, udr35,
           'Residual', rf_residual
    FROM base WHERE rf_residual IN ('Low', 'Medium', 'High')
),

weekly AS (
    SELECT
        'IN_T4W' AS period_key,
        review_week_start AS period_start,
        review_week_label AS period_label,
        program_name,
        program_group,
        uw_type,
        tl_name,
        manager_name,
        uw_name,
        risk_factor_name,
        risk_factor_rating,
        COUNT(*) AS review_count,
        SUM(udr35) AS dq_count,
        ROUND(SUM(udr35) * 100.0 / NULLIF(COUNT(*), 0), 2) AS dq_pct
    FROM long_rf
    WHERE is_in_t4w = 1
    GROUP BY 1,2,3,4,5,6,7,8,9
),

monthly AS (
    SELECT
        p.period_key,
        l.review_month AS period_start,
        TO_CHAR(l.review_month, 'YYYY-MM') AS period_label,
        l.program_name,
        l.program_group,
        l.uw_type,
        l.tl_name,
        l.manager_name,
        l.uw_name,
        l.risk_factor_name,
        l.risk_factor_rating,
        COUNT(*) AS review_count,
        SUM(l.udr35) AS dq_count,
        ROUND(SUM(l.udr35) * 100.0 / NULLIF(COUNT(*), 0), 2) AS dq_pct
    FROM long_rf l
    INNER JOIN (
        SELECT 'IN_3MO' AS period_key, 1 AS flag
        UNION ALL SELECT 'IN_6MO', 1
        UNION ALL SELECT 'IN_12MO', 1
    ) p ON 1=1
    WHERE (p.period_key = 'IN_3MO'  AND l.is_in_3mo  = 1)
       OR (p.period_key = 'IN_6MO'  AND l.is_in_6mo  = 1)
       OR (p.period_key = 'IN_12MO' AND l.is_in_12mo = 1)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

SELECT * FROM weekly
UNION ALL
SELECT * FROM monthly;


-- ---------------------------------------------------------------------------
-- 2) CALIBRATION HEAL — DQ%(High) - DQ%(Low) by UW, factor, period
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RF_CALIBRATION_HEAL AS

WITH dq AS (
    SELECT *
    FROM ANALYTICS.CREDIT.MUW_RF_DQ_BY_RATING
    WHERE review_count >= 5
),

pivoted AS (
    SELECT
        period_key,
        period_start,
        period_label,
        program_name,
        program_group,
        uw_type,
        is_complete,
        is_mature_6mo,
        tl_name,
        manager_name,
        uw_name,
        risk_factor_name,
        MAX(CASE WHEN risk_factor_rating = 'High' THEN dq_pct END) AS dq_pct_high,
        MAX(CASE WHEN risk_factor_rating = 'Low'  THEN dq_pct END) AS dq_pct_low,
        MAX(CASE WHEN risk_factor_rating = 'High' THEN review_count END) AS n_high,
        MAX(CASE WHEN risk_factor_rating = 'Low'  THEN review_count END) AS n_low
    FROM dq
    WHERE risk_factor_rating IN ('Low', 'High')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

SELECT
    *,
    ROUND(dq_pct_high - dq_pct_low, 2) AS calibration_heal,
    CASE
        WHEN dq_pct_high IS NULL OR dq_pct_low IS NULL THEN 'Insufficient buckets'
        WHEN dq_pct_high > dq_pct_low THEN 'Aligned (High > Low DQ)'
        WHEN dq_pct_high < dq_pct_low THEN 'Inverted (High < Low DQ)'
        ELSE 'Flat'
    END AS calibration_flag
FROM pivoted;


-- ---------------------------------------------------------------------------
-- 3) PORTFOLIO COMPOSITION — Module A (filterable by program + uw_type)
--    Snapshot: rnk_latest = 1 within each period window
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RF_PORTFOLIO_COMP AS

WITH perioded AS (
    SELECT 'IN_T4W' AS period_key, s.*
    FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD s
    WHERE s.is_in_t4w = 1 AND s.vantage_bucket IS NOT NULL
    UNION ALL
    SELECT 'IN_3MO', s.* FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD s
    WHERE s.is_in_3mo = 1 AND s.vantage_bucket IS NOT NULL
    UNION ALL
    SELECT 'IN_6MO', s.* FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD s
    WHERE s.is_in_6mo = 1 AND s.vantage_bucket IS NOT NULL
    UNION ALL
    SELECT 'IN_12MO', s.* FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD s
    WHERE s.is_in_12mo = 1 AND s.vantage_bucket IS NOT NULL
),

scoped AS (
    SELECT *
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.period_key, p.fbbid
                ORDER BY p.review_complete_date DESC
            ) AS rn_period_latest
        FROM perioded p
    )
    WHERE rn_period_latest = 1
)

SELECT
    period_key,
    program_name,
    program_group,
    uw_type,
    tl_name,
    manager_name,
    uw_name,
    'Vantage' AS dimension_name,
    vantage_bucket AS dimension_value,
    COUNT(DISTINCT fbbid) AS customer_count
FROM perioded
GROUP BY 1,2,3,4,5,6,7,8

UNION ALL

SELECT period_key, program_name, program_group, tl_name, manager_name, uw_name,
       'OG Bucket', og_bucket_group_banded, COUNT(DISTINCT fbbid)
FROM perioded
WHERE og_bucket_group_banded IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8

UNION ALL

SELECT period_key, program_name, program_group, tl_name, manager_name, uw_name,
       'Revenue', revenue_band, COUNT(DISTINCT fbbid)
FROM perioded
WHERE revenue_band IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8;


-- ---------------------------------------------------------------------------
-- 4) OVERRIDE MATRIX — Module D (lookback on review_complete_date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RF_OVERRIDE_MATRIX AS

SELECT 'IN_T4W' AS period_key, review_complete_date, review_month, program_name, uw_type,
       tl_name, uw_name, automated_risk_grade, uw_risk_grade, COUNT(*) AS override_count
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE program_name = 'Second Look'
  AND automated_risk_grade IS NOT NULL AND uw_risk_grade IS NOT NULL AND is_in_t4w = 1
GROUP BY 2,3,4,5,6,7,8,9

UNION ALL

SELECT 'IN_3MO', review_complete_date, review_month, program_name, uw_type,
       tl_name, uw_name, automated_risk_grade, uw_risk_grade, COUNT(*)
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE program_name = 'Second Look'
  AND automated_risk_grade IS NOT NULL AND uw_risk_grade IS NOT NULL AND is_in_3mo = 1
GROUP BY 2,3,4,5,6,7,8,9

UNION ALL

SELECT 'IN_6MO', review_complete_date, review_month, program_name, uw_type,
       tl_name, uw_name, automated_risk_grade, uw_risk_grade, COUNT(*)
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE program_name = 'Second Look'
  AND automated_risk_grade IS NOT NULL AND uw_risk_grade IS NOT NULL AND is_in_6mo = 1
GROUP BY 2,3,4,5,6,7,8,9

UNION ALL

SELECT 'IN_12MO', review_complete_date, review_month, program_name, uw_type,
       tl_name, uw_name, automated_risk_grade, uw_risk_grade, COUNT(*)
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE program_name = 'Second Look'
  AND automated_risk_grade IS NOT NULL AND uw_risk_grade IS NOT NULL AND is_in_12mo = 1
GROUP BY 2,3,4,5,6,7,8,9;


-- ---------------------------------------------------------------------------
-- 5) DQ RATING MONOTONICITY CHECK — explains "inversion" per risk factor
-- ---------------------------------------------------------------------------
/*
SELECT
    period_key,
    risk_factor_name,
    risk_factor_rating,
    SUM(review_count) AS reviews,
    ROUND(SUM(dq_count) * 100.0 / NULLIF(SUM(review_count), 0), 2) AS dq_pct
FROM ANALYTICS.CREDIT.MUW_RF_DQ_BY_RATING
WHERE period_key = 'IN_3MO'
  AND program_name = 'All'  -- remove or filter as needed
GROUP BY 1,2,3
ORDER BY 2, 3;
*/
