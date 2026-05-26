/*
=============================================================================
  MUW Risk Factors — Weekly aggregates for TL dashboard charts
  
  Prerequisite: ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD (TL Dashboard.sql)
  
  Objects created (materialized tables, daily refresh after base table):
    ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND  — Module B.1 / C.1
    ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ      — Module B.2 / B.3 / C.2 / C.3

  Date anchor     : REVIEW_COMPLETE_DATE (same as REVIEW_MONTH on base table)
  Week grain      : REVIEW_WEEK_START (Wed-to-Wed), excludes current in-flight week
  Rollups (4)     : UW×program, UW×ALL, team×program, team×ALL
                    team rows use UW_NAME = '' and PROGRAM_NAME = 'ALL' where applicable
  DQ seasoning    : reviews with REVIEW_COMPLETE_DATE <= CURRENT_DATE - 90 days
=============================================================================
*/

-- ---------------------------------------------------------------------------
-- Shared: eligible reviews + Wed week (exclude partial / in-flight week)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE AS

WITH period_anchor AS (
    SELECT DATEADD(
        'day',
        -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), CURRENT_DATE), 7),
        CURRENT_DATE
    ) AS current_wed_week_start
)

SELECT
    d.*,
    d.review_week_start                                           AS week_start_wed,
    d.review_week_label                                           AS week_label_wed,
    pa.current_wed_week_start,
    CASE
        WHEN d.review_week_start >= pa.current_wed_week_start THEN 1
        ELSE 0
    END                                                           AS is_partial_week,
    CASE
        WHEN d.review_complete_date <= DATEADD('day', -90, CURRENT_DATE) THEN 1
        ELSE 0
    END                                                           AS is_seasoned_90d
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD d
CROSS JOIN period_anchor pa
WHERE d.review_week_start IS NOT NULL
  AND d.review_week_start < pa.current_wed_week_start   /* exclude in-flight week */
  AND d.uw_name IS NOT NULL
  AND COALESCE(d.uw_is_active, TRUE) = TRUE;


-- ---------------------------------------------------------------------------
-- 1) WEEKLY_TREND — avg numeric rating (1–3) per factor
--    Denominator: Complete% reviews (Module B.1); change to all reviews if needed
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND AS

WITH src AS (
    SELECT *
    FROM ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE
    WHERE is_partial_week = 0
      AND is_complete = 1
),

/* --- per UW × program --- */
uw_program AS (
    SELECT
        'UW_PROGRAM'                                              AS rollup_type,
        uw_name,
        uw_id_resolved,
        tl_name,
        week_start_wed,
        week_label_wed,
        program_name,
        COUNT(*)                                                  AS review_count,
        ROUND(AVG(rf_bank_balance_num), 3)                        AS avg_bank_balance,
        ROUND(AVG(rf_quality_of_revenue_num), 3)                  AS avg_quality_of_revenue,
        ROUND(AVG(rf_personal_credit_num), 3)                     AS avg_personal_credit,
        ROUND(AVG(rf_industry_num), 3)                            AS avg_industry,
        ROUND(AVG(rf_debt_service_num), 3)                        AS avg_debt_service,
        ROUND(AVG(rf_residual_num), 3)                            AS avg_residual
    FROM src
    GROUP BY 1,2,3,4,5,6,7
),

/* --- per UW × ALL programs --- */
uw_all AS (
    SELECT
        'UW_ALL'                                                  AS rollup_type,
        uw_name,
        uw_id_resolved,
        tl_name,
        week_start_wed,
        week_label_wed,
        'ALL'                                                     AS program_name,
        COUNT(*)                                                  AS review_count,
        ROUND(AVG(rf_bank_balance_num), 3)                        AS avg_bank_balance,
        ROUND(AVG(rf_quality_of_revenue_num), 3)                  AS avg_quality_of_revenue,
        ROUND(AVG(rf_personal_credit_num), 3)                     AS avg_personal_credit,
        ROUND(AVG(rf_industry_num), 3)                            AS avg_industry,
        ROUND(AVG(rf_debt_service_num), 3)                        AS avg_debt_service,
        ROUND(AVG(rf_residual_num), 3)                            AS avg_residual
    FROM src
    GROUP BY 1,2,3,4,5,6,7
),

/* --- team baseline × program (uw_name blank) --- */
team_program AS (
    SELECT
        'TEAM_PROGRAM'                                            AS rollup_type,
        ''                                                        AS uw_name,
        NULL                                                      AS uw_id_resolved,
        NULL                                                      AS tl_name,
        week_start_wed,
        week_label_wed,
        program_name,
        COUNT(*)                                                  AS review_count,
        ROUND(AVG(rf_bank_balance_num), 3)                        AS avg_bank_balance,
        ROUND(AVG(rf_quality_of_revenue_num), 3)                  AS avg_quality_of_revenue,
        ROUND(AVG(rf_personal_credit_num), 3)                     AS avg_personal_credit,
        ROUND(AVG(rf_industry_num), 3)                            AS avg_industry,
        ROUND(AVG(rf_debt_service_num), 3)                        AS avg_debt_service,
        ROUND(AVG(rf_residual_num), 3)                            AS avg_residual
    FROM src
    GROUP BY week_start_wed, week_label_wed, program_name
),

/* --- team baseline × ALL --- */
team_all AS (
    SELECT
        'TEAM_ALL'                                                AS rollup_type,
        ''                                                        AS uw_name,
        NULL                                                      AS uw_id_resolved,
        NULL                                                      AS tl_name,
        week_start_wed,
        week_label_wed,
        'ALL'                                                     AS program_name,
        COUNT(*)                                                  AS review_count,
        ROUND(AVG(rf_bank_balance_num), 3)                        AS avg_bank_balance,
        ROUND(AVG(rf_quality_of_revenue_num), 3)                  AS avg_quality_of_revenue,
        ROUND(AVG(rf_personal_credit_num), 3)                     AS avg_personal_credit,
        ROUND(AVG(rf_industry_num), 3)                            AS avg_industry,
        ROUND(AVG(rf_debt_service_num), 3)                        AS avg_debt_service,
        ROUND(AVG(rf_residual_num), 3)                            AS avg_residual
    FROM src
    GROUP BY week_start_wed, week_label_wed
)

SELECT *, CURRENT_TIMESTAMP() AS table_refreshed_at FROM uw_program
UNION ALL SELECT *, CURRENT_TIMESTAMP() FROM uw_all
UNION ALL SELECT *, CURRENT_TIMESTAMP() FROM team_program
UNION ALL SELECT *, CURRENT_TIMESTAMP() FROM team_all;


-- ---------------------------------------------------------------------------
-- 2) WEEKLY_DQ — file count + UDR35 count by factor × rating band
--    Seasoned >= 90 days; Complete% reviews
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ AS

WITH src AS (
    SELECT *
    FROM ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE
    WHERE is_partial_week = 0
      AND is_complete = 1
      AND is_seasoned_90d = 1
),

long_rf AS (
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Bank Balance' AS risk_factor_name, rf_bank_balance AS risk_factor_rating
    FROM src WHERE rf_bank_balance IN ('Low','Medium','High')
    UNION ALL
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Quality of Revenue', rf_quality_of_revenue
    FROM src WHERE rf_quality_of_revenue IN ('Low','Medium','High')
    UNION ALL
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Personal Credit', rf_personal_credit
    FROM src WHERE rf_personal_credit IN ('Low','Medium','High')
    UNION ALL
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Industry', rf_industry
    FROM src WHERE rf_industry IN ('Low','Medium','High')
    UNION ALL
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Existing Debt Service', rf_debt_service
    FROM src WHERE rf_debt_service IN ('Low','Medium','High')
    UNION ALL
    SELECT week_start_wed, week_label_wed, uw_name, uw_id_resolved, tl_name, program_name,
           udr35, 'Residual', rf_residual
    FROM src WHERE rf_residual IN ('Low','Medium','High')
),

uw_program_dq AS (
    SELECT 'UW_PROGRAM' AS rollup_type, uw_name, uw_id_resolved, tl_name,
           week_start_wed, week_label_wed, program_name, risk_factor_name, risk_factor_rating,
           COUNT(*) AS review_count, SUM(udr35) AS udr35_count
    FROM long_rf
    GROUP BY uw_name, uw_id_resolved, tl_name, week_start_wed, week_label_wed,
             program_name, risk_factor_name, risk_factor_rating
),

uw_all_dq AS (
    SELECT 'UW_ALL' AS rollup_type, uw_name, uw_id_resolved, tl_name,
           week_start_wed, week_label_wed, 'ALL' AS program_name, risk_factor_name, risk_factor_rating,
           COUNT(*) AS review_count, SUM(udr35) AS udr35_count
    FROM long_rf
    GROUP BY uw_name, uw_id_resolved, tl_name, week_start_wed, week_label_wed,
             risk_factor_name, risk_factor_rating
),

team_program_dq AS (
    SELECT 'TEAM_PROGRAM' AS rollup_type, '' AS uw_name, NULL AS uw_id_resolved, NULL AS tl_name,
           week_start_wed, week_label_wed, program_name, risk_factor_name, risk_factor_rating,
           COUNT(*) AS review_count, SUM(udr35) AS udr35_count
    FROM long_rf
    GROUP BY week_start_wed, week_label_wed, program_name, risk_factor_name, risk_factor_rating
),

team_all_dq AS (
    SELECT 'TEAM_ALL' AS rollup_type, '' AS uw_name, NULL AS uw_id_resolved, NULL AS tl_name,
           week_start_wed, week_label_wed, 'ALL' AS program_name, risk_factor_name, risk_factor_rating,
           COUNT(*) AS review_count, SUM(udr35) AS udr35_count
    FROM long_rf
    GROUP BY week_start_wed, week_label_wed, risk_factor_name, risk_factor_rating
),

combined_dq AS (
    SELECT * FROM uw_program_dq
    UNION ALL SELECT * FROM uw_all_dq
    UNION ALL SELECT * FROM team_program_dq
    UNION ALL SELECT * FROM team_all_dq
)

SELECT
    rollup_type, uw_name, uw_id_resolved, tl_name,
    week_start_wed, week_label_wed, program_name,
    risk_factor_name, risk_factor_rating,
    review_count, udr35_count,
    ROUND(udr35_count * 100.0 / NULLIF(review_count, 0), 2) AS dq_pct,
    CURRENT_TIMESTAMP() AS table_refreshed_at
FROM combined_dq;


/* =========================================================================
   VALIDATION — run after refresh
========================================================================= */

-- V0: Program volumes on base table (spec: all 6 programs present)
-- SELECT program_name, COUNT(*) AS reviews,
--        COUNT(DISTINCT review_week_start) AS weeks
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- GROUP BY 1 ORDER BY 2 DESC;

-- V1: Partial-week exclusion (current Wed week should be 0 rows in weekly base)
-- SELECT is_partial_week, COUNT(*) FROM ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE GROUP BY 1;

-- V2: Weekly trend row counts by rollup
-- SELECT rollup_type, COUNT(*) AS rows, COUNT(DISTINCT week_start_wed) AS weeks
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND
-- GROUP BY 1 ORDER BY 1;

-- V3: Team baseline matches sum of active UWs (same week/program, avg factors)
-- WITH uw AS (
--   SELECT week_start_wed, program_name,
--          SUM(review_count) AS uw_rev_sum,
--          SUM(avg_personal_credit * review_count) / NULLIF(SUM(review_count),0) AS uw_wtd_pc
--   FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND
--   WHERE rollup_type = 'UW_PROGRAM'
--   GROUP BY 1,2
-- ),
-- team AS (
--   SELECT week_start_wed, program_name, review_count AS team_rev, avg_personal_credit AS team_pc
--   FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND
--   WHERE rollup_type = 'TEAM_PROGRAM'
-- )
-- SELECT u.week_start_wed, u.program_name, u.uw_rev_sum, t.team_rev,
--        ABS(u.uw_rev_sum - t.team_rev) AS rev_diff
-- FROM uw u JOIN team t USING (week_start_wed, program_name)
-- WHERE ABS(u.uw_rev_sum - t.team_rev) > 0
-- ORDER BY rev_diff DESC LIMIT 20;

-- V4: Weekly DQ rows sum to monthly (example: one UW, one factor)
-- SELECT DATE_TRUNC('month', week_start_wed) AS m,
--        SUM(review_count) AS w_rev, SUM(udr35_count) AS w_udr35
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ
-- WHERE rollup_type = 'TEAM_ALL' AND risk_factor_name = 'Personal Credit'
--   AND risk_factor_rating = 'High'
-- GROUP BY 1 ORDER BY 1;

-- V5: Seasoned filter — no reviews newer than 90 days in WEEKLY_DQ
-- SELECT MAX(review_complete_date) FROM ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE b
-- WHERE b.is_seasoned_90d = 1 AND b.is_complete = 1;


/* =========================================================================
   DASHBOARD SAMPLE QUERIES — swap HTML / BI data source
========================================================================= */

-- B.1 Team trend, all programs, last 52 weeks (team baseline)
/*
SELECT week_start_wed, week_label_wed,
       avg_bank_balance, avg_quality_of_revenue, avg_personal_credit,
       avg_industry, avg_debt_service, avg_residual, review_count
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND
WHERE rollup_type = 'TEAM_ALL'
  AND week_start_wed >= DATEADD('week', -52, CURRENT_DATE)
ORDER BY week_start_wed;
*/

-- C.1 UW vs team — Personal Credit avg, Pre-Approval only
/*
SELECT week_start_wed,
       MAX(CASE WHEN rollup_type = 'UW_PROGRAM' THEN avg_personal_credit END) AS uw_avg_pc,
       MAX(CASE WHEN rollup_type = 'TEAM_PROGRAM' THEN avg_personal_credit END) AS team_avg_pc
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND
WHERE uw_name IN ('Christopher Dykes', '')
  AND program_name = 'Pre-Approval'
  AND rollup_type IN ('UW_PROGRAM','TEAM_PROGRAM')
GROUP BY 1 ORDER BY 1;
*/

-- B.2 DQ small multiples — team, all programs, Personal Credit
/*
SELECT week_start_wed, risk_factor_rating, review_count, udr35_count, dq_pct
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ
WHERE rollup_type = 'TEAM_ALL'
  AND risk_factor_name = 'Personal Credit'
ORDER BY 1, 2;
*/

-- B.3 Calibration heal (High − Low DQ pp) — derived in app or:
/*
SELECT week_start_wed,
       MAX(CASE WHEN risk_factor_rating = 'High' THEN dq_pct END)
     - MAX(CASE WHEN risk_factor_rating = 'Low'  THEN dq_pct END) AS calibration_heal_pp
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ
WHERE rollup_type = 'TEAM_ALL'
  AND risk_factor_name = 'Industry'
GROUP BY 1 ORDER BY 1;
*/
