-- --============================================================
-- -- SETTLEMENT TEST — WEEKLY OPERATIONAL QUERY (VERIFIED)
-- -- ============================================================
-- -- Run every Friday in this order:
-- --   1. ONE-TIME SETUP  — run once at test launch to create table
-- --   2. STEP 1          — run every Friday to assign new entrants
-- --   3. STEP 2          — run every Friday to generate offer list
-- --
-- -- Key design:
-- --   - ~100 new accounts enter DPD 3+ each week over 3 months
-- --   - Each account is assigned TEST/CONTROL once, on first entry
-- --   - Assignment is permanent — never changes regardless of
-- --     balance or grade movements in subsequent weeks
-- --   - Offers (discount/tenure) update weekly based on current
-- --     grade and balance — only the GROUP is frozen
-- --   - TEST/CONTROL assignment excludes: active bankruptcy, settlement,
-- --     custom plan, or payment within 30 days (cleaner test population)
-- -- ============================================================



-- -- ============================================================
-- -- ONE-TIME SETUP — run once at test launch, never again
-- -- ============================================================
-- -- ============================================================
-- -- ONE-TIME SETUP (STABILIZED)
-- -- ============================================================
-- Ensure you are using a role that has CREATE TABLE permissions
-- USE ROLE <YOUR_ROLE_HERE>; 

USE DATABASE ANALYTICS;
USE SCHEMA CREDIT;

CREATE TABLE IF NOT EXISTS ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS (
    fbbid                      VARCHAR NOT NULL,
    group_assignment           VARCHAR NOT NULL,
    first_eligible_date        DATE    NOT NULL,
    risk_tier_at_entry         VARCHAR NOT NULL,
    bal_band_at_entry          VARCHAR NOT NULL,
    flag_bankruptcy            INTEGER NOT NULL,
    flag_settlement            INTEGER NOT NULL,
    flag_custom_plan           INTEGER NOT NULL,
    days_since_last_payment    INTEGER,
    flag_payment_within_30d    INTEGER NOT NULL,
    CONSTRAINT pk_fbbid PRIMARY KEY (fbbid)
);

-- If SETTLEMENT_TEST_GROUPS already existed without the flag columns, CREATE TABLE IF NOT EXISTS
-- does nothing and INSERT fails on FLAG_BANKRUPTCY. These ALTERs add missing columns; IF NOT EXISTS
-- keeps reruns safe. Defaults backfill existing rows before new inserts.
ALTER TABLE ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS ADD COLUMN IF NOT EXISTS flag_bankruptcy INTEGER DEFAULT 0;
ALTER TABLE ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS ADD COLUMN IF NOT EXISTS flag_settlement INTEGER DEFAULT 0;
ALTER TABLE ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS ADD COLUMN IF NOT EXISTS flag_custom_plan INTEGER DEFAULT 0;
ALTER TABLE ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS ADD COLUMN IF NOT EXISTS days_since_last_payment INTEGER;
ALTER TABLE ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS ADD COLUMN IF NOT EXISTS flag_payment_within_30d INTEGER DEFAULT 0;


-- -- ============================================================
-- -- STEP 1 — Run every Friday: assign newly eligible accounts
-- -- ============================================================
-- -- Inserts accounts currently in DPD 15-91 that are not yet in
-- -- the assignment table. Excludes bankruptcy, settlement, custom
-- -- plan, or recent payment (<=30 days) before randomization.
-- -- Safe to re-run — existing accounts are skipped by the NOT IN filter.
-- -- ============================================================

INSERT INTO ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS (
    fbbid,
    group_assignment,
    first_eligible_date,
    risk_tier_at_entry,
    bal_band_at_entry,
    flag_bankruptcy,
    flag_settlement,
    flag_custom_plan,
    days_since_last_payment,
    flag_payment_within_30d
)
WITH params AS (
    SELECT
        '2026-04-13'::DATE AS test_launch_date,
        MAX(edate) AS this_week_snapshot
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE DAYOFWEEK(edate) BETWEEN 1 AND 5
),
dacd_snapshot AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        IS_BANKRUPTCY,
        BANKRUPTCY_STATUS,
        IS_SETTLEMENT
    FROM bi.public.daily_approved_customers_data
    CROSS JOIN params
    WHERE edate = params.this_week_snapshot
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
),
last_payment AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MAX(payment_transmission_date) AS last_payment_date
    FROM bi.finance.payments_data
    WHERE payment_status = 'FUND'
      AND direction = 'D'
    GROUP BY 1
),
cfs_settlement AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MAX(CASE WHEN status_name = 'IS_IN_DISCOUNTED_SETTLEMENT'
            THEN status_value::INTEGER END) AS cfs_is_in_settlement,
        MAX(CASE WHEN status_name = 'SETTLEMENT_STATUS'
            THEN TRIM(status_value::VARCHAR, '"') END) AS cfs_status
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name IN ('IS_IN_DISCOUNTED_SETTLEMENT', 'SETTLEMENT_STATUS')
      AND last_row = 1
    GROUP BY 1
),
cjk_backy AS (
    SELECT
        FBBID::VARCHAR AS fbbid,
        CURRENT_STATUS AS cjk_status
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY settlement_rnk DESC, event_time DESC) = 1
),
active_custom_plans AS (
    SELECT
        dpp.fbbid::VARCHAR AS fbbid
    FROM bi.finance.dim_payment_plan dpp
    WHERE dpp.is_custom_plan = 1
      AND dpp.duration IS NOT NULL
    GROUP BY dpp.fbbid
    HAVING MAX(DATEADD('day',
        CASE
            WHEN dpp.time_units = 'MONTH' THEN dpp.duration * 30
            WHEN dpp.time_units = 'WEEK'  THEN dpp.duration * 7
            WHEN dpp.time_units = 'DAY'   THEN dpp.duration
            ELSE 0
        END,
        dpp.payment_plan_start_date)) >= (SELECT this_week_snapshot FROM params)
),
new_entrants AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        SUM(outstanding_balance_due::FLOAT) AS total_os
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    CROSS JOIN params
    WHERE edate = params.this_week_snapshot
      AND product_type <> 'Flexpay'
      AND dpd_days::INT BETWEEN 15 AND 91
      AND is_charged_off = 0
    GROUP BY fbbid::VARCHAR
    HAVING fbbid::VARCHAR NOT IN (SELECT fbbid FROM ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS)
),
scores AS (
    SELECT
        identifier_id AS fbbid,
        CASE
            WHEN score*100 >= 0  AND score*100 <= 30 THEN 'A'
            WHEN score*100 > 30  AND score*100 <= 40 THEN 'B'
            WHEN score*100 > 40  AND score*100 <= 55 THEN 'C'
            WHEN score*100 > 55  AND score*100 <= 70 THEN 'D'
            WHEN score*100 > 70  AND score*100 <= 85 THEN 'E'
            WHEN score*100 > 85                      THEN 'F'
        END AS raw_grade
    FROM CDC_V2.SCORING.SCORING_MODEL_SCORES
    CROSS JOIN params
    WHERE model_name = 'smm-collections-SV-v2-scorecard-production'
      AND identifier_type = 'FUNDBOX_BUSINESS'
      AND created_time::DATE >= DATEADD('day', -7, params.this_week_snapshot)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY identifier_id ORDER BY created_time DESC) = 1
),
base AS (
    SELECT
        n.fbbid,
        n.total_os,
        CASE
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') THEN 'Low'
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') THEN 'Mid'
            ELSE 'High'
        END AS risk_tier,
        CASE
            WHEN n.total_os < 5000 THEN '< $5K'
            WHEN n.total_os < 30000 THEN '$5K–$30K'
            ELSE '> $30K'
        END AS bal_band,
        DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) AS days_since_last_payment,
        CASE
            WHEN DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) IS NOT NULL
             AND DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) <= 30
            THEN 1 ELSE 0
        END AS flag_payment_within_30d,
        CASE
            WHEN COALESCE(d.IS_BANKRUPTCY::INTEGER, 0) = 1
             AND COALESCE(d.BANKRUPTCY_STATUS, '') NOT IN (
                 'DISMISSED_RESUME_COLLECTIONS', 'DISCHARGED_RESUME_COLLECTIONS')
            THEN 1 ELSE 0
        END AS flag_bankruptcy,
        CASE
            WHEN COALESCE(d.IS_SETTLEMENT::INTEGER, 0) = 1
              OR COALESCE(cs.cfs_is_in_settlement, 0) = 1
              OR cs.cfs_status IN ('ACTIVE', 'CREATED', 'FUNDED')
              OR cjk.cjk_status IN ('ACTIVE', 'FUNDED')
            THEN 1 ELSE 0
        END AS flag_settlement,
        CASE WHEN acp.fbbid IS NOT NULL THEN 1 ELSE 0 END AS flag_custom_plan
    FROM new_entrants n
    CROSS JOIN params p
    LEFT JOIN scores s ON s.fbbid = n.fbbid
    LEFT JOIN dacd_snapshot d ON d.fbbid = n.fbbid
    LEFT JOIN last_payment lp ON lp.fbbid = n.fbbid
    LEFT JOIN cfs_settlement cs ON cs.fbbid = n.fbbid
    LEFT JOIN cjk_backy cjk ON cjk.fbbid = n.fbbid
    LEFT JOIN active_custom_plans acp ON acp.fbbid = n.fbbid
),
eligible_for_assignment AS (
    SELECT *
    FROM base
    WHERE flag_bankruptcy = 0
      AND flag_settlement = 0
      AND flag_custom_plan = 0
      AND flag_payment_within_30d = 0
),
ranked AS (
    SELECT *,
        COUNT(*) OVER (PARTITION BY risk_tier, bal_band) AS stratum_n,
        ROW_NUMBER() OVER (
            PARTITION BY risk_tier, bal_band
            ORDER BY HASH(fbbid, 42)
        ) AS rand_rank
    FROM eligible_for_assignment
)
SELECT
    fbbid,
    CASE WHEN rand_rank <= CEIL(stratum_n * 0.5) THEN 'TEST' ELSE 'CONTROL' END AS group_assignment,
    p.test_launch_date AS first_eligible_date,
    risk_tier AS risk_tier_at_entry,
    bal_band AS bal_band_at_entry,
    flag_bankruptcy,
    flag_settlement,
    flag_custom_plan,
    days_since_last_payment,
    flag_payment_within_30d
FROM ranked
CROSS JOIN params p;


-- STEP 2 — Offer list: same exclusion as Step 1 (clean_cohort) so rows
-- match the TEST/CONTROL eligible population.

WITH params AS (
    SELECT MAX(edate) AS this_week_snapshot
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE DAYOFWEEK(edate) BETWEEN 1 AND 5
),
dacd_snapshot AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        IS_BANKRUPTCY,
        BANKRUPTCY_STATUS,
        IS_SETTLEMENT
    FROM bi.public.daily_approved_customers_data
    CROSS JOIN params
    WHERE edate = params.this_week_snapshot
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
),
last_payment AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MAX(payment_transmission_date) AS last_payment_date
    FROM bi.finance.payments_data
    WHERE payment_status = 'FUND'
      AND direction = 'D'
    GROUP BY 1
),
cfs_settlement AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MAX(CASE WHEN status_name = 'IS_IN_DISCOUNTED_SETTLEMENT'
            THEN status_value::INTEGER END) AS cfs_is_in_settlement,
        MAX(CASE WHEN status_name = 'SETTLEMENT_STATUS'
            THEN TRIM(status_value::VARCHAR, '"') END) AS cfs_status
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name IN ('IS_IN_DISCOUNTED_SETTLEMENT', 'SETTLEMENT_STATUS')
      AND last_row = 1
    GROUP BY 1
),
cjk_backy AS (
    SELECT
        FBBID::VARCHAR AS fbbid,
        CURRENT_STATUS AS cjk_status
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY settlement_rnk DESC, event_time DESC) = 1
),
active_custom_plans AS (
    SELECT
        dpp.fbbid::VARCHAR AS fbbid
    FROM bi.finance.dim_payment_plan dpp
    WHERE dpp.is_custom_plan = 1
      AND dpp.duration IS NOT NULL
    GROUP BY dpp.fbbid
    HAVING MAX(DATEADD('day',
        CASE
            WHEN dpp.time_units = 'MONTH' THEN dpp.duration * 30
            WHEN dpp.time_units = 'WEEK'  THEN dpp.duration * 7
            WHEN dpp.time_units = 'DAY'   THEN dpp.duration
            ELSE 0
        END,
        dpp.payment_plan_start_date)) >= (SELECT this_week_snapshot FROM params)
),
cohort AS (
    SELECT
        fbbid::VARCHAR AS fbbid,
        MAX(CEIL(dpd_days::INT / 7.0)) AS dpd_bucket,
        SUM(outstanding_balance_due::FLOAT) AS total_os
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    CROSS JOIN params
    WHERE edate = params.this_week_snapshot
      AND product_type <> 'Flexpay'
      AND dpd_days::INT BETWEEN 15 AND 91
      AND is_charged_off = 0
    GROUP BY fbbid::VARCHAR
),
acct_attrs AS (
    SELECT
        c.fbbid,
        DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) AS days_since_last_payment,
        CASE
            WHEN DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) IS NOT NULL
             AND DATEDIFF('day', lp.last_payment_date, p.this_week_snapshot) <= 30
            THEN 1 ELSE 0
        END AS flag_payment_within_30d,
        CASE
            WHEN COALESCE(d.IS_BANKRUPTCY::INTEGER, 0) = 1
             AND COALESCE(d.BANKRUPTCY_STATUS, '') NOT IN (
                 'DISMISSED_RESUME_COLLECTIONS', 'DISCHARGED_RESUME_COLLECTIONS')
            THEN 1 ELSE 0
        END AS flag_bankruptcy,
        CASE
            WHEN COALESCE(d.IS_SETTLEMENT::INTEGER, 0) = 1
              OR COALESCE(cs.cfs_is_in_settlement, 0) = 1
              OR cs.cfs_status IN ('ACTIVE', 'CREATED', 'FUNDED')
              OR cjk.cjk_status IN ('ACTIVE', 'FUNDED')
            THEN 1 ELSE 0
        END AS flag_settlement,
        CASE WHEN acp.fbbid IS NOT NULL THEN 1 ELSE 0 END AS flag_custom_plan
    FROM cohort c
    CROSS JOIN params p
    LEFT JOIN dacd_snapshot d ON d.fbbid = c.fbbid
    LEFT JOIN last_payment lp ON lp.fbbid = c.fbbid
    LEFT JOIN cfs_settlement cs ON cs.fbbid = c.fbbid
    LEFT JOIN cjk_backy cjk ON cjk.fbbid = c.fbbid
    LEFT JOIN active_custom_plans acp ON acp.fbbid = c.fbbid
),
clean_cohort AS (
    SELECT
        c.fbbid,
        c.dpd_bucket,
        c.total_os
    FROM cohort c
    INNER JOIN acct_attrs a ON a.fbbid = c.fbbid
    WHERE a.flag_bankruptcy = 0
      AND a.flag_settlement = 0
      AND a.flag_custom_plan = 0
      AND a.flag_payment_within_30d = 0
),
scores AS (
    SELECT
        identifier_id AS fbbid,
        score * 100 AS score_x100,
        CASE
            WHEN score*100 >= 0  AND score*100 <= 30 THEN 'A'
            WHEN score*100 > 30  AND score*100 <= 40 THEN 'B'
            WHEN score*100 > 40  AND score*100 <= 55 THEN 'C'
            WHEN score*100 > 55  AND score*100 <= 70 THEN 'D'
            WHEN score*100 > 70  AND score*100 <= 85 THEN 'E'
            WHEN score*100 > 85                      THEN 'F'
        END AS raw_grade
    FROM CDC_V2.SCORING.SCORING_MODEL_SCORES
    CROSS JOIN params
    WHERE model_name = 'smm-collections-SV-v2-scorecard-production'
      AND identifier_type = 'FUNDBOX_BUSINESS'
      AND created_time::DATE >= DATEADD('day', -7, params.this_week_snapshot)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY identifier_id ORDER BY created_time DESC) = 1
),
offers AS (
    SELECT
        c.fbbid,
        c.dpd_bucket,
        ROUND(c.total_os, 2) AS total_os,
        ROUND(s.score_x100, 1) AS score_x100,
        CASE WHEN s.raw_grade IS NULL THEN 'NG' ELSE s.raw_grade END AS display_grade,
        CASE
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') THEN 'Low'
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') THEN 'Mid'
            ELSE 'High'
        END AS risk_tier,
        CASE
            WHEN c.total_os < 5000 THEN '< $5K'
            WHEN c.total_os < 30000 THEN '$5K–$30K'
            ELSE '> $30K'
        END AS bal_band,
        CASE
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 5000 THEN 15
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 30000 THEN 20
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') THEN 25
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 5000 THEN 20
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 30000 THEN 30
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') THEN 35
            WHEN c.total_os < 5000 THEN 40
            WHEN c.total_os < 30000 THEN 50
            ELSE 50
        END AS opt1_disc,
        CASE
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 5000 THEN 10
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 30000 THEN 15
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') THEN 20
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 5000 THEN 15
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 30000 THEN 25
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') THEN 30
            WHEN c.total_os < 5000 THEN 25
            WHEN c.total_os < 30000 THEN 40
            ELSE 45
        END AS opt2_disc,
        CASE
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 5000 THEN NULL
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') AND c.total_os < 30000 THEN 10
            WHEN COALESCE(s.raw_grade,'D') IN ('A','B') THEN 15
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 5000 THEN NULL
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') AND c.total_os < 30000 THEN 20
            WHEN COALESCE(s.raw_grade,'D') IN ('C','D') THEN 25
            WHEN c.total_os < 5000 THEN NULL
            WHEN c.total_os < 30000 THEN 30
            ELSE 40
        END AS opt3_disc,
        a.days_since_last_payment,
        a.flag_payment_within_30d,
        a.flag_bankruptcy,
        a.flag_settlement,
        a.flag_custom_plan
    FROM clean_cohort c
    LEFT JOIN scores s ON s.fbbid = c.fbbid
    LEFT JOIN acct_attrs a ON a.fbbid = c.fbbid
)
SELECT
    o.fbbid,
    o.dpd_bucket,
    o.display_grade AS backy_grade,
    o.score_x100,
    o.risk_tier,
    o.bal_band,
    o.total_os,
    o.days_since_last_payment,
    o.flag_payment_within_30d,
    o.flag_bankruptcy,
    o.flag_settlement,
    o.flag_custom_plan,
    COALESCE(g.group_assignment, 'NEW') AS group_assignment,
    o.opt1_disc AS opt1_discount,
    1 AS opt1_term_weeks,
    1 AS opt1_term_quad,
    o.opt2_disc AS opt2_discount,
    24 AS opt2_term_weeks,
    6 AS opt2_term_quad,
    o.opt3_disc AS opt3_discount,
    CASE WHEN o.opt3_disc IS NOT NULL THEN 48 END AS opt3_term_weeks,
    CASE WHEN o.opt3_disc IS NOT NULL THEN 12 END AS opt3_term_quad
FROM offers o
LEFT JOIN ANALYTICS.CREDIT.SETTLEMENT_TEST_GROUPS g ON g.fbbid = o.fbbid
ORDER BY g.group_assignment, o.risk_tier, o.bal_band, o.fbbid;
