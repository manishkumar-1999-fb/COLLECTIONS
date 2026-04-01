-- ============================================================
-- MASTER TABLE: DELINQUENT ACCOUNTS — TWO SNAPSHOTS
-- Snapshots: Dec 15, 2025 and Jan 1, 2026
-- Collection scorecard only — 15-day window per snapshot
-- Model  : smm-collections-SV-v2-scorecard-production
-- Window : Most recent score within 15 days BEFORE each snapshot
-- Run with : CREDIT_ROLE
-- Output   : ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
-- ============================================================

SELECT * FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
ORDER BY snapshot_date, dpd_bucket;

CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT AS

WITH

-- ── 1. Define snapshot dates ───────────────────────────────────────────────
snapshots AS (
    SELECT '2025-12-15'::DATE AS snapshot_date
    UNION ALL
    SELECT '2026-01-01'::DATE AS snapshot_date
),

-- ── 2. Delinquent accounts for each snapshot ───────────────────────────────
delinquent AS (
    SELECT
        f.fbbid::VARCHAR                                AS fbbid,
        s.snapshot_date,
        MAX(f.dpd_days)::INT                            AS dpd_days,
        FLOOR(MAX(f.dpd_days) / 7)::INT                AS dpd_bucket,
        SUM(f.outstanding_principal_due)                AS principal,
        sum(f.outstanding_balance_due)                  as Total_balance
    FROM bi.finance.finance_metrics_daily f
    CROSS JOIN snapshots s
    WHERE f.edate         = s.snapshot_date
      AND f.product_type <> 'Flexpay'
      AND f.dpd_days::INT > 0
      AND f.is_charged_off = 0
    GROUP BY f.fbbid, s.snapshot_date
),

-- ── 3. Latest collection score per account within 15-day window per snapshot
-- For each snapshot, get the most recent score in the 15 days before it
all_scores AS (
    SELECT
        IDENTIFIER_ID                                   AS fbbid,
        SCORE                                           AS collection_score,
        MODEL_RAW_SCORE                                 AS collection_raw_score,
        MODEL_DECISION                                  AS collection_grade,
        CONFIDENCE                                      AS score_confidence,
        PREDICTION_TIME                                 AS scored_at
    FROM CDC_V2.SCORING.SCORING_MODEL_SCORES
    WHERE MODEL_NAME    = 'smm-collections-SV-v2-scorecard-production'
      AND IS_SHADOW_RUN = FALSE
      AND PREDICTION_TIME >= '2025-11-30'   -- 15 days before first snapshot (Dec 15)
      AND PREDICTION_TIME <= '2026-01-01'   -- up to last snapshot
),

scores_ranked AS (
    SELECT
        d.fbbid,
        d.snapshot_date,
        sc.collection_score,
        sc.collection_raw_score,
        sc.collection_grade,
        sc.score_confidence,
        sc.scored_at,
        DATEDIFF('day', sc.scored_at::DATE, d.snapshot_date) AS days_before_snapshot,
        ROW_NUMBER() OVER (
            PARTITION BY d.fbbid, d.snapshot_date
            ORDER BY sc.scored_at DESC
        )                                               AS rn
    FROM delinquent d
    LEFT JOIN all_scores sc
        ON sc.fbbid = d.fbbid
       AND sc.scored_at >= DATEADD('day', -15, d.snapshot_date)
       AND sc.scored_at <= d.snapshot_date
),

scores AS (
    SELECT
        fbbid,
        snapshot_date,
        collection_score,
        collection_raw_score,
        collection_grade,
        score_confidence,
        scored_at,
        days_before_snapshot
    FROM scores_ranked
    WHERE rn = 1
),

-- ── 4. Charge-off status as of NOW ─────────────────────────────────────────
co_lookup AS (
    SELECT
        fbbid::VARCHAR                                  AS fbbid,
        MIN(CASE WHEN is_charged_off = 1
                 THEN edate END)                        AS first_co_date,
        MAX(is_charged_off)                             AS is_currently_co
    FROM bi.finance.finance_metrics_daily
    WHERE product_type <> 'Flexpay'
    GROUP BY fbbid
)

-- ── FINAL SELECT ──────────────────────────────────────────────────────────
SELECT

    -- ── Identity ──────────────────────────────────────────────────────────
    d.fbbid,
    d.snapshot_date,

    -- ── DPD ───────────────────────────────────────────────────────────────
    d.dpd_days,
    d.dpd_bucket,
    CASE
        WHEN d.dpd_days BETWEEN 1  AND 7                THEN '01. 1-7d   (Bkt 1)'
        WHEN d.dpd_days BETWEEN 8  AND 14               THEN '02. 8-14d  (Bkt 2)'
        WHEN d.dpd_days BETWEEN 15 AND 21               THEN '03. 15-21d (Bkt 3)'
        WHEN d.dpd_days BETWEEN 22 AND 28               THEN '04. 22-28d (Bkt 4)'
        WHEN d.dpd_days BETWEEN 29 AND 35               THEN '05. 29-35d (Bkt 5)'
        WHEN d.dpd_days BETWEEN 36 AND 42               THEN '06. 36-42d (Bkt 6)'
        WHEN d.dpd_days BETWEEN 43 AND 49               THEN '07. 43-49d (Bkt 7)'
        WHEN d.dpd_days BETWEEN 50 AND 56               THEN '08. 50-56d (Bkt 8)'
        WHEN d.dpd_days BETWEEN 57 AND 63               THEN '09. 57-63d (Bkt 9)'
        WHEN d.dpd_days BETWEEN 64 AND 70               THEN '10. 64-70d (Bkt 10)'
        WHEN d.dpd_days BETWEEN 71 AND 77               THEN '11. 71-77d (Bkt 11)'
        WHEN d.dpd_days BETWEEN 78 AND 84               THEN '12. 78-84d (Bkt 12)'
        ELSE                                                 '13. 85-91d (Bkt 13)'
    END                                                 AS dpd_label,

    -- ── Principal ─────────────────────────────────────────────────────────
    d.principal,
    d.Total_balance,

    -- ── Collection scorecard ──────────────────────────────────────────────
    COALESCE(s.collection_grade, 'NG')                  AS collection_grade,
    s.collection_score,
    s.collection_raw_score,
    s.score_confidence,
    s.scored_at,
    s.days_before_snapshot,
    CASE WHEN s.collection_score IS NOT NULL THEN 1 ELSE 0 END AS has_collection_score,

    -- ── Charge-off outcome (as of NOW) ────────────────────────────────────
    CASE WHEN co.first_co_date > d.snapshot_date
         THEN 1 ELSE 0 END                              AS did_charge_off,
    co.first_co_date,
    co.is_currently_co,
    CASE WHEN co.first_co_date > d.snapshot_date
         THEN DATEDIFF('day', d.snapshot_date, co.first_co_date)
    END                                                 AS days_to_chargeoff,
    CASE WHEN co.first_co_date > d.snapshot_date
         THEN DATEDIFF('month', d.snapshot_date, co.first_co_date)
    END                                                 AS months_to_chargeoff

FROM delinquent   d
LEFT JOIN scores  s  ON s.fbbid = d.fbbid AND s.snapshot_date = d.snapshot_date
LEFT JOIN co_lookup co ON co.fbbid = d.fbbid;


-- ============================================================
-- VALIDATION — run immediately after CREATE to verify
-- ============================================================

SELECT
    snapshot_date,
    COUNT(*)                                            AS total_accounts,
    SUM(has_collection_score)                           AS scored,
    COUNT(*) - SUM(has_collection_score)                AS unscored,
    ROUND(SUM(has_collection_score) * 100.0
          / COUNT(*), 1)                                AS coverage_pct,
    SUM(did_charge_off)                                 AS charge_offs,
    ROUND(SUM(did_charge_off) * 100.0
          / COUNT(*), 1)                                AS co_rate_pct,
    ROUND(SUM(principal), 0)                            AS total_principal
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
GROUP BY snapshot_date
ORDER BY snapshot_date;


-- ============================================================
-- SUMMARY QUERIES — Dual Snapshot Analysis
-- ============================================================

-- S1: Collection grade summary by snapshot
SELECT
    snapshot_date,
    collection_grade,
    COUNT(*)                                            AS accounts,
    SUM(did_charge_off)                                 AS charge_offs,
    ROUND(SUM(did_charge_off) * 100.0
          / NULLIF(COUNT(*), 0), 1)                     AS co_rate_pct,
    ROUND(SUM(principal), 0)                            AS portfolio_principal,
    ROUND(SUM(CASE WHEN did_charge_off = 1
                   THEN principal ELSE 0 END), 0)       AS co_principal,
    ROUND(AVG(collection_score), 1)                     AS avg_score,
    ROUND(AVG(CASE WHEN did_charge_off = 1
                   THEN months_to_chargeoff END), 1)    AS avg_mob_to_co
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
GROUP BY snapshot_date, collection_grade
ORDER BY snapshot_date, collection_grade;


-- S2: Collection grade × DPD bucket by snapshot
SELECT
    snapshot_date,
    collection_grade,
    dpd_bucket,
    dpd_label,
    COUNT(*)                                            AS accounts,
    SUM(did_charge_off)                                 AS charge_offs,
    ROUND(SUM(did_charge_off) * 100.0
          / NULLIF(COUNT(*), 0), 1)                     AS co_rate_pct,
    ROUND(SUM(principal), 0)                            AS portfolio_principal,
    ROUND(SUM(CASE WHEN did_charge_off = 1
                   THEN principal ELSE 0 END), 0)       AS co_principal
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
GROUP BY snapshot_date, collection_grade, dpd_bucket, dpd_label
ORDER BY snapshot_date, collection_grade, dpd_bucket;


-- S3: Score freshness — how old are the scores being mapped?
SELECT
    snapshot_date,
    days_before_snapshot,
    COUNT(*)                                            AS accounts,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (PARTITION BY snapshot_date), 1) AS pct_of_snapshot
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
WHERE has_collection_score = 1
GROUP BY snapshot_date, days_before_snapshot
ORDER BY snapshot_date, days_before_snapshot;


-- S4: Comparison across snapshots — same grade, different outcomes?
SELECT
    collection_grade,
    SUM(CASE WHEN snapshot_date = '2025-12-15' THEN 1 ELSE 0 END) AS dec15_accounts,
    SUM(CASE WHEN snapshot_date = '2025-12-15' THEN did_charge_off ELSE 0 END) AS dec15_co,
    ROUND(SUM(CASE WHEN snapshot_date = '2025-12-15' THEN did_charge_off ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN snapshot_date = '2025-12-15' THEN 1 ELSE 0 END), 0), 1) AS dec15_co_rate,
    SUM(CASE WHEN snapshot_date = '2026-01-01' THEN 1 ELSE 0 END) AS jan01_accounts,
    SUM(CASE WHEN snapshot_date = '2026-01-01' THEN did_charge_off ELSE 0 END) AS jan01_co,
    ROUND(SUM(CASE WHEN snapshot_date = '2026-01-01' THEN did_charge_off ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN snapshot_date = '2026-01-01' THEN 1 ELSE 0 END), 0), 1) AS jan01_co_rate
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
GROUP BY collection_grade
ORDER BY collection_grade;


-- S5: DPD bucket distribution by snapshot
SELECT
    snapshot_date,
    dpd_label,
    COUNT(*)                                            AS accounts,
    SUM(did_charge_off)                                 AS charge_offs,
    ROUND(SUM(did_charge_off) * 100.0
          / NULLIF(COUNT(*), 0), 1)                     AS co_rate_pct,
    ROUND(SUM(principal), 0)                            AS total_principal
FROM ANALYTICS.CREDIT.MASTER_COLLECTION_GRADES_DUAL_SNAPSHOT
GROUP BY snapshot_date, dpd_label
ORDER BY snapshot_date, dpd_label;
