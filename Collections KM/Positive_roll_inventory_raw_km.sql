-- Unified positive roll + KM-style raw inventory (single script, two tables).
-- (1) POSITIVE_ROLL_INVENTORY_RAW_FBBID_WEEK_BUCKET — daily grain in Wed–Tue window + roll flags.
-- (2) POSITIVE_ROLL_RATE_WEEKLY — same weekly metrics as former Positive_roll_rate_WedThu.sql (built from raw roll_* columns).
--
-- KM raw: Collections_KM.sql — fmd_agg (customer daily), customer_overdue, dpd_bucket_group.
-- Roll: cohort Wed inventory Wed–Tue, outcome next Wed, is_positive_roll / ODB QA (see classified).
-- Edit pull_weeks in both statements (kept identical below).

-- =============================================================================
-- TABLE 1: Raw (fbbid × week × inventory_edate × dpd_bucket) + roll_* from classified
-- =============================================================================
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.POSITIVE_ROLL_INVENTORY_RAW_FBBID_WEEK_BUCKET AS
WITH pull_weeks AS (
    SELECT
        '2024-01-01'::DATE AS week_from_inclusive,
        CURRENT_DATE()::DATE AS week_through_inclusive
),
first_table AS (
    SELECT
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2024-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),
-- KM customer-level daily (raw rows)
fmd_agg AS (
    SELECT
        fmd.fbbid,
        fmd.edate,
        MAX(fmd.dpd_days) AS dpd_days,
        MAX(fmd.dpd_bucket) AS dpd_bucket,
        SUM(fmd.outstanding_principal_due * COALESCE(flu.loan_fx_rate, 1.0)) AS outstanding_principal_due,
        MAX(fmd.is_charged_off) AS is_charged_off
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.edate >= '2024-01-01'
      AND fmd.PRODUCT_TYPE <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
    GROUP BY fmd.fbbid, fmd.edate
),
overdue_balance_wed AS (
    SELECT
        T2.EDATE AS snapshot_wednesday,
        T1.FBBID,
        SUM(TO_DOUBLE(T1.STATUS_VALUE) * COALESCE(flu.loan_fx_rate, 1.0)) AS overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = T1.LOAN_KEY
    JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND DAYOFWEEK(T2.EDATE) = 3
        AND T2.EDATE <= CURRENT_DATE
    JOIN (
        SELECT DISTINCT fmd2.loan_key, fmd2.edate
        FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd2
        WHERE fmd2.loan_operational_status <> 'CNCL'
          AND fmd2.is_charged_off = 0
    ) t3 ON t3.loan_key = T1.loan_key AND t3.edate = T2.EDATE
    WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
    GROUP BY T2.EDATE, T1.FBBID
),
cohort_weeks AS (
    SELECT d.edate AS week_start_wednesday
    FROM BI.INTERNAL.DATES d
    CROSS JOIN pull_weeks p
    WHERE DAYOFWEEK(d.edate) = 3
      AND d.edate >= '2024-01-01'
      AND d.edate BETWEEN p.week_from_inclusive AND p.week_through_inclusive
      AND DATEADD(week, 1, d.edate) <= CURRENT_DATE()
),
week_day_offsets AS (
    SELECT seq AS d
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq
        FROM TABLE (GENERATOR (ROWCOUNT => 7))
    )
),
week_spine AS (
    SELECT
        cw.week_start_wednesday,
        DATEADD(day, o.d, cw.week_start_wednesday) AS inv_date
    FROM cohort_weeks cw
    CROSS JOIN week_day_offsets o
),
inventory_dates AS (
    SELECT DISTINCT inv_date FROM week_spine
),
overdue_balance_daily AS (
    SELECT
        T2.EDATE AS as_of_date,
        T1.FBBID,
        SUM(TO_DOUBLE(T1.STATUS_VALUE) * COALESCE(flu.loan_fx_rate, 1.0)) AS overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = T1.LOAN_KEY
    JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND T2.EDATE <= CURRENT_DATE
    JOIN inventory_dates id ON id.inv_date = T2.EDATE
    JOIN (
        SELECT DISTINCT fmd2.loan_key, fmd2.edate
        FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd2
        WHERE fmd2.loan_operational_status <> 'CNCL'
          AND fmd2.is_charged_off = 0
    ) t3 ON t3.loan_key = T1.loan_key AND t3.edate = T2.EDATE
    WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
    GROUP BY T2.EDATE, T1.FBBID
),
cohort_accounts AS (
    SELECT DISTINCT
        cw.week_start_wednesday,
        fmd.fbbid,
        fmd.loan_key
    FROM cohort_weeks cw
    JOIN BI.FINANCE.FINANCE_METRICS_DAILY fmd
        ON fmd.edate BETWEEN cw.week_start_wednesday AND DATEADD(day, 6, cw.week_start_wednesday)
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    WHERE fmd.edate >= '2024-01-01'
      AND fmd.PRODUCT_TYPE <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 0
      AND fmd.dpd_bucket BETWEEN 1 AND 13
),
first_delinquent_in_week AS (
    SELECT
        cw.week_start_wednesday,
        fmd.fbbid,
        MIN(fmd.edate) AS first_delinquent_date
    FROM cohort_weeks cw
    JOIN BI.FINANCE.FINANCE_METRICS_DAILY fmd
        ON fmd.edate BETWEEN cw.week_start_wednesday AND DATEADD(day, 6, cw.week_start_wednesday)
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    WHERE fmd.edate >= '2024-01-01'
      AND fmd.PRODUCT_TYPE <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 0
      AND fmd.dpd_bucket BETWEEN 1 AND 13
    GROUP BY cw.week_start_wednesday, fmd.fbbid
),
odb_max_in_window AS (
    SELECT
        ws.week_start_wednesday,
        ca.fbbid,
        MAX(ob.overdue_balance) AS odb_max_in_window
    FROM week_spine ws
    JOIN cohort_accounts ca
        ON ca.week_start_wednesday = ws.week_start_wednesday
    LEFT JOIN overdue_balance_daily ob
        ON ob.fbbid = ca.fbbid AND ob.as_of_date = ws.inv_date
    GROUP BY ws.week_start_wednesday, ca.fbbid
),
dpd_inv_window AS (
    SELECT
        cw.week_start_wednesday,
        fmd.fbbid,
        MAX(fmd.dpd_bucket) AS dpd_w0_max
    FROM cohort_weeks cw
    JOIN BI.FINANCE.FINANCE_METRICS_DAILY fmd
        ON fmd.edate BETWEEN cw.week_start_wednesday AND DATEADD(day, 6, cw.week_start_wednesday)
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    WHERE fmd.PRODUCT_TYPE <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 0
    GROUP BY cw.week_start_wednesday, fmd.fbbid
),
dpd_next_wed AS (
    SELECT
        cw.week_start_wednesday,
        fmd.fbbid,
        fmd.dpd_bucket AS dpd_w1,
        fmd.is_charged_off AS co_w1
    FROM cohort_weeks cw
    JOIN BI.FINANCE.FINANCE_METRICS_DAILY fmd
        ON fmd.edate = DATEADD(week, 1, cw.week_start_wednesday)
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    WHERE fmd.PRODUCT_TYPE <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cw.week_start_wednesday, fmd.fbbid ORDER BY fmd.edate DESC) = 1
),
cohort_fbbid_week AS (
    SELECT DISTINCT week_start_wednesday, fbbid
    FROM cohort_accounts
),
cohort_enriched AS (
    SELECT
        c.week_start_wednesday,
        DATEADD(day, 6, c.week_start_wednesday) AS week_end_tuesday,
        DATEADD(week, 1, c.week_start_wednesday) AS outcome_wednesday,
        c.fbbid,
        fd.first_delinquent_date,
        ob_fd.overdue_balance AS odb_first_delinquent_day,
        omx.odb_max_in_window,
        COALESCE(ob0.overdue_balance, 0) AS odb_w0,
        ob1.overdue_balance AS odb_w1_raw,
        (nw.fbbid IS NOT NULL) AS has_dpd_w1,
        (ob1.fbbid IS NOT NULL) AS has_odb_w1,
        iw.dpd_w0_max,
        nw.dpd_w1,
        nw.co_w1,
        (nw.fbbid IS NOT NULL OR ob1.fbbid IS NOT NULL) AS has_outcome_snapshot
    FROM cohort_fbbid_week c
    LEFT JOIN first_delinquent_in_week fd
        ON fd.week_start_wednesday = c.week_start_wednesday AND fd.fbbid = c.fbbid
    LEFT JOIN overdue_balance_daily ob_fd
        ON ob_fd.fbbid = c.fbbid AND ob_fd.as_of_date = fd.first_delinquent_date
    LEFT JOIN odb_max_in_window omx
        ON omx.week_start_wednesday = c.week_start_wednesday AND omx.fbbid = c.fbbid
    LEFT JOIN overdue_balance_wed ob0
        ON ob0.fbbid = c.fbbid AND ob0.snapshot_wednesday = c.week_start_wednesday
    LEFT JOIN overdue_balance_wed ob1
        ON ob1.fbbid = c.fbbid AND ob1.snapshot_wednesday = DATEADD(week, 1, c.week_start_wednesday)
    LEFT JOIN dpd_inv_window iw
        ON iw.week_start_wednesday = c.week_start_wednesday AND iw.fbbid = c.fbbid
    LEFT JOIN dpd_next_wed nw
        ON nw.week_start_wednesday = c.week_start_wednesday AND nw.fbbid = c.fbbid
),
classified AS (
    SELECT
        ce.week_start_wednesday,
        ce.week_end_tuesday,
        ce.outcome_wednesday,
        ce.fbbid,
        ce.first_delinquent_date,
        ce.odb_first_delinquent_day,
        ce.odb_max_in_window,
        ce.odb_w0,
        ce.odb_w1_raw,
        ce.dpd_w0_max,
        ce.dpd_w1,
        ce.co_w1,
        ce.has_outcome_snapshot,
        ce.has_odb_w1,
        (ROUND(ce.odb_first_delinquent_day, 2) IS DISTINCT FROM ROUND(ce.odb_max_in_window, 2)) AS odb_max_ne_first_delinq_day,
        CASE
            WHEN NOT ce.has_outcome_snapshot THEN FALSE
            WHEN COALESCE(ce.co_w1, 0) = 1 THEN FALSE
            WHEN COALESCE(ce.dpd_w1, -1) = 0 AND COALESCE(ce.co_w1, 0) = 0 THEN TRUE
            WHEN ce.has_odb_w1 AND ce.odb_w1_raw = 0 THEN TRUE
            WHEN ce.has_odb_w1 AND ce.odb_w1_raw < ce.odb_w0 THEN TRUE
            WHEN ce.has_dpd_w1 AND ce.dpd_w0_max IS NOT NULL AND ce.dpd_w1 < ce.dpd_w0_max THEN TRUE
            ELSE FALSE
        END AS is_positive_roll
    FROM cohort_enriched ce
),
customer_overdue AS (
    SELECT
        T2.EDATE,
        T1.FBBID,
        SUM(TO_DOUBLE(T1.STATUS_VALUE) * COALESCE(flu.loan_fx_rate, 1.0)) AS total_overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = T1.LOAN_KEY
    JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND T2.EDATE <= CURRENT_DATE
    JOIN inventory_dates id ON id.inv_date = T2.EDATE
    JOIN (
        SELECT DISTINCT loan_key, edate, fbbid
        FROM BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE loan_operational_status <> 'CNCL'
          AND is_charged_off = 0
    ) t3
        ON t3.loan_key = T1.loan_key
        AND t3.edate = T2.EDATE
    WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
    GROUP BY T2.EDATE, T1.FBBID
),
raw_inventory_days AS (
    SELECT
        ws.week_start_wednesday,
        DATEADD(day, 6, ws.week_start_wednesday) AS week_end_tuesday,
        DATEADD(week, 1, ws.week_start_wednesday) AS outcome_wednesday,
        ws.inv_date AS inventory_edate,
        fa.fbbid,
        fa.dpd_bucket,
        fa.dpd_days,
        fa.outstanding_principal_due,
        fa.is_charged_off,
        CASE
            WHEN fa.dpd_bucket = 0 AND fa.is_charged_off = 0 THEN '00. Bucket 0'
            WHEN fa.dpd_bucket IN (1, 2) AND fa.is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN fa.dpd_bucket IN (3, 4, 5, 6, 7, 8) AND fa.is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN fa.dpd_bucket IN (9, 10, 11, 12, 13) AND fa.is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN fa.is_charged_off = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        co.total_overdue_balance,
        IFF(fd.first_delinquent_date = fa.edate, 1, 0) AS is_first_delinquent_day_in_week,
        fo.dpd_bucket AS outcome_dpd_bucket,
        fo.dpd_days AS outcome_dpd_days,
        fo.outstanding_principal_due AS outcome_outstanding_principal_due,
        coo.total_overdue_balance AS outcome_total_overdue_balance
    FROM week_spine ws
    INNER JOIN fmd_agg fa
        ON fa.edate = ws.inv_date
    LEFT JOIN customer_overdue co
        ON co.fbbid = fa.fbbid AND co.edate = fa.edate
    LEFT JOIN first_delinquent_in_week fd
        ON fd.week_start_wednesday = ws.week_start_wednesday
        AND fd.fbbid = fa.fbbid
    LEFT JOIN fmd_agg fo
        ON fo.fbbid = fa.fbbid
        AND fo.edate = DATEADD(week, 1, ws.week_start_wednesday)
    LEFT JOIN customer_overdue coo
        ON coo.fbbid = fa.fbbid
        AND coo.edate = DATEADD(week, 1, ws.week_start_wednesday)
    WHERE fa.is_charged_off = 0
      AND fa.dpd_bucket BETWEEN 1 AND 13
),
raw_with_roll AS (
    SELECT
        r.*,
        cl.first_delinquent_date AS roll_first_delinquent_date,
        cl.odb_first_delinquent_day AS roll_odb_first_delinquent_day,
        cl.odb_max_in_window AS roll_odb_max_in_window,
        cl.odb_w0 AS roll_odb_w0,
        cl.odb_w1_raw AS roll_odb_w1_raw,
        cl.dpd_w0_max AS roll_dpd_w0_max,
        cl.dpd_w1 AS roll_dpd_w1,
        cl.co_w1 AS roll_co_w1,
        cl.has_outcome_snapshot AS roll_has_outcome_snapshot,
        cl.has_odb_w1 AS roll_has_odb_w1,
        cl.odb_max_ne_first_delinq_day AS roll_odb_max_ne_first_delinq_day,
        cl.is_positive_roll AS roll_is_positive_roll
    FROM raw_inventory_days r
    LEFT JOIN classified cl
        ON cl.week_start_wednesday = r.week_start_wednesday
        AND cl.fbbid = r.fbbid
)
SELECT
    rw.*,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM raw_with_roll rw
ORDER BY rw.week_start_wednesday, rw.fbbid, rw.inventory_edate, rw.dpd_bucket;

-- =============================================================================
-- TABLE 2: Weekly positive roll metrics (same definitions as legacy weekly_agg)
-- Built from TABLE 1 roll_* columns — one row per fbbid-week after DISTINCT.
-- =============================================================================
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.POSITIVE_ROLL_RATE_WEEKLY AS
SELECT
    week_start_wednesday,
    week_end_tuesday,
    outcome_wednesday,
    TO_VARCHAR(week_start_wednesday, 'YYYY-MM-DD')
        || ' inventory Wed-Tue, outcome '
        || TO_VARCHAR(outcome_wednesday, 'YYYY-MM-DD') AS period_label,
    COUNT(DISTINCT fbbid) AS cohort_accounts,
    COUNT(DISTINCT CASE WHEN roll_is_positive_roll THEN fbbid END) AS positive_roll_accounts,
    ROUND(SUM(roll_odb_w0), 2) AS cohort_odb_w0,
    ROUND(SUM(CASE WHEN roll_is_positive_roll THEN roll_odb_w0 END), 2) AS positive_roll_odb_w0,
    ROUND(
        COUNT(DISTINCT CASE WHEN roll_is_positive_roll THEN fbbid END) * 100.0
        / NULLIF(COUNT(DISTINCT fbbid), 0),
        4
    ) AS positive_roll_rate_unit_pct,
    ROUND(
        SUM(CASE WHEN roll_is_positive_roll THEN roll_odb_w0 END) * 100.0
        / NULLIF(SUM(roll_odb_w0), 0),
        4
    ) AS positive_roll_rate_dollar_pct,
    ROUND(SUM(roll_odb_first_delinquent_day), 2) AS cohort_odb_first_delinquent_day,
    ROUND(SUM(roll_odb_max_in_window), 2) AS cohort_odb_max_in_window,
    COUNT(DISTINCT CASE WHEN roll_odb_max_ne_first_delinq_day THEN fbbid END) AS accts_odb_max_ne_first_delinq,
    COUNT(DISTINCT CASE
        WHEN roll_odb_first_delinquent_day IS NOT NULL AND roll_odb_max_in_window IS NOT NULL THEN fbbid
    END) AS accts_with_both_first_and_max_odb,
    ROUND(
        COUNT(DISTINCT CASE WHEN roll_odb_max_ne_first_delinq_day THEN fbbid END) * 100.0
        / NULLIF(
            COUNT(DISTINCT CASE
                WHEN roll_odb_first_delinquent_day IS NOT NULL AND roll_odb_max_in_window IS NOT NULL THEN fbbid
            END),
            0
        ),
        4
    ) AS pct_accts_max_odb_ne_first_delinq_odb,
    CURRENT_TIMESTAMP() AS refreshed_at
    FROM (
        SELECT DISTINCT
            week_start_wednesday,
            week_end_tuesday,
            outcome_wednesday,
            fbbid,
            roll_is_positive_roll,
            roll_odb_w0,
            roll_odb_first_delinquent_day,
            roll_odb_max_in_window,
            roll_odb_max_ne_first_delinq_day
        FROM ANALYTICS.CREDIT.POSITIVE_ROLL_INVENTORY_RAW_FBBID_WEEK_BUCKET
    ) d
GROUP BY week_start_wednesday, week_end_tuesday, outcome_wednesday
ORDER BY week_start_wednesday;

/*
-- Optional: collapse raw to fbbid × week × dpd_bucket (uncomment to run standalone)
SELECT
    week_start_wednesday,
    week_end_tuesday,
    outcome_wednesday,
    fbbid,
    dpd_bucket,
    dpd_bucket_group,
    MIN(inventory_edate) AS first_edate_in_bucket,
    MAX(inventory_edate) AS last_edate_in_bucket,
    COUNT(*) AS days_observed_in_bucket,
    MAX(roll_is_positive_roll) AS roll_is_positive_roll_any_day
FROM ANALYTICS.CREDIT.POSITIVE_ROLL_INVENTORY_RAW_FBBID_WEEK_BUCKET
GROUP BY 1, 2, 3, 4, 5, 6;
*/
