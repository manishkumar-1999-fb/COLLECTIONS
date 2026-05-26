-- Vertical collection rate (inventory week = cohort Wednesday .. Tuesday, same fbbid base as positive roll).
-- Payments: map FUND debit by payment_planned_transmission_date falling anywhere in that Wed–Tue window.
-- PDP: FACT_DPD trigger path (same as MIS M2 snippet), same date window on payment_planned_transmission_date.
-- Past-due: KM-style DPD-drop + payment same day (T3), map by payment_event_time::DATE into the same window.
--
-- Outputs:
--   (1) ANALYTICS.CREDIT.VERTICAL_COLLECTION_RAW_FBBID_WEEK — account × week detail + past_due.
--   (2) INSERT INTO ANALYTICS.CREDIT.MIS_EFFICIENCY_METRICS_WEEKLY — one TOTAL row per inventory week (M2 slots only).
--
-- Edit pull_weeks to match Positive_roll_inventory_raw_km.sql when running both.

-- =============================================================================
-- 1) Raw: same cohort_fbbid_week as positive roll + collections in window
-- =============================================================================
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.VERTICAL_COLLECTION_RAW_FBBID_WEEK AS
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
cohort_weeks AS (
    SELECT d.edate AS week_start_wednesday
    FROM BI.INTERNAL.DATES d
    CROSS JOIN pull_weeks p
    WHERE DAYOFWEEK(d.edate) = 3
      AND d.edate >= '2024-01-01'
      AND d.edate BETWEEN p.week_from_inclusive AND p.week_through_inclusive
      AND DATEADD(week, 1, d.edate) <= CURRENT_DATE()
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
cohort_fbbid_week AS (
    SELECT DISTINCT week_start_wednesday, fbbid
    FROM cohort_accounts
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
cohort_wed_odb AS (
    SELECT
        c.week_start_wednesday,
        c.fbbid,
        COALESCE(ob.overdue_balance, 0) AS cohort_wednesday_odb
    FROM cohort_fbbid_week c
    LEFT JOIN overdue_balance_wed ob
        ON ob.fbbid = c.fbbid AND ob.snapshot_wednesday = c.week_start_wednesday
),
collections_deduped AS (
    SELECT
        a1.*,
        flu_cd.loan_fx_rate,
        ROW_NUMBER() OVER (PARTITION BY a1.payment_id ORDER BY a1.payment_event_time ASC) AS rn
    FROM BI.FINANCE.PAYMENTS_MODEL a1
    LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY a2
        ON a1.loan_key = a2.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_cd ON flu_cd.loan_key = a1.loan_key
    WHERE a2.product_type <> 'Flexpay'
      AND a2.loan_created_date = a2.edate
      AND a1.payment_status = 'FUND'
      AND a1.direction = 'D'
      AND a1.payment_planned_transmission_date IS NOT NULL
      AND a1.payment_planned_transmission_date::DATE >= '2024-01-01'
),
payments_in_inventory_week AS (
    SELECT
        cw.week_start_wednesday,
        DATEADD(day, 6, cw.week_start_wednesday) AS week_end_tuesday,
        cd.loan_key,
        fbb.fbbid,
        cd.payment_id,
        cd.payment_planned_transmission_date::DATE AS pmt_plan_date,
        TO_DOUBLE(cd.payment_components_json:payment_amount) * COALESCE(cd.loan_fx_rate, 1.0) AS payment_amount_fx
    FROM collections_deduped cd
    INNER JOIN cohort_weeks cw
        ON cd.payment_planned_transmission_date::DATE BETWEEN cw.week_start_wednesday
            AND DATEADD(day, 6, cw.week_start_wednesday)
    INNER JOIN BI.FINANCE.FINANCE_METRICS_DAILY fbb
        ON fbb.loan_key = cd.loan_key
        AND fbb.edate = cd.payment_planned_transmission_date::DATE
    LEFT JOIN first_table ft ON ft.loan_key = cd.loan_key
    WHERE cd.rn = 1
      AND fbb.product_type <> 'Flexpay'
      AND fbb.original_payment_plan_description NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
),
total_pmt_by_acct_week AS (
    SELECT
        week_start_wednesday,
        week_end_tuesday,
        fbbid,
        SUM(payment_amount_fx) AS total_collected_in_week
    FROM payments_in_inventory_week
    GROUP BY week_start_wednesday, week_end_tuesday, fbbid
),
pdp_base AS (
    SELECT
        dim.fbbid,
        dim.loan_key,
        fact.triggered_by_payment_id
    FROM BI.FINANCE.FACT_DPD fact
    INNER JOIN BI.FINANCE.DIM_DPD dim USING (hash_table_key)
    INNER JOIN BI.FINANCE.DIM_LOAN dl ON dim.loan_key = dl.loan_key
    WHERE dl.loan_created_time::DATE >= '2023-01-01'
      AND fact.triggered_by_payment_id IS NOT NULL
),
pdp_funded AS (
    SELECT DISTINCT
        pb.fbbid,
        pb.loan_key,
        pb.triggered_by_payment_id
    FROM pdp_base pb
    INNER JOIN BI.FINANCE.PAYMENTS_MODEL pm
        ON pm.related_service_id = pb.triggered_by_payment_id
        AND pm.payment_status = 'FUND'
),
pdp_deduped AS (
    SELECT
        pf.fbbid,
        pf.loan_key,
        pf.triggered_by_payment_id,
        pm.payment_planned_transmission_date,
        TO_DOUBLE(pm.payment_components_json:payment_amount) * COALESCE(flu.loan_fx_rate, 1.0) AS payment_amount_fx
    FROM pdp_funded pf
    LEFT JOIN BI.FINANCE.PAYMENTS_MODEL pm
        ON pm.related_service_id = pf.triggered_by_payment_id
        AND pm.payment_status = 'FUND'
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.loan_key = pm.loan_key
    WHERE pm.payment_planned_transmission_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pf.triggered_by_payment_id, pf.loan_key, pm.payment_planned_transmission_date
        ORDER BY pm.payment_planned_transmission_date
    ) = 1
),
pdp_in_inventory_week AS (
    SELECT
        cw.week_start_wednesday,
        DATEADD(day, 6, cw.week_start_wednesday) AS week_end_tuesday,
        pd.fbbid,
        SUM(pd.payment_amount_fx) AS pdp_collected_in_week
    FROM pdp_deduped pd
    INNER JOIN cohort_weeks cw
        ON pd.payment_planned_transmission_date::DATE BETWEEN cw.week_start_wednesday
            AND DATEADD(day, 6, cw.week_start_wednesday)
    GROUP BY cw.week_start_wednesday, week_end_tuesday, pd.fbbid
),
fmd_agg_payments AS (
    SELECT
        fmd.loan_key,
        fmd.fbbid,
        fmd.edate,
        MAX(fmd.dpd_bucket) AS dpd_bucket,
        MAX(fmd.is_charged_off) AS is_charged_off_any
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON ft.loan_key = fmd.loan_key
    WHERE fmd.edate >= '2024-01-01'
      AND fmd.product_type <> 'Flexpay'
      AND fmd.original_payment_plan_description NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
    GROUP BY fmd.loan_key, fmd.fbbid, fmd.edate
),
fmd_with_lag AS (
    SELECT
        *,
        ZEROIFNULL(LAG(dpd_bucket) OVER (PARTITION BY loan_key ORDER BY edate)) AS prev_dpd_bucket
    FROM fmd_agg_payments
),
fmd_drop_days AS (
    SELECT *
    FROM fmd_with_lag
    WHERE (dpd_bucket < prev_dpd_bucket)
       OR (prev_dpd_bucket > 0)
),
past_due_payment_rows AS (
    SELECT DISTINCT
        t2.fbbid,
        t2.loan_key,
        t2.edate AS dpd_drop_date,
        pm_1.payment_event_time::DATE AS payment_event_date,
        TO_DOUBLE(pm_1.payment_components_json:payment_amount) * COALESCE(flu_pm.loan_fx_rate, 1.0) AS payment_amount_fx
    FROM BI.FINANCE.PAYMENTS_MODEL pm_1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_pm ON flu_pm.loan_key = pm_1.loan_key
    INNER JOIN fmd_drop_days t2
        ON pm_1.loan_key = t2.loan_key
        AND CASE
            WHEN pm_1.payment_method_type = 'CC' THEN pm_1.payment_status = 'FUND'
            ELSE pm_1.payment_status IN ('AUTH', 'TRNS')
        END
        AND pm_1.payment_event_time::DATE = t2.edate
        AND pm_1.direction = 'D'
    WHERE EXISTS (
        SELECT 1
        FROM BI.FINANCE.PAYMENTS_MODEL pm_2
        WHERE pm_1.payment_id = pm_2.payment_id
          AND pm_2.payment_status = 'FUND'
    )
),
past_due_in_inventory_week AS (
    SELECT
        cw.week_start_wednesday,
        DATEADD(day, 6, cw.week_start_wednesday) AS week_end_tuesday,
        pdp.fbbid,
        SUM(pdp.payment_amount_fx) AS past_due_collected_in_week
    FROM past_due_payment_rows pdp
    INNER JOIN cohort_weeks cw
        ON pdp.payment_event_date BETWEEN cw.week_start_wednesday
            AND DATEADD(day, 6, cw.week_start_wednesday)
    GROUP BY cw.week_start_wednesday, week_end_tuesday, pdp.fbbid
),
raw_base AS (
    SELECT
        o.week_start_wednesday,
        DATEADD(day, 6, o.week_start_wednesday) AS week_end_tuesday,
        o.fbbid,
        o.cohort_wednesday_odb,
        COALESCE(tp.total_collected_in_week, 0) AS total_collected_in_week,
        COALESCE(pdp.pdp_collected_in_week, 0) AS pdp_collected_in_week,
        COALESCE(pst.past_due_collected_in_week, 0) AS past_due_collected_in_week
    FROM cohort_wed_odb o
    LEFT JOIN total_pmt_by_acct_week tp
        ON tp.week_start_wednesday = o.week_start_wednesday
        AND tp.fbbid = o.fbbid
    LEFT JOIN pdp_in_inventory_week pdp
        ON pdp.week_start_wednesday = o.week_start_wednesday
        AND pdp.fbbid = o.fbbid
    LEFT JOIN past_due_in_inventory_week pst
        ON pst.week_start_wednesday = o.week_start_wednesday
        AND pst.fbbid = o.fbbid
)
SELECT
    r.*,
    ROUND(
        r.total_collected_in_week * 100.0 / NULLIF(r.cohort_wednesday_odb, 0),
        4
    ) AS pct_vertical_total_vs_cohort_wed_odb,
    ROUND(
        r.pdp_collected_in_week * 100.0 / NULLIF(r.cohort_wednesday_odb, 0),
        4
    ) AS pct_vertical_pdp_vs_cohort_wed_odb,
    ROUND(
        r.past_due_collected_in_week * 100.0 / NULLIF(r.cohort_wednesday_odb, 0),
        4
    ) AS pct_vertical_past_due_vs_cohort_wed_odb,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM raw_base r
ORDER BY r.week_start_wednesday, r.fbbid;

-- =============================================================================
-- 2) MIS row: M2-style columns; week_end_date = inventory Tuesday (week label)
--    m2_bow_odb / m2_new_inflow_odb set to 0; collectible = sum cohort Wed ODB
--    Past-due is NOT a separate MIS column — see raw table past_due_collected_in_week
-- =============================================================================
-- Optional: avoid duplicate metric rows on re-run
-- DELETE FROM ANALYTICS.CREDIT.MIS_EFFICIENCY_METRICS_WEEKLY WHERE metric_id = 'M2_VERTICAL_COLLECTION_INVENTORY_WEEK';

INSERT INTO ANALYTICS.CREDIT.MIS_EFFICIENCY_METRICS_WEEKLY (
    metric_id,
    week_end_date,
    week_start_date,
    period_label,
    dimension,
    maturity_status,
    m1_total_cust,
    m1_positive_roll_cust,
    m1_cured_cust,
    m1_improved_cust,
    m1_stayed_cust,
    m1_worsened_cust,
    m1_charged_off_cust,
    m1_total_odb,
    m1_positive_roll_odb,
    m1_cured_odb,
    m1_improved_odb,
    m1_total_os,
    m1_positive_roll_os,
    m1_pct_positive_roll_odb,
    m1_pct_positive_roll_cust,
    m1_pct_positive_roll_os,
    m1_pct_cured_odb,
    m1_pct_improved_odb,
    m2_bow_odb,
    m2_new_inflow_odb,
    m2_collectible_odb,
    m2_total_collected,
    m2_pdp_collected,
    m2_pct_total_collected,
    m2_pct_pdp_collected,
    m3_cohort_size,
    m3_entry_odb,
    m3_cum_collected_w2,
    m3_cum_collected_w4,
    m3_cum_collected_w6,
    m3_cum_collected_w8,
    m3_cum_collected_w10,
    m3_cum_collected_w12,
    m3_pct_recovered_w2,
    m3_pct_recovered_w4,
    m3_pct_recovered_w6,
    m3_pct_recovered_w8,
    m3_pct_recovered_w10,
    m3_pct_recovered_w12,
    refreshed_at
)
WITH pull_weeks AS (
    SELECT
        '2024-01-01'::DATE AS week_from_inclusive,
        CURRENT_DATE()::DATE AS week_through_inclusive
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
weekly_from_raw AS (
    SELECT
        week_end_tuesday AS week_end_date,
        week_start_wednesday AS week_start_date,
        ROUND(SUM(cohort_wednesday_odb), 2) AS m2_collectible_odb,
        ROUND(SUM(total_collected_in_week), 2) AS m2_total_collected,
        ROUND(SUM(pdp_collected_in_week), 2) AS m2_pdp_collected
    FROM ANALYTICS.CREDIT.VERTICAL_COLLECTION_RAW_FBBID_WEEK
    GROUP BY week_start_wednesday, week_end_tuesday
)
SELECT
    'M2_VERTICAL_COLLECTION_INVENTORY_WEEK' AS metric_id,
    w.week_end_date,
    w.week_start_date,
    TO_VARCHAR(w.week_end_date, 'Mon DD') AS period_label,
    'TOTAL' AS dimension,
    NULL::VARCHAR AS maturity_status,
    NULL::INT,
    NULL::INT,
    NULL::INT,
    NULL::INT,
    NULL::INT,
    NULL::INT,
    NULL::INT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    0::FLOAT AS m2_bow_odb,
    0::FLOAT AS m2_new_inflow_odb,
    w.m2_collectible_odb,
    w.m2_total_collected,
    w.m2_pdp_collected,
    ROUND(w.m2_total_collected * 100.0 / NULLIF(w.m2_collectible_odb, 0), 2) AS m2_pct_total_collected,
    ROUND(w.m2_pdp_collected * 100.0 / NULLIF(w.m2_collectible_odb, 0), 2) AS m2_pct_pdp_collected,
    NULL::INT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    NULL::FLOAT,
    CURRENT_TIMESTAMP()
FROM weekly_from_raw w;
