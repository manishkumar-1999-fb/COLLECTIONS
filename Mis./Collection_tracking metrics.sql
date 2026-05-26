CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MIS_EFFICIENCY_METRICS_WEEKLY AS
WITH date_bounds AS (
    SELECT
        DATEADD(week,-14,DATE_TRUNC('week',CURRENT_DATE()::DATE+4)::DATE+2) AS min_date,
        DATEADD(week,-20,DATE_TRUNC('week',CURRENT_DATE()::DATE+4)::DATE+2) AS m3_min_date,
        CURRENT_DATE() AS max_date,
        DATEADD(week,-13,DATE_TRUNC('week',CURRENT_DATE()::DATE+4)::DATE+2) AS report_start
),
first_table AS (
    SELECT loan_key, loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY WHERE edate >= '2024-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),
fmd_daily AS (
    SELECT fmd.fbbid, fmd.loan_key, fmd.edate,
        DATE_TRUNC('week',fmd.edate::DATE+4)::DATE+2 AS week_end_date,
        fmd.dpd_bucket, fmd.is_charged_off,
        fmd.outstanding_principal_due*COALESCE(flu.loan_fx_rate,1.0) AS os_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key=ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY=fmd.LOAN_KEY
    CROSS JOIN date_bounds db
    WHERE fmd.PRODUCT_TYPE<>'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status<>'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.edate>=db.m3_min_date AND fmd.is_charged_off=0 AND fmd.dpd_bucket BETWEEN 1 AND 13
),
overdue_balance AS (
    SELECT T2.EDATE AS week_end_date, T1.FBBID,
        SUM(TO_DOUBLE(T1.STATUS_VALUE)*COALESCE(flu.loan_fx_rate,1.0)) AS overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY=T1.LOAN_KEY
    JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND DAYOFWEEK(T2.EDATE)=3 AND T2.EDATE<=CURRENT_DATE
    JOIN (SELECT DISTINCT fmd2.loan_key,fmd2.edate FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd2
          WHERE fmd2.loan_operational_status<>'CNCL' AND fmd2.is_charged_off=0) t3
        ON t3.loan_key=T1.loan_key AND t3.edate=T2.EDATE
    WHERE T1.STATUS_NAME='APD_LOAN_TOTAL_AMOUNT'
    GROUP BY T2.EDATE,T1.FBBID
),
m1_entry AS (
    SELECT week_end_date, fbbid,
        CASE MAX(dpd_bucket) WHEN 1 THEN 'DPD 1-2' WHEN 2 THEN 'DPD 1-2'
            WHEN 3 THEN 'DPD 3-8' WHEN 4 THEN 'DPD 3-8' WHEN 5 THEN 'DPD 3-8'
            WHEN 6 THEN 'DPD 3-8' WHEN 7 THEN 'DPD 3-8' WHEN 8 THEN 'DPD 3-8'
            WHEN 9 THEN 'DPD 9-13' WHEN 10 THEN 'DPD 9-13' WHEN 11 THEN 'DPD 9-13'
            WHEN 12 THEN 'DPD 9-13' WHEN 13 THEN 'DPD 9-13' END AS peak_bucket,
        MAX(os_principal) AS os_at_peak
    FROM fmd_daily GROUP BY week_end_date,fbbid HAVING peak_bucket IS NOT NULL
),
m1_eow AS (
    SELECT DATE_TRUNC('week',edate::DATE+4)::DATE+2 AS week_end_date, fbbid,
        CASE WHEN MAX(is_charged_off)=1 THEN 99 WHEN MAX(dpd_bucket)=0 THEN 0
             WHEN MAX(dpd_bucket) IN (1,2) THEN 1
             WHEN MAX(dpd_bucket) BETWEEN 3 AND 8 THEN 2
             WHEN MAX(dpd_bucket) BETWEEN 9 AND 13 THEN 3 END AS eow_bucket_num
    FROM (SELECT fmd.fbbid,fmd.edate,fmd.dpd_bucket,fmd.is_charged_off
          FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
          LEFT JOIN first_table ft ON fmd.loan_key=ft.loan_key
          CROSS JOIN date_bounds db
          WHERE DAYOFWEEK(fmd.edate)=3 AND fmd.edate>=db.min_date
            AND fmd.PRODUCT_TYPE<>'Flexpay'
            AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
            AND (ft.loan_operational_status<>'CNCL' OR ft.loan_operational_status IS NULL))
    GROUP BY week_end_date,fbbid
),
m1_cure AS (
    SELECT DATE_TRUNC('week',edate::DATE+4)::DATE+2 AS week_end_date, fbbid,
        MAX(CASE WHEN dpd_bucket=0 AND is_charged_off=0 THEN 1 ELSE 0 END) AS is_cured_thu
    FROM (SELECT fmd.fbbid,fmd.edate,fmd.dpd_bucket,fmd.is_charged_off
          FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
          LEFT JOIN first_table ft ON fmd.loan_key=ft.loan_key
          CROSS JOIN date_bounds db
          WHERE fmd.edate=(DATE_TRUNC('week',fmd.edate::DATE+4)::DATE+2)+1
            AND DAYOFWEEK(fmd.edate)=4 AND fmd.edate>=db.min_date
            AND fmd.PRODUCT_TYPE<>'Flexpay'
            AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
            AND (ft.loan_operational_status<>'CNCL' OR ft.loan_operational_status IS NULL))
    GROUP BY week_end_date,fbbid
),
m1_odb AS (
    SELECT e.week_end_date,e.fbbid,SUM(ob.overdue_balance) AS odb_at_peak
    FROM m1_entry e
    LEFT JOIN overdue_balance ob ON ob.fbbid=e.fbbid AND ob.week_end_date=e.week_end_date
    GROUP BY e.week_end_date,e.fbbid
),
m1_classified AS (
    SELECT e.week_end_date, e.week_end_date-6 AS week_start_date, e.fbbid, e.peak_bucket,
        e.os_at_peak, COALESCE(o.odb_at_peak,0) AS odb_at_peak,
        eow.eow_bucket_num, COALESCE(c.is_cured_thu,0) AS is_cured_thu,
        CASE WHEN COALESCE(c.is_cured_thu,0)=1 THEN 'Cured'
             WHEN eow.eow_bucket_num < CASE e.peak_bucket WHEN 'DPD 1-2' THEN 1 WHEN 'DPD 3-8' THEN 2 WHEN 'DPD 9-13' THEN 3 END THEN 'Improved'
             WHEN eow.eow_bucket_num = CASE e.peak_bucket WHEN 'DPD 1-2' THEN 1 WHEN 'DPD 3-8' THEN 2 WHEN 'DPD 9-13' THEN 3 END THEN 'Stayed'
             WHEN eow.eow_bucket_num=99 THEN 'Charged Off' ELSE 'Worsened' END AS outcome
    FROM m1_entry e
    LEFT JOIN m1_eow eow ON eow.fbbid=e.fbbid AND eow.week_end_date=e.week_end_date
    LEFT JOIN m1_cure c ON c.fbbid=e.fbbid AND c.week_end_date=e.week_end_date
    LEFT JOIN m1_odb o ON o.fbbid=e.fbbid AND o.week_end_date=e.week_end_date
),
m1_bucket_agg AS (
    SELECT week_end_date, week_start_date, peak_bucket AS dimension,
        COUNT(DISTINCT fbbid) AS m1_total_cust,
        COUNT(DISTINCT CASE WHEN outcome IN ('Cured','Improved') THEN fbbid END) AS m1_positive_roll_cust,
        COUNT(DISTINCT CASE WHEN outcome='Cured' THEN fbbid END) AS m1_cured_cust,
        COUNT(DISTINCT CASE WHEN outcome='Improved' THEN fbbid END) AS m1_improved_cust,
        COUNT(DISTINCT CASE WHEN outcome='Stayed' THEN fbbid END) AS m1_stayed_cust,
        COUNT(DISTINCT CASE WHEN outcome='Worsened' THEN fbbid END) AS m1_worsened_cust,
        COUNT(DISTINCT CASE WHEN outcome='Charged Off' THEN fbbid END) AS m1_charged_off_cust,
        ROUND(SUM(odb_at_peak),2) AS m1_total_odb,
        ROUND(SUM(CASE WHEN outcome IN ('Cured','Improved') THEN odb_at_peak END),2) AS m1_positive_roll_odb,
        ROUND(SUM(CASE WHEN outcome='Cured' THEN odb_at_peak END),2) AS m1_cured_odb,
        ROUND(SUM(CASE WHEN outcome='Improved' THEN odb_at_peak END),2) AS m1_improved_odb,
        ROUND(SUM(os_at_peak),2) AS m1_total_os,
        ROUND(SUM(CASE WHEN outcome IN ('Cured','Improved') THEN os_at_peak END),2) AS m1_positive_roll_os,
        ROUND(m1_positive_roll_odb*100.0/NULLIF(m1_total_odb,0),2) AS m1_pct_positive_roll_odb,
        ROUND(m1_positive_roll_cust*100.0/NULLIF(m1_total_cust,0),2) AS m1_pct_positive_roll_cust,
        ROUND(m1_positive_roll_os*100.0/NULLIF(m1_total_os,0),2) AS m1_pct_positive_roll_os,
        ROUND(m1_cured_odb*100.0/NULLIF(m1_total_odb,0),2) AS m1_pct_cured_odb,
        ROUND(m1_improved_odb*100.0/NULLIF(m1_total_odb,0),2) AS m1_pct_improved_odb
    FROM m1_classified GROUP BY week_end_date,week_start_date,peak_bucket
),
m1_total_agg AS (
    SELECT week_end_date, week_start_date, 'TOTAL' AS dimension,
        SUM(m1_total_cust) AS m1_total_cust, SUM(m1_positive_roll_cust) AS m1_positive_roll_cust,
        SUM(m1_cured_cust) AS m1_cured_cust, SUM(m1_improved_cust) AS m1_improved_cust,
        SUM(m1_stayed_cust) AS m1_stayed_cust, SUM(m1_worsened_cust) AS m1_worsened_cust,
        SUM(m1_charged_off_cust) AS m1_charged_off_cust,
        ROUND(SUM(m1_total_odb),2) AS m1_total_odb, ROUND(SUM(m1_positive_roll_odb),2) AS m1_positive_roll_odb,
        ROUND(SUM(m1_cured_odb),2) AS m1_cured_odb, ROUND(SUM(m1_improved_odb),2) AS m1_improved_odb,
        ROUND(SUM(m1_total_os),2) AS m1_total_os, ROUND(SUM(m1_positive_roll_os),2) AS m1_positive_roll_os,
        ROUND(SUM(m1_positive_roll_odb)*100.0/NULLIF(SUM(m1_total_odb),0),2) AS m1_pct_positive_roll_odb,
        ROUND(SUM(m1_positive_roll_cust)*100.0/NULLIF(SUM(m1_total_cust),0),2) AS m1_pct_positive_roll_cust,
        ROUND(SUM(m1_positive_roll_os)*100.0/NULLIF(SUM(m1_total_os),0),2) AS m1_pct_positive_roll_os,
        ROUND(SUM(m1_cured_odb)*100.0/NULLIF(SUM(m1_total_odb),0),2) AS m1_pct_cured_odb,
        ROUND(SUM(m1_improved_odb)*100.0/NULLIF(SUM(m1_total_odb),0),2) AS m1_pct_improved_odb
    FROM m1_bucket_agg GROUP BY week_end_date,week_start_date
)
SELECT 'M1_POSITIVE_ROLL' AS metric_id,
    week_end_date, week_start_date,
    TO_VARCHAR(week_end_date,'Mon DD') AS period_label,
    dimension, NULL::VARCHAR AS maturity_status,
    m1_total_cust,m1_positive_roll_cust,m1_cured_cust,m1_improved_cust,
    m1_stayed_cust,m1_worsened_cust,m1_charged_off_cust,
    m1_total_odb,m1_positive_roll_odb,m1_cured_odb,m1_improved_odb,
    m1_total_os,m1_positive_roll_os,
    m1_pct_positive_roll_odb,m1_pct_positive_roll_cust,m1_pct_positive_roll_os,
    m1_pct_cured_odb,m1_pct_improved_odb,
    NULL::FLOAT AS m2_bow_odb,NULL::FLOAT AS m2_new_inflow_odb,NULL::FLOAT AS m2_collectible_odb,
    NULL::FLOAT AS m2_total_collected,NULL::FLOAT AS m2_pdp_collected,
    NULL::FLOAT AS m2_pct_total_collected,NULL::FLOAT AS m2_pct_pdp_collected,
    NULL::INT AS m3_cohort_size,NULL::FLOAT AS m3_entry_odb,
    NULL::FLOAT AS m3_cum_collected_w2,NULL::FLOAT AS m3_cum_collected_w4,
    NULL::FLOAT AS m3_cum_collected_w6,NULL::FLOAT AS m3_cum_collected_w8,
    NULL::FLOAT AS m3_cum_collected_w10,NULL::FLOAT AS m3_cum_collected_w12,
    NULL::FLOAT AS m3_pct_recovered_w2,NULL::FLOAT AS m3_pct_recovered_w4,
    NULL::FLOAT AS m3_pct_recovered_w6,NULL::FLOAT AS m3_pct_recovered_w8,
    NULL::FLOAT AS m3_pct_recovered_w10,NULL::FLOAT AS m3_pct_recovered_w12,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM (SELECT * FROM m1_bucket_agg UNION ALL SELECT * FROM m1_total_agg)
WHERE week_end_date>=(SELECT report_start FROM date_bounds)
ORDER BY week_end_date, dimension;
