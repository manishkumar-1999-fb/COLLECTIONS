-------------------------------------------- Pre-CO Loan Level --------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_metrics AS
WITH base AS (
    SELECT
        loan_key,
        fbbid,
        week_end_date,
        week_end_date - 6 AS week_start_date,
        principal_paid,
        total_paid,
        CASE WHEN dpd_days_corrected is NULL AND is_charged_off IS NULL THEN 0
        ELSE dpd_days_corrected
        END AS dpd_days_corrected,
        
        CASE WHEN lag_dpd_days_corrected is NULL AND lag_is_charged_off IS NULL THEN 0
        ELSE lag_dpd_days_corrected
        END AS lag_dpd_days_corrected,
        
        is_charged_off,
        lag_is_charged_off,
        
        CASE
            WHEN dpd_days_corrected = 0 and is_charged_off = 0 THEN '00. Bucket 0'
            -- WHEN dpd_days_corrected IS NULL AND is_charged_off IS NULL THEN '00. Bucket 0'
            WHEN dpd_days_corrected BETWEEN 1 AND 14 and is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected BETWEEN 15 AND 56 and is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected BETWEEN 57 AND 91 and is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected <= 98 and is_charged_off = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        CASE 
            WHEN lag_dpd_days_corrected = 0 and lag_is_charged_off = 0 THEN '00. Bucket 0'
            when lag_dpd_days_corrected between 1 and 14 and lag_is_charged_off = 0 then '01. Bucket 1-2'
            when lag_dpd_days_corrected between 15 and 56 and lag_is_charged_off = 0 then '02. Bucket 3-8'
            when lag_dpd_days_corrected between 57 and 91 and lag_is_charged_off = 0 then '03. Bucket 9-13'
            WHEN lag_dpd_days_corrected <= 98 or lag_is_charged_off = 1 THEN '04. CHOF'
        end as prev_dpd_bucket_group,
        outstanding_principal_due,
        lag_outstanding_principal_due,
        os_0_90,
        os_1_90,
        os_p_1_90,
    FROM analytics.credit.loan_level_data_pb
),

total_overdue AS (
SELECT
    T2.EDATE AS week_end_date,
    T1.FBBID,
    T1.LOAN_KEY,
    SUM(T1.STATUS_VALUE) AS total_overdue_balance
FROM BI.FINANCE.LOAN_STATUSES T1
JOIN BI.INTERNAL.DATES T2
    ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
    AND DAYOFWEEK(T2.EDATE) = 3
    AND T2.EDATE <= CURRENT_DATE
JOIN (
    SELECT DISTINCT loan_key, edate
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE loan_operational_status <> 'CNCL'
    AND is_charged_off = 0
  ) t3
    ON t3.loan_key = T1.loan_key
    AND t3.edate = T2.EDATE
WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
GROUP BY 1,2,3
),

collections AS (
  WITH deduped AS (
    SELECT 
        A1.*,
        ROW_NUMBER() OVER (PARTITION BY PAYMENT_ID ORDER BY PAYMENT_EVENT_TIME ASC) AS rn
    FROM BI.FINANCE.PAYMENTS_STATUSES_UNITED A1
    LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY A2 
        ON A1.LOAN_KEY = A2.LOAN_KEY
    WHERE
        PRODUCT_TYPE <> 'Flexpay'
        AND LOAN_CREATED_DATE = EDATE
        AND PAYMENT_STATUS = 'FUND'
        AND DIRECTION = 'D'
        AND PAYMENT_EVENT_TIME::DATE >= '2020-12-01'
  ),
  base_collected AS (
    SELECT 
        DATE_TRUNC('WEEK', PAYMENT_EVENT_TIME::DATE + 4)::DATE + 2 AS collection_week,
        FBBID,
        LOAN_KEY,
        SUM(PAYMENT_PRINCIPAL_AMOUNT) AS total_collections
    FROM deduped
    WHERE rn = 1
    GROUP BY 1,2,3
  )
  SELECT
      bc.FBBID,
      bc.LOAN_KEY,
      bc.collection_week AS week_end_date,
      bc.total_collections
  FROM base_collected bc
),

combined AS (
    SELECT
        b.*,
        odb.total_overdue_balance,
        c.total_collections,

        CASE 
            -- From 1-2
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected = 0 and is_charged_off = 0 THEN 'Cure_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected BETWEEN 1 AND 14 and is_charged_off = 0 THEN 'Stay_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected BETWEEN 15 AND 98 THEN 'Worsen_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND is_charged_off = 1 and lag_is_charged_off = 0 THEN 'Worsen_1_2'

            -- From 3-8
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected = 0 and is_charged_off = 0 THEN 'Cure_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected BETWEEN 1 AND 14 and is_charged_off = 0 THEN 'Improve_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected BETWEEN 15 AND 56 and is_charged_off = 0 THEN 'Stay_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected BETWEEN 57 AND 98 THEN 'Worsen_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND is_charged_off = 1 and lag_is_charged_off = 0 THEN 'Worsen_3_8'


            -- From 9-13
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected = 0 and is_charged_off = 0 THEN 'Cure_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected BETWEEN 1 AND 56 and is_charged_off = 0 THEN 'Improve_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected BETWEEN 57 AND 91 and is_charged_off = 0 THEN 'Stay_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected BETWEEN 92 AND 98 THEN 'Worsen_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND is_charged_off = 1 and lag_is_charged_off = 0 THEN 'Worsen_9_13'

            ELSE NULL
        END
        AS transition_type

    FROM base b
    LEFT JOIN total_overdue odb
        ON b.fbbid = odb.fbbid AND b.loan_key = odb.loan_key AND b.week_end_date = odb.week_end_date
    LEFT JOIN collections c
        ON b.fbbid = c.fbbid AND b.loan_key = c.loan_key AND b.week_end_date = c.week_end_date
),

aggregated as (
SELECT
    week_start_date,
    week_end_date,

    SUM(os_1_90) as pre_co_total_os,
    SUM(os_0_90) as total_os_due,

    -- Loan counts
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN loan_key END) AS num_loans_0,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN loan_key END) AS num_loans_1_2,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN loan_key END) AS num_loans_3_8,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN loan_key END) AS num_loans_9_13,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' THEN loan_key END) AS num_loans_co,

    -- Lagged Loan counts
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '00. Bucket 0' THEN loan_key END) AS lag_num_loans_0,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '01. Bucket 1-2' THEN loan_key END) AS lag_num_loans_1_2,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '02. Bucket 3-8' THEN loan_key END) AS lag_num_loans_3_8,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '03. Bucket 9-13' THEN loan_key END) AS lag_num_loans_9_13,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '04. CHOF' THEN loan_key END) AS lag_num_loans_co,

    -- Outstanding principal
    SUM(CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN outstanding_principal_due END) AS sum_os_0,
    SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN outstanding_principal_due END) AS sum_os_1_2,
    SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN outstanding_principal_due END) AS sum_os_3_8,
    SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN outstanding_principal_due END) AS sum_os_9_13,
    SUM(CASE WHEN dpd_bucket_group = '04. CHOF' THEN outstanding_principal_due END) AS sum_os_co,

    --Lagged Outstanding principal
    SUM(CASE WHEN prev_dpd_bucket_group = '00. Bucket 0' THEN lag_outstanding_principal_due END) AS lag_sum_os_0,
    SUM(CASE WHEN prev_dpd_bucket_group = '01. Bucket 1-2' THEN lag_outstanding_principal_due END) AS lag_sum_os_1_2,
    SUM(CASE WHEN prev_dpd_bucket_group = '02. Bucket 3-8' THEN lag_outstanding_principal_due END) AS lag_sum_os_3_8,
    SUM(CASE WHEN prev_dpd_bucket_group = '03. Bucket 9-13' THEN lag_outstanding_principal_due END) AS lag_sum_os_9_13,
    SUM(CASE WHEN prev_dpd_bucket_group = '04. CHOF' THEN lag_outstanding_principal_due END) AS lag_sum_os_co,


    -- Overdue balance
    SUM(CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN total_overdue_balance END) AS sum_odb_0,
    SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN total_overdue_balance END) AS sum_odb_1_2,
    SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN total_overdue_balance END) AS sum_odb_3_8,
    SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN total_overdue_balance END) AS sum_odb_9_13,
    SUM(CASE WHEN dpd_bucket_group = '04. CHOF' THEN total_overdue_balance END) AS sum_odb_co,

    -- Customer counts
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN fbbid END) AS num_cust_0,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS num_cust_1_2,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS num_cust_3_8,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS num_cust_9_13,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' THEN fbbid END) AS num_cust_co,

    -- Lagged Customer counts
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '00. Bucket 0' THEN fbbid END) AS lag_num_cust_0,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS lag_num_cust_1_2,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS lag_num_cust_3_8,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS lag_num_cust_9_13,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '04. CHOF' THEN fbbid END) AS lag_num_cust_co,


    -- Collections
    SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN total_collections END) AS collected_1_2,
    SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN total_collections END) AS collected_3_8,
    SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN total_collections END) AS collected_9_13,

    -- Roll rates from each previous group
    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_1_2' THEN loan_key END) AS num_cured_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_1_2' THEN loan_key END) AS num_improved_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_1_2' THEN loan_key END) AS num_stayed_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_1_2' THEN loan_key END) AS num_worsened_1_2,

    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_3_8' THEN loan_key END) AS num_cured_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_3_8' THEN loan_key END) AS num_improved_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_3_8' THEN loan_key END) AS num_stayed_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_3_8' THEN loan_key END) AS num_worsened_3_8,

    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_9_13' THEN loan_key END) AS num_cured_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_9_13' THEN loan_key END) AS num_improved_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_9_13' THEN loan_key END) AS num_stayed_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_9_13' THEN loan_key END) AS num_worsened_9_13,

    SUM(CASE WHEN transition_type = 'Cure_1_2' THEN lag_outstanding_principal_due END) AS os_cured_1_2,
    SUM(CASE WHEN transition_type = 'Improve_1_2' THEN lag_outstanding_principal_due END) AS os_improved_1_2,
    SUM(CASE WHEN transition_type = 'Stay_1_2' THEN lag_outstanding_principal_due END) AS os_stayed_1_2,
    SUM(CASE WHEN transition_type = 'Worsen_1_2' THEN lag_outstanding_principal_due END) AS os_worsened_1_2,

    SUM(CASE WHEN transition_type = 'Cure_3_8' THEN lag_outstanding_principal_due END) AS os_cured_3_8,
    SUM(CASE WHEN transition_type = 'Improve_3_8' THEN lag_outstanding_principal_due END) AS os_improved_3_8,
    SUM(CASE WHEN transition_type = 'Stay_3_8' THEN lag_outstanding_principal_due END) AS os_stayed_3_8,
    SUM(CASE WHEN transition_type = 'Worsen_3_8' THEN lag_outstanding_principal_due END) AS os_worsened_3_8,

    SUM(CASE WHEN transition_type = 'Cure_9_13' THEN lag_outstanding_principal_due END) AS os_cured_9_13,
    SUM(CASE WHEN transition_type = 'Improve_9_13' THEN lag_outstanding_principal_due END) AS os_improved_9_13,
    SUM(CASE WHEN transition_type = 'Stay_9_13' THEN lag_outstanding_principal_due END) AS os_stayed_9_13,
    SUM(CASE WHEN transition_type = 'Worsen_9_13' THEN lag_outstanding_principal_due END) AS os_worsened_9_13,

FROM combined
WHERE dpd_bucket_group IS NOT NULL
GROUP BY week_start_date, week_end_date
ORDER BY week_start_date, week_end_date
)

SELECT 
    a.week_start_date,
    a.week_end_date,
    num_cust_1_2,
    num_cust_3_8,
    num_cust_9_13,
    num_loans_1_2,
    num_loans_3_8,
    num_loans_9_13,
    pre_co_total_os,
    total_os_due,
    sum_os_1_2,
    sum_os_3_8,
    sum_os_9_13,
    sum_odb_1_2,
    sum_odb_3_8,
    sum_odb_9_13,
    collected_1_2,
    collected_3_8,
    collected_9_13,
    
    -------------------- DPD 1–2 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_1_2 / NULLIF(lag_num_loans_1_2, 0),0)
    AS became_current_1_2_pct_num,
    
    COALESCE(num_stayed_1_2 / NULLIF(lag_num_loans_1_2, 0),0)
    AS stayed_1_2_pct_num,
    
    COALESCE(num_worsened_1_2 / NULLIF(lag_num_loans_1_2, 0),0)
    AS worsened_1_2_pct_num,
    
    COALESCE(num_improved_1_2 / NULLIF(lag_num_loans_1_2, 0),0)
    AS improved_1_2_pct_num,
    
    -- OS metrics
    -- COALESCE(os_cured_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    -- AS became_current_1_2_pct_os,
    
    -- COALESCE(os_stayed_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    -- AS stayed_1_2_pct_os,
    
    -- COALESCE(os_worsened_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    -- AS worsened_1_2_pct_os,
    
    -- COALESCE(os_improved_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    -- AS improved_1_2_pct_os,
    
    
    -------------------- DPD 3–8 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_3_8 / NULLIF(lag_num_loans_3_8, 0),0)
    AS became_current_3_8_pct_num,
    
    COALESCE(num_stayed_3_8 / NULLIF(lag_num_loans_3_8, 0),0)
    AS stayed_3_8_pct_num,
    
    COALESCE(num_worsened_3_8 / NULLIF(lag_num_loans_3_8, 0),0)
    AS worsened_3_8_pct_num,
    
    COALESCE(num_improved_3_8 / NULLIF(lag_num_loans_3_8, 0),0)
    AS improved_3_8_pct_num,
    
    -- OS metrics
    -- COALESCE(os_cured_3_8 / NULLIF(lag_sum_os_3_8, 0),0)
    -- AS became_current_3_8_pct_os,
    
    -- COALESCE(os_stayed_3_8 / NULLIF(lag_sum_os_3_8, 0),0)
    -- AS stayed_3_8_pct_os,
    
    -- COALESCE(os_worsened_3_8 / NULLIF(lag_sum_os_3_8, 0),0) 
    -- AS worsened_3_8_pct_os,
    
    -- COALESCE(os_improved_3_8 / NULLIF(lag_sum_os_3_8, 0),0) 
    -- AS improved_3_8_pct_os,
    
    
    ---------------------- DPD 9–13 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_9_13 / NULLIF(lag_num_loans_9_13, 0),
    0) AS became_current_9_13_pct_num,
    
    COALESCE(num_stayed_9_13 / NULLIF(lag_num_loans_9_13, 0),
    0) AS stayed_9_13_pct_num,
    
    COALESCE(num_worsened_9_13 / NULLIF(lag_num_loans_9_13, 0),
    0) AS worsened_9_13_pct_num,
    
    COALESCE(num_improved_9_13 / NULLIF(lag_num_loans_9_13, 0),
    0) AS improved_9_13_pct_num,
    
    -- OS metrics
    -- COALESCE(os_cured_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    -- AS became_current_9_13_pct_os,
    
    -- COALESCE(os_stayed_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    -- AS stayed_9_13_pct_os,
    
    -- COALESCE(os_worsened_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    -- AS worsened_9_13_pct_os,
    
    -- COALESCE(os_improved_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    -- AS improved_9_13_pct_os

    (a.os_cured_1_2 + a.os_stayed_1_2 + a.os_worsened_1_2 + a.os_improved_1_2) AS sum_total_trans_1_2,

 COALESCE(a.os_cured_1_2 / NULLIF((a.os_cured_1_2 + a.os_stayed_1_2 + a.os_worsened_1_2 + a.os_improved_1_2), 0), 0) AS became_current_1_2_pct_os,
    COALESCE(a.os_stayed_1_2 / NULLIF((a.os_cured_1_2 + a.os_stayed_1_2 + a.os_worsened_1_2 + a.os_improved_1_2), 0), 0) AS stayed_1_2_pct_os,
    COALESCE(a.os_worsened_1_2 / NULLIF((a.os_cured_1_2 + a.os_stayed_1_2 + a.os_worsened_1_2 + a.os_improved_1_2), 0), 0) AS worsened_1_2_pct_os,
    COALESCE(a.os_improved_1_2 / NULLIF((a.os_cured_1_2 + a.os_stayed_1_2 + a.os_worsened_1_2 + a.os_improved_1_2), 0), 0) AS improved_1_2_pct_os,
    (a.os_cured_3_8 + a.os_stayed_3_8 + a.os_worsened_3_8 + a.os_improved_3_8) AS sum_total_trans_3_8,

COALESCE(a.os_cured_3_8 / NULLIF((a.os_cured_3_8 + a.os_stayed_3_8 + a.os_worsened_3_8 + a.os_improved_3_8), 0), 0) AS became_current_3_8_pct_os,
    COALESCE(a.os_stayed_3_8 / NULLIF((a.os_cured_3_8 + a.os_stayed_3_8 + a.os_worsened_3_8 + a.os_improved_3_8), 0), 0) AS stayed_3_8_pct_os,
    COALESCE(a.os_worsened_3_8 / NULLIF((a.os_cured_3_8 + a.os_stayed_3_8 + a.os_worsened_3_8 + a.os_improved_3_8), 0), 0) AS worsened_3_8_pct_os,
    COALESCE(a.os_improved_3_8 / NULLIF((a.os_cured_3_8 + a.os_stayed_3_8 + a.os_worsened_3_8 + a.os_improved_3_8), 0), 0) AS improved_3_8_pct_os,

    (a.os_cured_9_13 + a.os_stayed_9_13 + a.os_worsened_9_13 + a.os_improved_9_13) AS sum_total_trans_9_13,

COALESCE(a.os_cured_9_13 / NULLIF((a.os_cured_9_13 + a.os_stayed_9_13 + a.os_worsened_9_13 + a.os_improved_9_13), 0), 0) AS became_current_9_13_pct_os,
    COALESCE(a.os_stayed_9_13 / NULLIF((a.os_cured_9_13 + a.os_stayed_9_13 + a.os_worsened_9_13 + a.os_improved_9_13), 0), 0) AS stayed_9_13_pct_os,
    COALESCE(a.os_worsened_9_13 / NULLIF((a.os_cured_9_13 + a.os_stayed_9_13 + a.os_worsened_9_13 + a.os_improved_9_13), 0), 0) AS worsened_9_13_pct_os,
    COALESCE(a.os_improved_9_13 / NULLIF((a.os_cured_9_13 + a.os_stayed_9_13 + a.os_worsened_9_13 + a.os_improved_9_13), 0), 0) AS improved_9_13_pct_os,

FROM aggregated a;


----------------------------------------------- Pre-CO Settlements ----------------------------------------------------
CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_settlements AS
WITH all_offers AS (
    SELECT 
        fbbid,
        DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS week_end_date,
        DATEADD(day,-6, DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2) AS week_start_date,
        edate,
        status_name,
        CASE WHEN status_name = 'SETTLEMENT_STATUS' 
                  AND status_value = 'FUNDED' THEN status_value END AS settlement_status_funded,
        CASE WHEN status_name = 'FINAL_SETTLEMENT_AMOUNT' 
                  THEN status_value::NUMERIC END AS settlement_amount
    FROM bi.finance.customer_finance_statuses
    WHERE status_group = 'DISCOUNTED_SETTLEMENT'
      AND (
            status_name = 'FINAL_SETTLEMENT_AMOUNT'
         OR status_name = 'SETTLEMENT_STATUS'
      )
),
-- select * from all_offers where week_end_Date = '2025-07-09';

funded_settlements AS (
    SELECT
        fa.fbbid,
        fa.edate AS final_amount_edate,
        fu.edate AS funded_edate
    FROM all_offers fa
    JOIN (
        SELECT fbbid, edate
        FROM all_offers
        WHERE settlement_status_funded = 'FUNDED'
    ) fu
      ON fa.fbbid = fu.fbbid
     AND fa.settlement_amount IS NOT NULL
     AND fa.edate <= fu.edate
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fa.fbbid, fu.edate
        ORDER BY fa.edate DESC
    ) = 1
),

offers_agg AS (
    SELECT
        a1.week_start_date,
        a1.week_end_date,
        a1.fbbid,
        a1.edate,
        a1.settlement_amount,
        a2.dpd_days,
        a2.is_chargeoff,
        CASE 
            WHEN dpd_days IS NULL AND is_chargeoff = 0 THEN 0
            WHEN dpd_days IS NULL AND is_chargeoff = 1 THEN 98
            ELSE dpd_days
        END AS dpd_days_corrected,
        CASE
            WHEN dpd_days_corrected = 0 AND is_chargeoff = 0 THEN '00. Bucket 0'
            WHEN dpd_days_corrected BETWEEN 1 AND 14 AND is_chargeoff = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected BETWEEN 15 AND 56 AND is_chargeoff = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected BETWEEN 57 AND 91 AND is_chargeoff = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected <= 98 AND is_chargeoff = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        CASE 
            WHEN fs.fbbid IS NOT NULL 
                 AND a1.edate = fs.final_amount_edate THEN 1
            ELSE 0
        END AS is_completed
    FROM all_offers a1
    LEFT JOIN bi.public.daily_approved_customers_data a2 
           ON a1.fbbid = a2.fbbid 
          AND a1.edate = a2.edate - 1
    LEFT JOIN funded_settlements fs 
           ON a1.fbbid = fs.fbbid 
          AND a1.edate = fs.final_amount_edate
    WHERE a1.settlement_amount IS NOT NULL
)

SELECT 
    week_start_date,
    week_end_date,

    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS preco_num_settlements_1_2,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS preco_num_settlements_3_8,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS preco_num_settlements_9_13,

    COALESCE(SUM(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN settlement_amount END),0) AS preco_amt_settlements_1_2,
    COALESCE(SUM(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN settlement_amount END),0) AS preco_amt_settlements_3_8,
    COALESCE(SUM(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN settlement_amount END),0) AS preco_amt_settlements_9_13,

    COUNT(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS preco_num_completed_settlements_1_2,
    COUNT(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS preco_num_completed_settlements_3_8,
    COUNT(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS preco_num_completed_settlements_9_13,

    COALESCE(SUM(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '01. Bucket 1-2' THEN settlement_amount END),0) AS preco_amt_completed_settlements_1_2,
    COALESCE(SUM(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '02. Bucket 3-8' THEN settlement_amount END),0) AS preco_amt_completed_settlements_3_8,
    COALESCE(SUM(DISTINCT CASE WHEN is_completed = 1 AND dpd_bucket_group = '03. Bucket 9-13' THEN settlement_amount END),0) AS preco_amt_completed_settlements_9_13,

FROM offers_agg
GROUP BY 1,2
ORDER BY 1,2;

-------------------------------------------- Pre-CO Customer Level --------------------------------------------
CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_cust_roll_metrics AS
WITH base AS (
    SELECT
        c.fbbid,
        c.week_end_date,
        week_end_date - 6 AS week_start_date,
        
        -- CASE WHEN dpd_days_corrected_fmd is NULL AND is_charged_off_fmd IS NULL THEN 0
        -- ELSE dpd_days_corrected_fmd
        -- END AS dpd_days_corrected,

        ifnull(LAG(dpd_days_corrected_fmd) OVER (PARTITION BY fbbid ORDER BY week_end_date),0) AS lag_dpd_days_corrected,
        ifnull(LAG(IS_CHARGED_OFF_FMD) OVER (PARTITION BY fbbid ORDER BY week_end_date),0) AS lag_is_charged_off,
        ifnull(LAG(OUTSTANDING_PRINCIPAL_DUE) OVER (PARTITION BY fbbid ORDER BY week_end_date),0) AS lag_outstanding_principal_due,
        
        -- CASE WHEN lag_dpd_days_corrected is NULL AND is_charged_off_fmd IS NULL THEN 0
        -- ELSE lag_dpd_days_corrected
        -- END AS lag_dpd_days_corrected,
        
        is_charged_off_fmd,
        dpd_days_corrected_fmd,
        outstanding_principal_due,
            
        CASE
            WHEN dpd_days_corrected_fmd = 0 and is_charged_off_fmd = 0 THEN '00. Bucket 0'
            -- WHEN dpd_days_corrected IS NULL AND is_charged_off IS NULL THEN '00. Bucket 0'
            WHEN dpd_days_corrected_fmd BETWEEN 1 AND 14 and is_charged_off_fmd = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected_fmd BETWEEN 15 AND 56 and is_charged_off_fmd = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected_fmd BETWEEN 57 AND 91 and is_charged_off_fmd = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected_fmd <= 98 and is_charged_off_fmd = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        CASE 
            WHEN lag_dpd_days_corrected = 0 and lag_is_charged_off = 0 THEN '00. Bucket 0'
            when lag_dpd_days_corrected between 1 and 14 and lag_is_charged_off = 0 then '01. Bucket 1-2'
            when lag_dpd_days_corrected between 15 and 56 and lag_is_charged_off = 0 then '02. Bucket 3-8'
            when lag_dpd_days_corrected between 57 and 91 and lag_is_charged_off = 0 then '03. Bucket 9-13'
            WHEN lag_dpd_days_corrected <= 98 or lag_is_charged_off = 1 THEN '04. CHOF'
        end as prev_dpd_bucket_group,
    FROM analytics.credit.customer_level_data_td c
),

combined AS (
    SELECT
        b.*,

        CASE 
            -- From 1-2
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected_fmd = 0 and is_charged_off_fmd = 0 THEN 'Cure_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected_fmd BETWEEN 1 AND 14 and is_charged_off_fmd = 0 THEN 'Stay_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND dpd_days_corrected_fmd BETWEEN 15 AND 98 THEN 'Worsen_1_2'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND is_charged_off_fmd = 1 and lag_is_charged_off = 0 THEN 'Worsen_1_2'

            -- From 3-8
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected_fmd = 0 and is_charged_off_fmd = 0 THEN 'Cure_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected_fmd BETWEEN 1 AND 14 and is_charged_off_fmd = 0 THEN 'Improve_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected_fmd BETWEEN 15 AND 56 and is_charged_off_fmd = 0 THEN 'Stay_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND dpd_days_corrected_fmd BETWEEN 57 AND 98 THEN 'Worsen_3_8'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND is_charged_off_fmd = 1 and lag_is_charged_off = 0 THEN 'Worsen_3_8'


            -- From 9-13
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected_fmd = 0 and is_charged_off_fmd = 0 THEN 'Cure_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected_fmd BETWEEN 1 AND 56 and is_charged_off_fmd = 0 THEN 'Improve_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected_fmd BETWEEN 57 AND 91 and is_charged_off_fmd = 0 THEN 'Stay_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND dpd_days_corrected_fmd BETWEEN 92 AND 98 THEN 'Worsen_9_13'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND is_charged_off_fmd = 1 and lag_is_charged_off = 0 THEN 'Worsen_9_13'

            ELSE NULL
        END
        AS transition_type

    FROM base b
),

aggregated as (
SELECT
    week_start_date,
    week_end_date,

    -- Outstanding principal
    SUM(CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN outstanding_principal_due END) AS sum_os_0,
    SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN outstanding_principal_due END) AS sum_os_1_2,
    SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN outstanding_principal_due END) AS sum_os_3_8,
    SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN outstanding_principal_due END) AS sum_os_9_13,
    SUM(CASE WHEN dpd_bucket_group = '04. CHOF' THEN outstanding_principal_due END) AS sum_os_co,

    --Lagged Outstanding principal
    SUM(CASE WHEN prev_dpd_bucket_group = '00. Bucket 0' THEN lag_outstanding_principal_due END) AS lag_sum_os_0,
    SUM(CASE WHEN prev_dpd_bucket_group = '01. Bucket 1-2' THEN lag_outstanding_principal_due END) AS lag_sum_os_1_2,
    SUM(CASE WHEN prev_dpd_bucket_group = '02. Bucket 3-8' THEN lag_outstanding_principal_due END) AS lag_sum_os_3_8,
    SUM(CASE WHEN prev_dpd_bucket_group = '03. Bucket 9-13' THEN lag_outstanding_principal_due END) AS lag_sum_os_9_13,
    SUM(CASE WHEN prev_dpd_bucket_group = '04. CHOF' THEN lag_outstanding_principal_due END) AS lag_sum_os_co,

    -- Customer counts
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '00. Bucket 0' THEN fbbid END) AS num_cust_0,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS num_cust_1_2,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS num_cust_3_8,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS num_cust_9_13,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' THEN fbbid END) AS num_cust_co,

    -- Lagged Customer counts
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '00. Bucket 0' THEN fbbid END) AS lag_num_cust_0,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS lag_num_cust_1_2,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS lag_num_cust_3_8,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS lag_num_cust_9_13,
    COUNT(DISTINCT CASE WHEN prev_dpd_bucket_group = '04. CHOF' THEN fbbid END) AS lag_num_cust_co,

    -- Roll rates from each previous group
    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_1_2' THEN fbbid END) AS num_cured_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_1_2' THEN fbbid END) AS num_improved_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_1_2' THEN fbbid END) AS num_stayed_1_2,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_1_2' THEN fbbid END) AS num_worsened_1_2,

    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_3_8' THEN fbbid END) AS num_cured_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_3_8' THEN fbbid END) AS num_improved_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_3_8' THEN fbbid END) AS num_stayed_3_8,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_3_8' THEN fbbid END) AS num_worsened_3_8,

    COUNT(DISTINCT CASE WHEN transition_type = 'Cure_9_13' THEN fbbid END) AS num_cured_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Improve_9_13' THEN fbbid END) AS num_improved_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Stay_9_13' THEN fbbid END) AS num_stayed_9_13,
    COUNT(DISTINCT CASE WHEN transition_type = 'Worsen_9_13' THEN fbbid END) AS num_worsened_9_13,

    SUM(CASE WHEN transition_type = 'Cure_1_2' THEN lag_outstanding_principal_due END) AS os_cured_1_2,
    SUM(CASE WHEN transition_type = 'Improve_1_2' THEN lag_outstanding_principal_due END) AS os_improved_1_2,
    SUM(CASE WHEN transition_type = 'Stay_1_2' THEN lag_outstanding_principal_due END) AS os_stayed_1_2,
    SUM(CASE WHEN transition_type = 'Worsen_1_2' THEN lag_outstanding_principal_due END) AS os_worsened_1_2,

    SUM(CASE WHEN transition_type = 'Cure_3_8' THEN lag_outstanding_principal_due END) AS os_cured_3_8,
    SUM(CASE WHEN transition_type = 'Improve_3_8' THEN lag_outstanding_principal_due END) AS os_improved_3_8,
    SUM(CASE WHEN transition_type = 'Stay_3_8' THEN lag_outstanding_principal_due END) AS os_stayed_3_8,
    SUM(CASE WHEN transition_type = 'Worsen_3_8' THEN lag_outstanding_principal_due END) AS os_worsened_3_8,

    SUM(CASE WHEN transition_type = 'Cure_9_13' THEN lag_outstanding_principal_due END) AS os_cured_9_13,
    SUM(CASE WHEN transition_type = 'Improve_9_13' THEN lag_outstanding_principal_due END) AS os_improved_9_13,
    SUM(CASE WHEN transition_type = 'Stay_9_13' THEN lag_outstanding_principal_due END) AS os_stayed_9_13,
    SUM(CASE WHEN transition_type = 'Worsen_9_13' THEN lag_outstanding_principal_due END) AS os_worsened_9_13,

FROM combined
WHERE dpd_bucket_group IS NOT NULL
GROUP BY week_start_date, week_end_date
ORDER BY week_start_date, week_end_date
)

SELECT 
    a.week_start_date,
    a.week_end_date,
    num_cust_1_2,
    num_cust_3_8,
    num_cust_9_13,
    -- sum_os_1_2,
    -- sum_os_3_8,
    -- sum_os_9_13,    
    -------------------- DPD 1–2 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_1_2 / NULLIF(lag_num_cust_1_2, 0),0)
    AS became_current_1_2_pct_num,
    
    -- COALESCE(num_stayed_1_2 / NULLIF(lag_num_cust_1_2, 0),0)
    -- AS stayed_1_2_pct_num,
    
    COALESCE(num_worsened_1_2 / NULLIF(lag_num_cust_1_2, 0),0)
    AS worsened_1_2_pct_num,
    
    COALESCE(num_improved_1_2 / NULLIF(lag_num_cust_1_2, 0),0)
    AS improved_1_2_pct_num,

    1 - (became_current_1_2_pct_num + worsened_1_2_pct_num + improved_1_2_pct_num) AS stayed_1_2_pct_num,
    
    -- OS metrics
    COALESCE(os_cured_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    AS became_current_1_2_pct_os,
    
    -- COALESCE(os_stayed_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    -- AS stayed_1_2_pct_os,
    
    COALESCE(os_worsened_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    AS worsened_1_2_pct_os,
    
    COALESCE(os_improved_1_2 / NULLIF(lag_sum_os_1_2, 0),0)
    AS improved_1_2_pct_os,

    1 - (became_current_1_2_pct_os + worsened_1_2_pct_os + improved_1_2_pct_os) AS stayed_1_2_pct_os,

    
    -------------------- DPD 3–8 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_3_8 / NULLIF(lag_num_cust_3_8, 0),0)
    AS became_current_3_8_pct_num,
    
    -- COALESCE(num_stayed_3_8 / NULLIF(lag_num_cust_3_8, 0),0)
    -- AS stayed_3_8_pct_num,
    
    COALESCE(num_worsened_3_8 / NULLIF(lag_num_cust_3_8, 0),0)
    AS worsened_3_8_pct_num,
    
    COALESCE(num_improved_3_8 / NULLIF(lag_num_cust_3_8, 0),0)
    AS improved_3_8_pct_num,

    1 - (became_current_3_8_pct_num + worsened_3_8_pct_num + improved_3_8_pct_num) AS stayed_3_8_pct_num,

    
    -- OS metrics
    COALESCE(os_cured_3_8 / NULLIF(lag_sum_os_3_8, 0),0)
    AS became_current_3_8_pct_os,
    
    -- COALESCE(os_stayed_3_8 / NULLIF(lag_sum_os_3_8, 0),0)
    -- AS stayed_3_8_pct_os,
    
    COALESCE(os_worsened_3_8 / NULLIF(lag_sum_os_3_8, 0),0) 
    AS worsened_3_8_pct_os,
    
    COALESCE(os_improved_3_8 / NULLIF(lag_sum_os_3_8, 0),0) 
    AS improved_3_8_pct_os,

    1 - (became_current_3_8_pct_os + worsened_3_8_pct_os + improved_3_8_pct_os) AS stayed_3_8_pct_os,

        ---------------------- DPD 9–13 Transitions --------------------
    -- Count metrics
    COALESCE(num_cured_9_13 / NULLIF(lag_num_cust_9_13, 0),
    0) AS became_current_9_13_pct_num,
    
    -- COALESCE(num_stayed_9_13 / NULLIF(lag_num_cust_9_13, 0),
    -- 0) AS stayed_9_13_pct_num,
    
    COALESCE(num_worsened_9_13 / NULLIF(lag_num_cust_9_13, 0),
    0) AS worsened_9_13_pct_num,
    
    COALESCE(num_improved_9_13 / NULLIF(lag_num_cust_9_13, 0),
    0) AS improved_9_13_pct_num,

    1 - (became_current_9_13_pct_num + worsened_9_13_pct_num + improved_9_13_pct_num) AS stayed_9_13_pct_num,
    
    -- OS metrics
    COALESCE(os_cured_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    AS became_current_9_13_pct_os,
    
    -- COALESCE(os_stayed_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    -- AS stayed_9_13_pct_os,
    
    COALESCE(os_worsened_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    AS worsened_9_13_pct_os,
    
    COALESCE(os_improved_9_13 / NULLIF(lag_sum_os_9_13, 0),0) 
    AS improved_9_13_pct_os,

    1 - (became_current_9_13_pct_os + worsened_9_13_pct_os + improved_9_13_pct_os) AS stayed_9_13_pct_os,

FROM aggregated a
ORDER BY 1,2;

----------------------------------------------- Final Pre-Chargeoff Table ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_final AS
SELECT 
    a3.*,
    a1.num_loans_1_2,
    a1.num_loans_3_8,
    a1.num_loans_9_13,
    a1.pre_co_total_os,
    a1.total_os_due,
    a1.sum_os_1_2,
    a1.sum_os_3_8,
    a1.sum_os_9_13,
    a1.sum_odb_1_2,
    a1.sum_odb_3_8,
    a1.sum_odb_9_13,
    a1.collected_1_2,
    a1.collected_3_8,
    a1.collected_9_13,
    preco_num_settlements_1_2,
    preco_num_settlements_3_8,
    preco_num_settlements_9_13,
    preco_amt_settlements_1_2,
    preco_amt_settlements_3_8,
    preco_amt_settlements_9_13,
    preco_num_completed_settlements_1_2,
    preco_num_completed_settlements_3_8,
    preco_num_completed_settlements_9_13,
    preco_amt_completed_settlements_1_2,
    preco_amt_completed_settlements_3_8,
    preco_amt_completed_settlements_9_13
    
FROM analytics.credit.km_collections_preco_metrics a1
LEFT JOIN analytics.credit.km_collections_preco_settlements a2 on a1.week_end_date = a2.week_end_date
LEFT JOIN analytics.credit.km_collections_preco_cust_roll_metrics a3 on a1.week_end_date = a3.week_end_date;

----------------------------------------------- Post-Chargeoff Metrics ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_postco_metrics AS
WITH base AS (
    SELECT
        loan_key,
        fbbid,
        week_end_date,
        week_end_date - 6 AS week_start_date,
        
        CASE WHEN dpd_days_corrected is NULL AND is_charged_off IS NULL THEN 0
        ELSE dpd_days_corrected
        END AS dpd_days_corrected,
        
        CASE WHEN lag_dpd_days_corrected is NULL AND lag_is_charged_off IS NULL THEN 0
        ELSE lag_dpd_days_corrected
        END AS lag_dpd_days_corrected,
        
        is_charged_off,
        lag_is_charged_off,
        is_after_co,
        charge_off_date,
        total_paid,
        principal_paid,
        
        CASE
            WHEN dpd_days_corrected = 0 and is_charged_off = 0 THEN '00. Bucket 0'
            -- WHEN dpd_days_corrected IS NULL AND is_charged_off IS NULL THEN '00. Bucket 0'
            WHEN dpd_days_corrected BETWEEN 1 AND 14 and is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected BETWEEN 15 AND 56 and is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected BETWEEN 57 AND 91 and is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected <= 98 and is_charged_off = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        CASE 
            WHEN lag_dpd_days_corrected = 0 and lag_is_charged_off = 0 THEN '00. Bucket 0'
            when lag_dpd_days_corrected between 1 and 14 and lag_is_charged_off = 0 then '01. Bucket 1-2'
            when lag_dpd_days_corrected between 15 and 56 and lag_is_charged_off = 0 then '02. Bucket 3-8'
            when lag_dpd_days_corrected between 57 and 91 and lag_is_charged_off = 0 then '03. Bucket 9-13'
            WHEN lag_dpd_days_corrected <= 98 or lag_is_charged_off = 1 THEN '04. CHOF'
        end as prev_dpd_bucket_group,
        outstanding_principal_due,
        lag_outstanding_principal_due,
        os_91,
        os_91_new,
        os_1_90,
        os_p_1_90,
    FROM analytics.credit.loan_level_data_pb
),

placement_status AS (
SELECT 
    fbbid,
    edate,
    recovery_suggested_state,
    CASE 
        WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
        OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
        WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
        ELSE 'Unknown'
    END AS postco_placement
FROM bi.public.daily_approved_customers_data
WHERE edate = CURRENT_DATE - 1
),

placement_status_2 AS (
SELECT 
    fbbid,
    edate,
    recovery_suggested_state,
    CASE 
        WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
        OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
        WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
        ELSE 'Unknown'
    END AS postco_placement
FROM bi.public.daily_approved_customers_data
WHERE edate >= '2020-12-01'
-- WHERE edate = CURRENT_DATE - 1

)

SELECT
    week_start_date,
    week_end_date,

    -- Customer counts
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) THEN a1.fbbid END) AS total_num_cust_co,
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) AND a2.postco_placement = 'Internal' THEN a1.fbbid END) AS internal_total_num_cust_co,
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) AND a2.postco_placement = 'External' THEN a1.fbbid END) AS external_total_num_cust_co,
    
    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN a1.fbbid END) AS new_num_cust_co,
    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'Internal' THEN a1.fbbid END) AS internal_new_num_cust_co,
    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'External' THEN a1.fbbid END) AS external_new_num_cust_co,


    -- Outstanding principal
    SUM(CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) THEN os_91 END) AS total_sum_os_co,
    SUM(CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) AND a2.postco_placement = 'Internal' THEN os_91 END) AS internal_total_sum_os_co,
    SUM(CASE WHEN is_charged_off = 1 and (charge_off_date between DATEADD(year, -5, week_end_date) AND week_end_date) AND a2.postco_placement = 'External' THEN os_91 END) AS external_total_sum_os_co,
    
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN os_91_new END) AS new_sum_os_co,
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'Internal' THEN os_91_new END) AS internal_new_sum_os_co,
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'External' THEN os_91_new END) AS external_new_sum_os_co,

    -- Loan counts
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 THEN loan_key END) AS total_num_loans_co,
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 AND a2.postco_placement = 'Internal' THEN loan_key END) AS internal_total_num_loans_co,
    COUNT(DISTINCT CASE WHEN is_charged_off = 1 AND a2.postco_placement = 'External' THEN loan_key END) AS external_total_num_loans_co,

    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN loan_key END) AS new_num_loans_co,
    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'Internal' THEN loan_key END) AS internal_new_num_loans_co,
    COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date AND a2.postco_placement = 'External' THEN loan_key END) AS external_new_num_loans_co,

    -- Recoveries
    SUM(CASE WHEN is_after_co = 1 THEN principal_paid END) AS post_co_recoveries,
    SUM(CASE WHEN is_after_co = 1 AND a3.postco_placement = 'Internal' THEN principal_paid END) AS internal_post_co_recoveries,
    SUM(CASE WHEN is_after_co = 1 AND a3.postco_placement = 'External' THEN principal_paid END) AS external_post_co_recoveries,
    
FROM base a1
LEFT JOIN placement_status a2 on a1.fbbid = a2.fbbid
LEFT JOIN placement_status_2 a3 on a1.fbbid = a3.fbbid and a1.week_end_date = a3.edate
-- WHERE dpd_bucket_group IS NOT NULL
GROUP BY week_start_date, week_end_date
ORDER BY week_start_date, week_end_date;

-- select * from analytics.credit.km_collections_postco_metrics;

----------------------------------------------- Latest Recovery State (Internal vs External) ----------------------------------------------------
-- CREATE OR REPLACE TABLE analytics.credit.postco_placement_status AS
-- SELECT 
--     fbbid,
--     edate,
--     recovery_suggested_state,
--     CASE 
--         WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
--         OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
--         WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
--         ELSE 'Unknown'
--     END AS postco_placement
-- FROM bi.public.daily_approved_customers_data
-- WHERE edate = CURRENT_DATE - 1;

-- SELECT * FROM analytics.credit.postco_placement_status;

----------------------------------------------- Settlements ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_postco_settlements AS
WITH all_offers AS (
    SELECT 
        fbbid,
        DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS week_end_date,
        DATEADD(day,-6, DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2) AS week_start_date,
        edate,
        status_name,
        CASE WHEN status_name = 'SETTLEMENT_STATUS' 
                  AND status_value = 'FUNDED' THEN status_value END AS settlement_status_funded,
        CASE WHEN status_name = 'FINAL_SETTLEMENT_AMOUNT' 
                  THEN status_value::NUMERIC END AS settlement_amount
    FROM bi.finance.customer_finance_statuses
    WHERE status_group = 'DISCOUNTED_SETTLEMENT'
      AND (
            status_name = 'FINAL_SETTLEMENT_AMOUNT'
         OR status_name = 'SETTLEMENT_STATUS'
      )
),

funded_settlements AS (
    SELECT
        fa.fbbid,
        fa.edate AS final_amount_edate,
        fu.edate AS funded_edate
    FROM all_offers fa
    JOIN (
        SELECT fbbid, edate
        FROM all_offers
        WHERE settlement_status_funded = 'FUNDED'
    ) fu
      ON fa.fbbid = fu.fbbid
     AND fa.settlement_amount IS NOT NULL
     AND fa.edate <= fu.edate
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fa.fbbid, fu.edate
        ORDER BY fa.edate DESC
    ) = 1
),

offers_agg AS (
    SELECT
        a1.week_start_date,
        a1.week_end_date,
        a1.fbbid,
        a1.edate,
        a1.settlement_amount,
        a2.dpd_days,
        a2.is_chargeoff,
        CASE 
            WHEN dpd_days IS NULL AND is_chargeoff = 0 THEN 0
            WHEN dpd_days IS NULL AND is_chargeoff = 1 THEN 98
            ELSE dpd_days
        END AS dpd_days_corrected,
        CASE
            WHEN dpd_days_corrected = 0 AND is_chargeoff = 0 THEN '00. Bucket 0'
            WHEN dpd_days_corrected BETWEEN 1 AND 14 AND is_chargeoff = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected BETWEEN 15 AND 56 AND is_chargeoff = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected BETWEEN 57 AND 91 AND is_chargeoff = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected <= 98 AND is_chargeoff = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        CASE 
            WHEN fs.fbbid IS NOT NULL 
                 AND a1.edate = fs.final_amount_edate THEN 1
            ELSE 0
        END AS is_completed
    FROM all_offers a1
    LEFT JOIN bi.public.daily_approved_customers_data a2 
           ON a1.fbbid = a2.fbbid 
          AND a1.edate = a2.edate - 1
    LEFT JOIN funded_settlements fs 
           ON a1.fbbid = fs.fbbid 
          AND a1.edate = fs.final_amount_edate
    WHERE a1.settlement_amount IS NOT NULL
),
placement_status AS (
SELECT 
    fbbid,
    edate,
    recovery_suggested_state,
    CASE 
        WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
        OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
        WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
        ELSE 'Unknown'
    END AS postco_placement
FROM bi.public.daily_approved_customers_data
-- WHERE edate = CURRENT_DATE - 1
)

SELECT 
    week_start_date,
    week_end_date,

    -- Offered settlements
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' THEN a1.fbbid END) AS postco_num_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' THEN settlement_amount END),0) AS postco_amt_settlements,
    
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'Internal' THEN a1.fbbid END) AS internal_postco_num_settlements,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'External' THEN a1.fbbid END) AS external_postco_num_settlements,
    
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'Internal' THEN settlement_amount END),0) AS internal_postco_amt_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'External' THEN settlement_amount END),0) AS external_postco_amt_settlements,

    -- Completed settlements
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 THEN a1.fbbid END) AS postco_num_completed_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 THEN settlement_amount END),0) AS postco_amt_completed_settlements,

    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'Internal' THEN a1.fbbid END) AS internal_postco_num_completed_settlements,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'External' THEN a1.fbbid END) AS external_postco_num_completed_settlements,

    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'Internal' THEN settlement_amount END),0) AS internal_postco_amt_completed_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'External' THEN settlement_amount END),0) AS external_postco_amt_completed_settlements


FROM offers_agg a1
LEFT JOIN placement_status a2 
    ON a1.fbbid = a2.fbbid 
   AND a1.edate = a2.edate 
GROUP BY 1,2
ORDER BY 1,2;
----------------------------------------------- Final Post-Chargeoff Table ----------------------------------------------------


CREATE OR REPLACE TABLE analytics.credit.collections_km_postco_final AS
WITH base AS (
    SELECT 
        a1.week_start_date,
        a1.week_end_date,
        a1.total_num_cust_co,
        a1.new_num_cust_co,
        a1.total_sum_os_co,
        a1.new_sum_os_co,
        a1.total_num_loans_co,
        a1.new_num_loans_co,
        a1.post_co_recoveries,
        internal_total_num_cust_co,
        a1.external_total_num_cust_co,
        a1.internal_new_num_cust_co,
        a1.external_new_num_cust_co,
        a1.internal_total_sum_os_co,
        a1.external_total_sum_os_co,
        a1.internal_new_sum_os_co,
        a1.external_new_sum_os_co,
        a1.internal_total_num_loans_co,
        a1.external_total_num_loans_co,
        a1.internal_new_num_loans_co,
        a1.external_new_num_loans_co,
        a1.internal_post_co_recoveries,
        a1.external_post_co_recoveries,
        a2.postco_num_settlements,
        a2.postco_amt_settlements,
        a2.internal_postco_num_settlements,
        a2.external_postco_num_settlements,
        a2.internal_postco_amt_settlements,
        a2.external_postco_amt_settlements,
        a2.postco_num_completed_settlements,
        a2.postco_amt_completed_settlements,
        a2.internal_postco_num_completed_settlements,
        a2.external_postco_num_completed_settlements,
        a2.internal_postco_amt_completed_settlements,
        a2.external_postco_amt_completed_settlements,

        SUM(a1.post_co_recoveries) OVER (ORDER BY a1.week_end_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_post_co_recoveries

    FROM analytics.credit.km_collections_postco_metrics a1
    LEFT JOIN analytics.credit.km_collections_postco_settlements a2 
        ON a1.week_end_date = a2.week_end_date
),
net_os_co AS (
    SELECT *,
        total_sum_os_co - running_post_co_recoveries AS net_total_sum_os_co
    FROM base
),
final AS (
    SELECT *,
        LAG(net_total_sum_os_co) OVER (ORDER BY week_end_date ASC) AS lag_net_total_sum_os_co
    FROM net_os_co
)
SELECT 
    *,
    post_co_recoveries / NULLIF(lag_net_total_sum_os_co, 0) AS perc_recovered_this_week,
    running_post_co_recoveries/ NULLIF(lag_net_total_sum_os_co, 0) AS perc_recovered_until_this_week
FROM final;

----------------------------------------------- Operations Metrics ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_ops_metrics AS
WITH ReportingCutoffDate AS (
    -- Calculate the date of the most recent Wednesday (inclusive of today if today is Wednesday)
    -- DAYOFWEEKISO: Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6, Sunday=7
    -- ((DAYOFWEEKISO(CURRENT_DATE) - 3 + 7) % 7) calculates the number of days to subtract.
    SELECT
        (CURRENT_DATE - ((DAYOFWEEKISO(CURRENT_DATE) - 3 + 7) % 7))::DATE AS last_wednesday_date
),
RankedLoanCreationRecords AS (
    -- Step 1a: Rank loan records from FINANCE_METRICS_DAILY
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE,
        ROW_NUMBER() OVER (PARTITION BY FBBID, LOAN_KEY ORDER BY EDATE ASC) as rn
    FROM
        BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE PRODUCT_TYPE <> 'Flexpay'
),
DistinctLoanCreation AS (
    -- Step 1b: Select the definitive LOAN_CREATED_DATE
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE
    FROM
        RankedLoanCreationRecords
    WHERE
        rn = 1
),
LoanCreatedWeek AS (
    -- Step 2: Calculate LOAN_CREATED_WEEK_END for loan cohort grouping
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE,
        (date_trunc('week', (LOAN_CREATED_DATE::date + INTERVAL '4 days'))::date + INTERVAL '2 days') AS LOAN_CREATED_WEEK_END
    FROM
        DistinctLoanCreation
),
FbbidPartner AS (
    -- Step 2b: Get PARTNER for each FBBID
    SELECT
        FBBID,
        is_test,
        sub_product,
        termunits,
        PARTNER AS PARTNER
    FROM
        INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2
    WHERE
        EDATE = MIN_EDATE
),
LoanCreatedWeekWithPartner AS (
    -- Step 2c: Combine LoanCreatedWeek with Partner information
    SELECT
        lcw.FBBID,
        lcw.LOAN_KEY,
        lcw.LOAN_CREATED_DATE,
        lcw.LOAN_CREATED_WEEK_END,
        fp.PARTNER,
        fp.termunits
    FROM
        LoanCreatedWeek lcw
    LEFT JOIN
        FbbidPartner fp ON lcw.FBBID = fp.FBBID
    WHERE fp.is_test = 0
    AND fp.sub_product <> 'Credit Builder'
    AND fp.sub_product <> 'mca'
),
RankedPayments AS (
    -- Step 3: Rank payments for each loan by their planned transmission date
    -- Filtered for payments due on or before the last Wednesday and for Debits
    SELECT
        pd.LOAN_KEY,
        pd.PAYMENT_PLANNED_TRANSMISSION_DATE,
        pd.PAYMENT_STATUS,
        ROW_NUMBER() OVER (PARTITION BY pd.LOAN_KEY ORDER BY pd.PAYMENT_PLANNED_TRANSMISSION_DATE ASC) as payment_rank
    FROM
        BI.FINANCE.PAYMENTS_DATA pd, ReportingCutoffDate rcd -- Cross join to get the cutoff date
    WHERE
        pd.PAYMENT_PLANNED_TRANSMISSION_DATE <= rcd.last_wednesday_date
        AND pd.DIRECTION = 'D'
),
FirstPaymentDefault AS (
    -- Step 4a: Identify loans with a first payment default
    SELECT
        LOAN_KEY,
        CASE WHEN PAYMENT_STATUS <> 'FUND' THEN 1 ELSE 0 END as is_first_payment_default
    FROM
        RankedPayments
    WHERE
        payment_rank = 1
),
LoanPaymentSpecifics AS (
    -- Step 4b: Get status for 1st, 2nd, 3rd payments
    SELECT
        LOAN_KEY,
        MAX(CASE WHEN payment_rank = 1 THEN PAYMENT_STATUS END) as p1_status,
        MAX(CASE WHEN payment_rank = 1 THEN 1 ELSE 0 END) as has_p1_record,

        MAX(CASE WHEN payment_rank = 2 THEN PAYMENT_STATUS END) as p2_status,
        MAX(CASE WHEN payment_rank = 2 THEN 1 ELSE 0 END) as has_p2_record,

        MAX(CASE WHEN payment_rank = 3 THEN PAYMENT_STATUS END) as p3_status,
        MAX(CASE WHEN payment_rank = 3 THEN 1 ELSE 0 END) as has_p3_record
    FROM
        RankedPayments
    WHERE
        payment_rank <= 3
    GROUP BY
        LOAN_KEY
),
FirstTwoPaymentsDefault AS (
    -- Step 4c: Identify loans where the first TWO payments were missed
    SELECT
        LOAN_KEY,
        CASE
            WHEN has_p1_record = 1 AND p1_status <> 'FUND' AND
                 has_p2_record = 1 AND p2_status <> 'FUND'
            THEN 1
            ELSE 0
        END as is_first_two_payments_default
    FROM
        LoanPaymentSpecifics
),
FirstThreePaymentsDefault AS (
    -- Step 4d: Identify loans where the first THREE payments were missed
    SELECT
        LOAN_KEY,
        CASE
            WHEN has_p1_record = 1 AND p1_status <> 'FUND' AND
                 has_p2_record = 1 AND p2_status <> 'FUND' AND
                 has_p3_record = 1 AND p3_status <> 'FUND'
            THEN 1
            ELSE 0
        END as is_first_three_payments_default
    FROM
        LoanPaymentSpecifics
),
LoansWithLatestStatus AS (
    -- Step 5: Get the latest IS_CHARGED_OFF status for each loan
    SELECT
        LOAN_KEY,
        FBBID,
        IS_CHARGED_OFF,
        ROW_NUMBER() OVER (PARTITION BY LOAN_KEY ORDER BY EDATE DESC) as rn
    FROM
        BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE PRODUCT_TYPE <> 'Flexpay'
),
LoanFundingSummary AS (
    -- Step 6: Determine if each loan has ever received any 'FUND' payment (debits due by last Wednesday)
    SELECT
        lcwp.LOAN_KEY,
        COALESCE(MAX(CASE WHEN pd.PAYMENT_STATUS = 'FUND' THEN 1 ELSE 0 END), 0) as has_any_funding
    FROM
        LoanCreatedWeekWithPartner lcwp
    CROSS JOIN ReportingCutoffDate rcd -- Make cutoff date available
    LEFT JOIN
        BI.FINANCE.PAYMENTS_DATA pd
            ON lcwp.LOAN_KEY = pd.LOAN_KEY
            AND pd.DIRECTION = 'D'
            AND pd.PAYMENT_PLANNED_TRANSMISSION_DATE <= rcd.last_wednesday_date
    GROUP BY
        lcwp.LOAN_KEY
),
Metric4Data AS (
    -- Step 7: Prepare data for Metric 4
    SELECT
        lcwp.LOAN_KEY,
        CASE
            WHEN lfs.has_any_funding = 0 AND (lls.IS_CHARGED_OFF = 1)
            THEN 1 ELSE 0
        END as is_no_funding_and_charge_off,
        CASE
            WHEN (lls.IS_CHARGED_OFF = 1)
            THEN 1 ELSE 0
        END as IS_CHARGED_OFF_for_denom
    FROM
        LoanCreatedWeekWithPartner lcwp
    LEFT JOIN
        LoanFundingSummary lfs ON lcwp.LOAN_KEY = lfs.LOAN_KEY
    LEFT JOIN
        LoansWithLatestStatus lls ON lcwp.LOAN_KEY = lls.LOAN_KEY AND lls.rn = 1
)
    -- Step 8: Aggregate all counts by LOAN_CREATED_WEEK_END and PARTNER
    SELECT
        lcwp.LOAN_CREATED_WEEK_END AS WEEK_END_DATE, -- This is for cohort grouping
        -- lcwp.PARTNER,
        -- lcwp.termunits,
        COUNT(DISTINCT lcwp.LOAN_KEY) as total_loans_originated,

        SUM(COALESCE(fpd.is_first_payment_default, 0)) as count_first_payment_default,
        SUM(COALESCE(f2pd.is_first_two_payments_default, 0)) as count_first_two_payments_default,
        SUM(COALESCE(f3pd.is_first_three_payments_default, 0)) as count_first_three_payments_default,

        SUM(COALESCE(m4d.is_no_funding_and_charge_off, 0)) as count_no_funding_and_charge_off,
        SUM(COALESCE(m4d.IS_CHARGED_OFF_for_denom, 0)) as count_total_charge_off_for_metric4
    FROM
        LoanCreatedWeekWithPartner lcwp
    LEFT JOIN
        FirstPaymentDefault fpd ON lcwp.LOAN_KEY = fpd.LOAN_KEY
    LEFT JOIN
        FirstTwoPaymentsDefault f2pd ON lcwp.LOAN_KEY = f2pd.LOAN_KEY
    LEFT JOIN
        FirstThreePaymentsDefault f3pd ON lcwp.LOAN_KEY = f3pd.LOAN_KEY
    LEFT JOIN
        Metric4Data m4d ON lcwp.LOAN_KEY = m4d.LOAN_KEY
    GROUP BY
        1
;


----------------------------------------------- Bankruptcy Statuses ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_ops_bankruptcy AS
WITH base AS (
SELECT
    dacd.fbbid,
    c.week_start_date,
    c.week_end_date,
    is_bankruptcy,
    lag(is_bankruptcy) over (partition by dacd.fbbid order by week_end_date) as prev_bk_status,
    lag(c.outstanding_principal_due) over (partition by dacd.fbbid order by week_end_date) as prev_os_principal,
    c.outstanding_principal_due
FROM bi.public.daily_approved_customers_data dacd
LEFT JOIN analytics.credit.customer_level_data_td c on dacd.fbbid = c.fbbid and dacd.edate = c.week_end_date
)

SELECT 
    week_start_date,
    week_end_date,

    ---Total Bankruptcies
    COUNT(DISTINCT CASE WHEN is_bankruptcy = 1 THEN fbbid END) AS total_num_bk,
    SUM(CASE WHEN is_bankruptcy = 1 THEN outstanding_principal_due END) AS total_sum_bk,

    ---New Bankruptcies
    COUNT(DISTINCT CASE WHEN is_bankruptcy = 1 and prev_bk_status = 0 THEN fbbid END) AS new_num_bk,
    SUM(CASE WHEN is_bankruptcy = 1 and prev_bk_status = 0 THEN outstanding_principal_due END) AS new_sum_bk,

    --- Exiting BK Status
    COUNT(DISTINCT CASE WHEN is_bankruptcy = 0 AND prev_bk_status = 1 THEN fbbid END) AS num_exited_bk,
    SUM(CASE WHEN is_bankruptcy = 0 AND prev_bk_status = 1 THEN prev_os_principal END) AS sum_exited_bk,
    
FROM base
WHERE week_end_date is not null
GROUP BY 1,2
ORDER BY 1,2;

----------------------------------------------- Final Operations Table ----------------------------------------------------
CREATE OR REPLACE TABLE analytics.credit.km_collections_ops_final AS
SELECT 
    a1.*,
    a2.total_num_bk,
    a2.total_sum_bk,
    a2.new_num_bk,
    a2.new_sum_bk
FROM analytics.credit.km_collections_ops_metrics a1
LEFT JOIN analytics.credit.km_collections_ops_bankruptcy a2 ON a1.week_end_date = a2.week_end_date;


----------------------------------------------- CHOF + Payment Rates ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_chof_payment_flow_rates AS
WITH base AS (
    SELECT
        loan_key,
        fbbid,
        week_end_date,
        outstanding_principal_due,
        dpd_days_corrected,
        lag_dpd_days_corrected,
        is_charged_off,
        lag_is_charged_off
    FROM analytics.credit.loan_level_data_pb
),

-- Identify entry into bucket 1 (DPD 1–7 from DPD 0)
entry_to_bucket1 AS (
    SELECT
        loan_key,
        fbbid,
        week_end_date AS cohort_week,
        outstanding_principal_due AS entry_os
    FROM base
    WHERE 
        dpd_days_corrected BETWEEN 1 AND 7
        AND is_charged_off = 0
        AND lag_dpd_days_corrected = 0
        AND lag_is_charged_off = 0
),

-- First charge-off event per loan
first_chargeoff_events AS (
    SELECT
        loan_key,
        MIN(week_end_date) AS chof_week
    FROM base
    WHERE is_charged_off = 1 AND dpd_days_corrected <= 98
    GROUP BY loan_key
),

-- OS at time of charge-off
chargeoff_os_values AS (
    SELECT
        b.loan_key,
        b.week_end_date,
        b.outstanding_principal_due AS raw_chargeoff_os
    FROM base b
    JOIN first_chargeoff_events f
      ON b.loan_key = f.loan_key
     AND b.week_end_date = f.chof_week
),

-- Collections before charge-off
collections_raw AS (
    WITH deduped AS (
        SELECT A1.*
        FROM BI.FINANCE.PAYMENTS_STATUSES_UNITED A1
        LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY A2 
            ON A1.LOAN_KEY = A2.LOAN_KEY
        WHERE
            PRODUCT_TYPE <> 'Flexpay'
            AND LOAN_CREATED_DATE = EDATE
            AND PAYMENT_STATUS = 'FUND'
            AND DIRECTION = 'D'
            AND PAYMENT_EVENT_TIME::DATE >= '2020-12-01'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY PAYMENT_ID, A1.LOAN_KEY
            ORDER BY 
                PAYMENT_STATUS_CHANGE_EVENT_TIME DESC,
                PAYMENT_EVENT_ORDER DESC
        ) = 1
    ),
    base_collected AS (
        SELECT
            LOAN_KEY,
            PAYMENT_EVENT_TIME::DATE AS PAYMENT_DATE,
            SUM(PAYMENT_PRINCIPAL_AMOUNT) AS total_collections
        FROM deduped
        GROUP BY 1, 2
    )
    SELECT
        LOAN_KEY,
        PAYMENT_DATE,
        total_collections
    FROM base_collected
),

charged_off_loans AS (
    SELECT
        loan_key,
        MIN(edate) AS charge_off_date
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE is_charged_off = 1
    GROUP BY loan_key
),

-- Valid collections after cohort entry, before charge-off
collections_after_entry AS (
    SELECT
        e.loan_key,
        e.cohort_week,
        SUM(c.total_collections) AS raw_total_collections
    FROM entry_to_bucket1 e
    LEFT JOIN collections_raw c
        ON e.loan_key = c.loan_key
       AND c.payment_date >= e.cohort_week
    LEFT JOIN charged_off_loans ch
        ON e.loan_key = ch.loan_key
    WHERE ch.charge_off_date IS NULL
       OR c.payment_date < ch.charge_off_date
    GROUP BY e.loan_key, e.cohort_week
),

collections_after_entry_120 AS (
    SELECT
        e.loan_key,
        e.cohort_week,
        SUM(c.total_collections) AS raw_total_collections_120
    FROM entry_to_bucket1 e
    LEFT JOIN collections_raw c
        ON e.loan_key = c.loan_key
       AND c.payment_date BETWEEN e.cohort_week AND DATEADD(day, 120, e.cohort_week)
    LEFT JOIN charged_off_loans ch
        ON e.loan_key = ch.loan_key
    WHERE ch.charge_off_date IS NULL
       OR c.payment_date < ch.charge_off_date
    GROUP BY e.loan_key, e.cohort_week
),

-- Combine all into one row per loan per cohort and apply allocation logic
loan_level_cohort_metrics AS (
    SELECT
        e.cohort_week,
        e.loan_key,
        e.entry_os,

        COALESCE(c.raw_total_collections, 0) AS raw_collections,
        COALESCE(c120.raw_total_collections_120, 0) AS raw_collections_120,

        COALESCE(ch.raw_chargeoff_os, 0) AS raw_chargeoff_os,

        -- Cap collections at entry_os
        LEAST(COALESCE(c.raw_total_collections, 0), e.entry_os) AS capped_collections,
        LEAST(COALESCE(c120.raw_total_collections_120, 0), e.entry_os) AS capped_collections_120,

        -- Cap chargeoff at remaining entry_os after collections
        CASE 
            WHEN ch.raw_chargeoff_os IS NOT NULL THEN 
                LEAST(ch.raw_chargeoff_os, GREATEST(e.entry_os - COALESCE(c.raw_total_collections, 0), 0))
            ELSE 0
        END AS capped_chargeoff_os
    FROM entry_to_bucket1 e
    LEFT JOIN collections_after_entry c
        ON e.loan_key = c.loan_key AND e.cohort_week = c.cohort_week
    LEFT JOIN collections_after_entry_120 c120
    ON e.loan_key = c120.loan_key AND e.cohort_week = c120.cohort_week
    LEFT JOIN chargeoff_os_values ch
        ON e.loan_key = ch.loan_key
),

-- Aggregate to cohort-week level
final_metrics AS (
    SELECT
        cohort_week,
        SUM(entry_os) AS total_entry_os,
        SUM(capped_collections) AS total_collections,
        SUM(capped_collections_120) AS total_collections_120,
        SUM(capped_chargeoff_os) AS total_chargeoff_os,
        ROUND(SUM(capped_collections) / NULLIF(SUM(entry_os), 0), 4) AS collection_flow_rate,
        ROUND(SUM(capped_collections_120) / NULLIF(SUM(entry_os), 0), 4) AS collection_flow_rate_120,
        ROUND(SUM(capped_chargeoff_os) / NULLIF(SUM(entry_os), 0), 4) AS chargeoff_flow_rate,
        ROUND((SUM(capped_collections) + SUM(capped_chargeoff_os)) / NULLIF(SUM(entry_os), 0), 4) AS total_flow_rate
    FROM loan_level_cohort_metrics
    WHERE entry_os > 0
    GROUP BY cohort_week
)

SELECT *
FROM final_metrics
ORDER BY cohort_week;

----------------------------------------------- Entering Collections ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_entry AS
WITH base AS (
SELECT
    loan_key,
    fbbid,
    week_end_date,
    outstanding_principal_due,
    dpd_days_corrected,
    lag_dpd_days_corrected,
    is_charged_off,
    lag_is_charged_off
FROM analytics.credit.loan_level_data_pb
),

entry_to_bucket1 AS (
SELECT
    loan_key,
    fbbid,
    week_end_date,
    outstanding_principal_due AS entry_os
FROM base
WHERE 
    dpd_days_corrected BETWEEN 1 AND 7
    AND is_charged_off = 0
    AND lag_dpd_days_corrected = 0
    AND lag_is_charged_off = 0
)
SELECT
    week_end_date,
    SUM(entry_os) AS sum_os_entering_collections
FROM entry_to_bucket1
GROUP BY 1
ORDER BY 1;

----------------------------------------------- Leaving Collections (Cured) ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_cured AS
WITH base AS (
SELECT
    loan_key,
    fbbid,
    week_end_date,
    outstanding_principal_due,
    lag_outstanding_principal_due,
    dpd_days_corrected,
    lag_dpd_days_corrected,
    is_charged_off,
    lag_is_charged_off
FROM analytics.credit.loan_level_data_pb
),

cured AS (
SELECT
    loan_key,
    fbbid,
    week_end_date,
    lag_outstanding_principal_due AS cured_os
FROM base
WHERE dpd_days_corrected = 0
    AND is_charged_off = 0
    AND lag_dpd_days_corrected BETWEEN 1 AND 91
    AND lag_is_charged_off = 0
)
SELECT
    week_end_date,
    SUM(cured_os) AS sum_os_leaving_collections
FROM cured
GROUP BY 1
ORDER BY 1;

----------------------------------------------- Final Table ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.km_collections_combined_rp AS
SELECT 
    a1.*,
    a2.TOTAL_NUM_CUST_CO,
    a2.NEW_NUM_CUST_CO,
    a2.TOTAL_SUM_OS_CO,
    a2.NEW_SUM_OS_CO,
    a2.TOTAL_NUM_LOANS_CO,
    a2.NEW_NUM_LOANS_CO,
    a2.POST_CO_RECOVERIES,
    a2.INTERNAL_TOTAL_NUM_CUST_CO,
    a2.EXTERNAL_TOTAL_NUM_CUST_CO,
    a2.INTERNAL_NEW_NUM_CUST_CO,
    a2.EXTERNAL_NEW_NUM_CUST_CO,
    a2.INTERNAL_TOTAL_SUM_OS_CO,
    a2.EXTERNAL_TOTAL_SUM_OS_CO,
    a2.INTERNAL_NEW_SUM_OS_CO,
    a2.EXTERNAL_NEW_SUM_OS_CO,
    a2.INTERNAL_TOTAL_NUM_LOANS_CO,
    a2.EXTERNAL_TOTAL_NUM_LOANS_CO,
    a2.INTERNAL_NEW_NUM_LOANS_CO,
    a2.EXTERNAL_NEW_NUM_LOANS_CO,
    a2.INTERNAL_POST_CO_RECOVERIES,
    a2.EXTERNAL_POST_CO_RECOVERIES,
    a2.POSTCO_NUM_SETTLEMENTS,
    a2.POSTCO_AMT_SETTLEMENTS,
    a2.INTERNAL_POSTCO_NUM_SETTLEMENTS,
    a2.EXTERNAL_POSTCO_NUM_SETTLEMENTS,
    a2.INTERNAL_POSTCO_AMT_SETTLEMENTS,
    a2.EXTERNAL_POSTCO_AMT_SETTLEMENTS,
    a2.POSTCO_NUM_COMPLETED_SETTLEMENTS,
    a2.POSTCO_AMT_COMPLETED_SETTLEMENTS,
    a2.INTERNAL_POSTCO_NUM_COMPLETED_SETTLEMENTS,
    a2.EXTERNAL_POSTCO_NUM_COMPLETED_SETTLEMENTS,
    a2.INTERNAL_POSTCO_AMT_COMPLETED_SETTLEMENTS,
    a2.EXTERNAL_POSTCO_AMT_COMPLETED_SETTLEMENTS,
    a2.RUNNING_POST_CO_RECOVERIES,
    a2.NET_TOTAL_SUM_OS_CO,
    a2.LAG_NET_TOTAL_SUM_OS_CO,
    a2.PERC_RECOVERED_THIS_WEEK,
    a2.PERC_RECOVERED_UNTIL_THIS_WEEK,
    a3.TOTAL_LOANS_ORIGINATED,
    a3.COUNT_FIRST_PAYMENT_DEFAULT,
    a3.COUNT_FIRST_TWO_PAYMENTS_DEFAULT,
    a3.COUNT_FIRST_THREE_PAYMENTS_DEFAULT,
    a3.COUNT_NO_FUNDING_AND_CHARGE_OFF,
    a3.COUNT_TOTAL_CHARGE_OFF_FOR_METRIC4,
    a3.TOTAL_NUM_BK,
    a3.TOTAL_SUM_BK,
    a3.NEW_NUM_BK,
    a3.NEW_SUM_BK,
    a4.TOTAL_ENTRY_OS,
    a4.TOTAL_CHARGEOFF_OS,
    a4.TOTAL_COLLECTIONS,
    a4.CHARGEOFF_FLOW_RATE,
    a4.COLLECTION_FLOW_RATE,
    a4.COLLECTION_FLOW_RATE_120,
    a5.SUM_OS_ENTERING_COLLECTIONS,
    a6.SUM_OS_LEAVING_COLLECTIONS

FROM analytics.credit.km_collections_preco_final a1
LEFT JOIN analytics.credit.collections_km_postco_final a2 on a1.week_end_date = a2.week_end_date
LEFT JOIN analytics.credit.km_collections_ops_final a3 on a1.week_end_date = a3.week_end_date
LEFT JOIN analytics.credit.km_collections_chof_payment_flow_rates a4 on a1.week_end_date = a4.cohort_week
LEFT JOIN analytics.credit.km_collections_entry a5 on a1.week_end_date = a5.week_end_date
LEFT JOIN analytics.credit.km_collections_cured a6 on a1.week_end_date = a6.week_end_date;
