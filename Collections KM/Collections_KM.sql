-- Collections Snapshot + Customer Level Metrics
-- Combines:
-- 1. Snapshot-level Pre-CO inventory (from Collections_New_Copy - Wednesday snapshot methodology)
-- 2. Customer roll rate with overdue balance and collections at customer level

-- ============================================================================================================
-- PART 1: SNAPSHOT LEVEL PRE-CO INVENTORY (from Collections_New_Copy)
-- ============================================================================================================
-- Select * from analytics.credit.km_snapshot_preco_metrics;


CREATE OR REPLACE TABLE analytics.credit.km_snapshot_preco_metrics AS
WITH date_bounds AS (
    SELECT 
        '2023-07-01'::date AS min_date,
        CURRENT_DATE() AS max_date
),

-- Filter for non-cancelled loans
first_table AS (
    SELECT 
        loan_key,
        loan_operational_status,
        ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) AS rnk
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    CROSS JOIN date_bounds db
    WHERE edate >= db.min_date
    QUALIFY rnk = 1
),

-- Base data from FMD with DPD correction and week calculations
finance_metrics AS (
    SELECT 
        fmd.loan_key,
        fmd.fbbid,
        fmd.edate,
        analytics.credit.get_week_end_date(fmd.edate) AS week_end_date,
        fmd.is_charged_off,
        fmd.OUTSTANDING_PRINCIPAL_DUE * COALESCE(flu.loan_fx_rate, 1.0) AS OUTSTANDING_PRINCIPAL_DUE,
        -- DPD correction logic
        CASE 
            WHEN fmd.is_charged_off = 1 AND fmd.DPD_days IS NULL THEN 98
            WHEN fmd.is_charged_off = 0 AND fmd.DPD_days IS NULL THEN 0 
            ELSE fmd.dpd_days 
        END AS dpd_days_corrected
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = fmd.LOAN_KEY
    CROSS JOIN date_bounds db
    WHERE fmd.PRODUCT_TYPE <> 'Flexpay'
        AND DAYOFWEEK(fmd.edate) = 3  -- Wednesday snapshots
        AND fmd.edate >= db.min_date
        AND (CASE WHEN fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%Term Loan%' THEN 1 ELSE 0 END) = 0
        AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
),

-- Weekly metrics with LAG calculations
weekly_metrics AS (
    SELECT 
        fbbid,
        loan_key,
        week_end_date,
        week_end_date - 6 AS week_start_date,
        is_charged_off,
        OUTSTANDING_PRINCIPAL_DUE,
        dpd_days_corrected,
        
        -- LAG calculations
        COALESCE(LAG(dpd_days_corrected) OVER (PARTITION BY loan_key ORDER BY week_end_date), 0) AS lag_dpd_days_corrected,
        COALESCE(LAG(is_charged_off) OVER (PARTITION BY loan_key ORDER BY week_end_date), 0) AS lag_is_charged_off,
        COALESCE(LAG(OUTSTANDING_PRINCIPAL_DUE) OVER (PARTITION BY loan_key ORDER BY week_end_date), 0) AS lag_outstanding_principal_due
    FROM finance_metrics
),

-- Base with bucket calculations
base AS (
    SELECT
        loan_key,
        fbbid,
        week_end_date,
        week_start_date,
        OUTSTANDING_PRINCIPAL_DUE,
        lag_outstanding_principal_due,
        
        CASE WHEN dpd_days_corrected IS NULL AND is_charged_off IS NULL THEN 0
        ELSE dpd_days_corrected
        END AS dpd_days_corrected,
        
        CASE WHEN lag_dpd_days_corrected IS NULL AND lag_is_charged_off IS NULL THEN 0
        ELSE lag_dpd_days_corrected
        END AS lag_dpd_days_corrected,
        
        is_charged_off,
        lag_is_charged_off,
        
        -- Current bucket group
        CASE
            WHEN dpd_days_corrected = 0 AND is_charged_off = 0 THEN '00. Bucket 0'
            WHEN dpd_days_corrected BETWEEN 1 AND 14 AND is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN dpd_days_corrected BETWEEN 15 AND 56 AND is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN dpd_days_corrected BETWEEN 57 AND 91 AND is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN dpd_days_corrected <= 98 AND is_charged_off = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        
        -- Previous bucket group
        CASE 
            WHEN lag_dpd_days_corrected = 0 AND lag_is_charged_off = 0 THEN '00. Bucket 0'
            WHEN lag_dpd_days_corrected BETWEEN 1 AND 14 AND lag_is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN lag_dpd_days_corrected BETWEEN 15 AND 56 AND lag_is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN lag_dpd_days_corrected BETWEEN 57 AND 91 AND lag_is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN lag_dpd_days_corrected <= 98 OR lag_is_charged_off = 1 THEN '04. CHOF'
        END AS prev_dpd_bucket_group,
        
        -- OS bucket calculations (replaces os_0_90, os_1_90, os_p_1_90 from pull table)
        CASE WHEN is_charged_off = 0 AND dpd_days_corrected BETWEEN 0 AND 91 
             THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_0_90,
        CASE WHEN is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 91 
             THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_1_90,
        CASE WHEN lag_is_charged_off = 0 AND lag_dpd_days_corrected BETWEEN 1 AND 91 
             THEN lag_outstanding_principal_due ELSE 0 END AS os_p_1_90
             
    FROM weekly_metrics
),

-- Total overdue balance from LOAN_STATUSES
total_overdue AS (
    SELECT
        T2.EDATE AS week_end_date,
        T1.FBBID,
        T1.LOAN_KEY,
        SUM(TO_DOUBLE(T1.STATUS_VALUE) * COALESCE(flu.loan_fx_rate, 1.0)) AS total_overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = T1.LOAN_KEY
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

combined AS (
    SELECT
        b.*,
        odb.total_overdue_balance
    FROM base b
    LEFT JOIN total_overdue odb
        ON b.fbbid = odb.fbbid 
        AND b.loan_key = odb.loan_key 
        AND b.week_end_date = odb.week_end_date
),

aggregated AS (
    SELECT
        week_start_date,
        week_end_date,

        SUM(os_1_90) AS pre_co_total_os,
        SUM(os_0_90) AS total_os_due,

        -- Outstanding principal by bucket
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN OUTSTANDING_PRINCIPAL_DUE END) AS sum_os_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN OUTSTANDING_PRINCIPAL_DUE END) AS sum_os_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN OUTSTANDING_PRINCIPAL_DUE END) AS sum_os_9_13,

        -- Overdue balance by bucket
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN total_overdue_balance END) AS sum_odb_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN total_overdue_balance END) AS sum_odb_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN total_overdue_balance END) AS sum_odb_9_13,
        SUM(CASE WHEN dpd_bucket_group IN ('01. Bucket 1-2','02. Bucket 3-8','03. Bucket 9-13') 
            THEN total_overdue_balance END) AS sum_odb_1_90,

        -- Customer counts
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS num_cust_1_2,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS num_cust_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS num_cust_9_13,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group IN ('01. Bucket 1-2','02. Bucket 3-8','03. Bucket 9-13') 
            THEN fbbid END) AS num_cust_1_13

    FROM combined
    WHERE dpd_bucket_group IS NOT NULL
    GROUP BY week_start_date, week_end_date
)

SELECT 
    week_start_date,
    week_end_date,
    
    -- Inventory counts
    num_cust_1_2,
    num_cust_3_8,
    num_cust_9_13,
    num_cust_1_13,
    
    -- Outstanding
    pre_co_total_os AS pre_co_total_os_snapshot,
    total_os_due AS total_os_due_snapshot,
    sum_os_1_2 AS sum_os_1_2_snapshot,
    sum_os_3_8 AS sum_os_3_8_snapshot,
    sum_os_9_13 AS sum_os_9_13_snapshot,
    
    -- Overdue Balance
    sum_odb_1_2 AS sum_odb_1_2_snapshot,
    sum_odb_3_8 AS sum_odb_3_8_snapshot,
    sum_odb_9_13 AS sum_odb_9_13_snapshot,
    sum_odb_1_90 AS total_odb_due_snapshot

FROM aggregated
ORDER BY week_start_date, week_end_date;




-- Select * from analytics.credit.km_customer_roll_metrics_weekly
-- ============================================================================================================
-- PART 2: CUSTOMER LEVEL ROLL RATE METRICS (Weekly methodology with overdue balance and collections)
-- ============================================================================================================
CREATE OR REPLACE TABLE analytics.credit.km_customer_roll_metrics_weekly AS
WITH fmd_agg AS (
    -- Aggregate to CUSTOMER level: MAX DPD across all loans, SUM outstanding
    SELECT
        fmd.fbbid,
        fmd.edate,
        MAX(fmd.dpd_days) AS dpd_days,
        MAX(fmd.dpd_bucket) AS dpd_bucket,
        SUM(fmd.outstanding_principal_due * COALESCE(flu.loan_fx_rate, 1.0)) AS outstanding_principal_due,
        MAX(fmd.is_charged_off) AS is_charged_off
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.edate >= '2020-10-01'
    AND fmd.PRODUCT_TYPE <> 'Flexpay'
    GROUP BY fmd.fbbid, fmd.edate
)
--Select sum(outstanding_principal_due) from fmd_agg where edate = '2026-02-04' and is_charged_off = 0;

-- Customer level overdue balance
,
Total_os as
(
select 
DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS collection_week,
sum(outstanding_principal_due) as total_os_due_snapshot
from fmd_agg 
where (dpd_days>=0 or dpd_days is null) and is_charged_off = 0
and DAYOFWEEK(EDATE) = 3
group by 1
)
-- Select collection_week, 
-- total_os_due_snapshot
-- from Total_os 
-- order by 1 desc;


,customer_overdue AS (
    SELECT
        T2.EDATE,
        T1.FBBID,
        SUM(TO_DOUBLE(T1.STATUS_VALUE) * COALESCE(flu.loan_fx_rate, 1.0)) AS total_overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = T1.LOAN_KEY
    JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND T2.EDATE >= '2020-10-01'
    JOIN (
        SELECT DISTINCT loan_key, edate, fbbid
        FROM BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE loan_operational_status <> 'CNCL'
          AND is_charged_off = 0
    ) t3
        ON t3.loan_key = T1.loan_key
        AND t3.edate = T2.EDATE
    WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
    GROUP BY 1, 2
),

--------------------------------------------------------------------------------
-- Past Due Payments at CUSTOMER level (cured payments from delinquent accounts)
--------------------------------------------------------------------------------
fmd_agg_payments AS (
    SELECT
        fmd.loan_key,
        fmd.fbbid,
        fmd.edate,
        MAX(fmd.dpd_days) AS dpd_days,
        MAX(fmd.dpd_bucket) AS dpd_bucket,
        MAX(fmd.is_charged_off) AS is_charged_off_any
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    WHERE edate >= '2020-01-01'
    GROUP BY 1, 2, 3
),

fmd_with_lag AS (
    SELECT 
        *,
        ZEROIFNULL(LAG(dpd_bucket) OVER (PARTITION BY loan_key ORDER BY edate)) AS prev_dpd_bucket
    FROM fmd_agg_payments
),

fmd_final AS (
    SELECT 
        *,
        CASE 
            WHEN dpd_bucket < prev_dpd_bucket THEN 'DROP IN DPD' 
        END AS CHECKING
    FROM fmd_with_lag
),

t2 AS (
    SELECT * FROM fmd_final
    WHERE CHECKING = 'DROP IN DPD'
    OR (prev_dpd_bucket > 0)
),

T3 AS (
    SELECT DISTINCT T2.*,
        pm_1.payment_planned_transmission_date,
        PM_1.payment_event_time,
        PM_1.PAYMENT_ID, 
        PM_1.RELATED_SERVICE_ID,
        PM_1.PAYMENT_TYPE,
        PM_1.PAYMENT_DESCRIPTION,
        PM_1.PAYMENT_METHOD_TYPE,
        PM_1.PARENT_PAYMENT_ID,
        TO_DOUBLE(PM_1.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) * COALESCE(flu_pm.loan_fx_rate, 1.0) AS payment_amount,
        TO_DOUBLE(PM_1.payment_components_json:Principal) * COALESCE(flu_pm.loan_fx_rate, 1.0) AS Principal
    FROM BI.FINANCE.PAYMENTS_MODEL PM_1
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_pm ON flu_pm.LOAN_KEY = PM_1.LOAN_KEY
    INNER JOIN T2
        ON PM_1.LOAN_KEY = T2.LOAN_KEY
        AND CASE WHEN PM_1.PAYMENT_METHOD_TYPE = 'CC' THEN PM_1.PAYMENT_STATUS = 'FUND'
            ELSE PM_1.PAYMENT_STATUS IN ('AUTH','TRNS') END
        AND PM_1.payment_event_time::DATE = T2.EDATE
        AND PM_1.DIRECTION = 'D'
    WHERE EXISTS (
        SELECT 1 FROM BI.FINANCE.PAYMENTS_MODEL PM_2
        WHERE PM_1.PAYMENT_ID = PM_2.PAYMENT_ID AND PM_2.PAYMENT_STATUS = 'FUND'
    )
),

past_due_payments_cust AS (
    SELECT 
        FBBID,
        DATE_TRUNC('WEEK', payment_event_time::DATE + 4)::DATE + 2 AS collection_week,
        SUM(payment_amount) AS past_due_payment,
        SUM(Principal) AS past_due_principal_paid
    FROM T3
    GROUP BY 1, 2
),

--------------------------------------------------------------------------------
-- Total Collections at CUSTOMER level
--------------------------------------------------------------------------------
collections_deduped AS (
    SELECT 
        A1.*,
        flu_cd.loan_fx_rate,
        ROW_NUMBER() OVER (PARTITION BY PAYMENT_ID ORDER BY PAYMENT_EVENT_TIME ASC) AS rn
    FROM BI.FINANCE.payments_model A1
    LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY A2 
        ON A1.LOAN_KEY = A2.LOAN_KEY
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_cd ON flu_cd.LOAN_KEY = A1.LOAN_KEY
    WHERE
        A2.PRODUCT_TYPE <> 'Flexpay'
        AND A2.LOAN_CREATED_DATE = A2.EDATE
        AND A1.PAYMENT_STATUS = 'FUND'
        AND A1.DIRECTION = 'D'
        AND A1.PAYMENT_EVENT_TIME::DATE >= '2020-01-01'
)
,

total_collections_cust AS (
    SELECT 
        DATE_TRUNC('WEEK', PAYMENT_EVENT_TIME::DATE + 4)::DATE + 2 AS collection_week,
        FBBID,
        sum(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) * COALESCE(loan_fx_rate, 1.0)) AS total_collections,
        sum(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PRINCIPAL) * COALESCE(loan_fx_rate, 1.0)) AS total_principal_collected
    FROM collections_deduped
    WHERE rn = 1
    GROUP BY 1, 2
),

--------------------------------------------------------------------------------
-- Customer Roll Rate Base
--------------------------------------------------------------------------------
base AS (
    SELECT 
        *,
        CASE
            WHEN dpd_bucket = 0 AND is_charged_off = 0 THEN '00. Bucket 0'
            WHEN dpd_bucket IN (1, 2) AND is_charged_off = 0 THEN '01. Bucket 1-2'
            WHEN dpd_bucket IN (3, 4, 5, 6, 7, 8) AND is_charged_off = 0 THEN '02. Bucket 3-8'
            WHEN dpd_bucket IN (9, 10, 11, 12, 13) AND is_charged_off = 0 THEN '03. Bucket 9-13'
            WHEN is_charged_off = 1 THEN '04. CHOF'
        END AS dpd_bucket_group,
        DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS collection_week
    FROM fmd_agg
    WHERE is_charged_off = 0 AND dpd_bucket > 0
),

-- First delinquent day in the week for each CUSTOMER
base1 AS (
    SELECT * 
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid, collection_week ORDER BY edate) = 1
),

-- Get overdue balance for first delinquent day
base_with_odb AS (
    SELECT 
        b.*,
        o.total_overdue_balance
    FROM base1 b
    LEFT JOIN customer_overdue o
        ON b.fbbid = o.fbbid AND b.edate = o.edate
),

-- Check if cured by end of week (day after week end) AND get Wednesday status
cured AS (
    SELECT l.* FROM (
        SELECT 
            a.*,
            b.dpd_bucket AS cured_dpd_bucket,
            b.is_charged_off AS cured_charged_off,
            c.dpd_bucket AS dpd_as_of_wed,
            c.outstanding_principal_due AS os_as_of_wed,
            c.is_charged_off  as charged_off_as_of_wed,
            d.total_overdue_balance AS odb_as_of_wed,
            CASE WHEN b.dpd_bucket = 0 THEN 1 ELSE 0 END AS cured,
            ROW_NUMBER() OVER (PARTITION BY a.fbbid, a.collection_week ORDER BY b.edate) AS rn
        FROM base_with_odb a
        LEFT JOIN fmd_agg b
            ON a.fbbid = b.fbbid 
            AND a.collection_week = DATEADD(day, -1, b.edate)
        LEFT JOIN fmd_agg c
            ON a.fbbid = c.fbbid
            AND a.collection_week = c.edate
        LEFT JOIN customer_overdue d
            ON a.fbbid = d.fbbid
            AND a.collection_week = d.edate
    ) l WHERE rn = 1
)
-- Select * from cured
-- where collection_week = '2026-02-04' ;
-- and dpd_as_of_wed in (3,4,5,6,7,8) ;
-- and dpd_bucket not in (3,4,5,6,7,8);
 

-- Add collections to cured
,with_collections AS (
    SELECT 
        c.*,
        pdp.past_due_payment,
        pdp.past_due_principal_paid,
        tc.total_collections,
        tc.total_principal_collected
    FROM cured c
    LEFT JOIN past_due_payments_cust pdp
        ON c.fbbid = pdp.fbbid AND c.collection_week = pdp.collection_week
    LEFT JOIN total_collections_cust tc
        ON c.fbbid = tc.fbbid AND c.collection_week = tc.collection_week
),

-- Within-week transitions at CUSTOMER level
with_transitions AS (
    SELECT
        *,
        CASE 
            -- From Bucket 1-2 during the week
            WHEN dpd_bucket IN (1, 2) AND cured_dpd_bucket = 0 THEN 'Cure_1_2'
            WHEN dpd_as_of_wed IN (1, 2) THEN 'Stay_1_2'
            WHEN dpd_bucket IN (1, 2) AND cured_dpd_bucket IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) THEN 'Worsen_1_2'

            -- From Bucket 3-8 during the week
            WHEN dpd_bucket IN (3, 4, 5, 6, 7, 8) AND cured_dpd_bucket = 0 THEN 'Cure_3_8'
            WHEN dpd_bucket IN (3, 4, 5, 6, 7, 8) AND cured_dpd_bucket IN (1, 2) THEN 'Improve_3_8'
            WHEN dpd_as_of_wed IN (3, 4, 5, 6, 7, 8) THEN 'Stay_3_8'
            WHEN dpd_bucket IN (3, 4, 5, 6, 7, 8) AND cured_dpd_bucket IN (9, 10, 11, 12, 13) THEN 'Worsen_3_8'

            -- From Bucket 9-13 during the week
            WHEN dpd_bucket IN (9, 10, 11, 12, 13) AND cured_dpd_bucket = 0 THEN 'Cure_9_13'
            WHEN dpd_bucket IN (9, 10, 11, 12, 13) AND cured_dpd_bucket IN (1, 2, 3, 4, 5, 6, 7, 8) THEN 'Improve_9_13'
            WHEN dpd_as_of_wed IN (9, 10, 11, 12, 13) THEN 'Stay_9_13'
            WHEN dpd_bucket IN (9, 10, 11, 12, 13) AND cured_charged_off = 1 THEN 'Worsen_9_13'

            ELSE NULL
        END AS transition_type
    FROM with_collections
),

-- Final aggregation at CUSTOMER level
final_aggregated AS (
    SELECT
        collection_week - 6 AS week_start_date,
        collection_week AS week_end_date,
        
        COUNT(DISTINCT CASE WHEN dpd_as_of_wed in (1,2) THEN fbbid END) AS num_cust_1_2_snapshot,
        COUNT(DISTINCT CASE WHEN dpd_as_of_wed in (3,4,5,6,7,8) THEN fbbid END) AS num_cust_3_8_snapshot,
        COUNT(DISTINCT CASE WHEN dpd_as_of_wed in (9,10,11,12,13) THEN fbbid END) AS num_cust_9_13_snapshot,

        COUNT(DISTINCT CASE WHEN dpd_as_of_wed in (1,2,3,4,5,6,7,8,9,10,11,12,13) THEN fbbid END) AS Pre_co_num_cust_snapshot,
    

        SUM(CASE WHEN dpd_as_of_wed IN (1, 2) THEN os_as_of_wed END) AS sum_os_1_2_snapshot,
        SUM(CASE WHEN dpd_as_of_wed IN (3,4,5,6,7,8) THEN os_as_of_wed END) AS sum_os_3_8_snapshot,
        SUM(CASE WHEN dpd_as_of_wed IN (9,10,11,12,13) THEN os_as_of_wed END) AS sum_os_9_13_snapshot,
        
        SUM(CASE WHEN dpd_as_of_wed IN (1,2,3,4,5,6,7,8,9,10,11,12,13) THEN os_as_of_wed END) AS sum_os_1_90_snapshot,

        
        
        SUM(CASE WHEN dpd_as_of_wed IN (1, 2) THEN odb_as_of_wed END) AS sum_odb_1_2_snapshot,
        SUM(CASE WHEN dpd_as_of_wed IN (3,4,5,6,7,8) THEN odb_as_of_wed END) AS sum_odb_3_8_snapshot,
        SUM(CASE WHEN dpd_as_of_wed IN (9,10,11,12,13) THEN odb_as_of_wed END) AS sum_odb_9_13_snapshot,
        
        SUM(CASE WHEN dpd_as_of_wed IN (1,2,3,4,5,6,7,8,9,10,11,12,13) THEN odb_as_of_wed END) AS sum_odb_1_90_snapshot,

        

        -------------------- CUSTOMER COUNTS --------------------
        -- Weekly customer counts (anyone delinquent during the week)
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN fbbid END) AS num_cust_1_2,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN fbbid END) AS num_cust_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN fbbid END) AS num_cust_9_13,

        -- Cured customer counts
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN fbbid END) AS num_cust_cured_1_2,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN fbbid END) AS num_cust_cured_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN fbbid END) AS num_cust_cured_9_13,

        -- Delinquent as of Wednesday customer counts
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (1,2) THEN fbbid END) AS num_cust_delinq_wed_1_2,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN fbbid END) AS num_cust_delinq_wed_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (9,10,11,12,13) THEN fbbid END) AS num_cust_delinq_wed_9_13,

        ---Roll as of wednesday 
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN fbbid END) AS num_cust_roll_wed_1_2,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (9,10,11,12,13) THEN fbbid END) AS num_cust_roll_wed_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND charged_off_as_of_wed = 1 THEN fbbid END) AS num_cust_roll_wed_9_13,

        ---Improved as of wednesday
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (1,2) THEN fbbid END) AS num_cust_Improved_wed_3_8,
        COUNT(DISTINCT CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN fbbid END) AS num_cust_Improved_wed_9_13,
        

        -------------------- OUTSTANDING PRINCIPAL --------------------
        -- Weekly OS (Total)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN outstanding_principal_due END) AS sum_os_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN outstanding_principal_due END) AS sum_os_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN outstanding_principal_due END) AS sum_os_9_13,

        -- Cured OS (started in bucket, cured by Wednesday)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN outstanding_principal_due END) AS sum_os_cured_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN outstanding_principal_due END) AS sum_os_cured_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN outstanding_principal_due END) AS sum_os_cured_9_13,

        -- Delinquent OS as of Wednesday (still delinquent)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (1,2) THEN outstanding_principal_due END) AS sum_os_delinq_wed_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN outstanding_principal_due END) AS sum_os_delinq_wed_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (9,10,11,12,13) THEN outstanding_principal_due END) AS sum_os_delinq_wed_9_13,

        ---Roll as of wednesday 
        Sum(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN outstanding_principal_due END) AS sum_os_roll_wed_1_2,
        Sum(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (9,10,11,12,13) THEN outstanding_principal_due END) AS sum_os_roll_wed_3_8,
        Sum(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND charged_off_as_of_wed = 1 THEN outstanding_principal_due END) AS sum_os_roll_wed_9_13,

        ---Improved as of wednesday
        Sum(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (1,2) THEN outstanding_principal_due END) AS sum_os_Improved_wed_3_8,
        Sum(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN outstanding_principal_due END) AS sum_os_Improved_wed_9_13,

        
        -------------------- OVERDUE BALANCE --------------------
        -- Weekly Overdue Balance (Total)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN total_overdue_balance END) AS sum_odb_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN total_overdue_balance END) AS sum_odb_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN total_overdue_balance END) AS sum_odb_9_13,

        -- Cured Overdue Balance (started in bucket, cured by Wednesday)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN total_overdue_balance END) AS sum_odb_cured_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN total_overdue_balance END) AS sum_odb_cured_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND (dpd_as_of_wed = 0 OR dpd_as_of_wed IS NULL) THEN total_overdue_balance END) AS sum_odb_cured_9_13,

        -- Delinquent Overdue Balance as of Wednesday (still delinquent)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (1,2) THEN total_overdue_balance END) AS sum_odb_delinq_wed_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN total_overdue_balance END) AS sum_odb_delinq_wed_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (9,10,11,12,13) THEN total_overdue_balance END) AS sum_odb_delinq_wed_9_13,

        Sum(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN total_overdue_balance END) AS sum_odb_roll_wed_1_2,
        Sum(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (9,10,11,12,13) THEN total_overdue_balance END) AS sum_odb_roll_wed_3_8,
        Sum(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND charged_off_as_of_wed = 1 THEN total_overdue_balance END) AS sum_odb_roll_wed_9_13,

        ---Improved as of wednesday
        Sum(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' AND dpd_as_of_wed in (1,2) THEN total_overdue_balance END) AS sum_odb_Improved_wed_3_8,
        Sum(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' AND dpd_as_of_wed in (3,4,5,6,7,8) THEN total_overdue_balance END) AS sum_odb_Improved_wed_9_13,
        
        -------------------- COLLECTIONS --------------------
        -- Past Due Payments (from delinquent accounts - cured payments)
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN past_due_payment END) AS past_due_collected_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN past_due_payment END) AS past_due_collected_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN past_due_payment END) AS past_due_collected_9_13,

        -- Total Collections
        SUM(CASE WHEN dpd_bucket_group = '01. Bucket 1-2' THEN total_collections END) AS total_collected_1_2,
        SUM(CASE WHEN dpd_bucket_group = '02. Bucket 3-8' THEN total_collections END) AS total_collected_3_8,
        SUM(CASE WHEN dpd_bucket_group = '03. Bucket 9-13' THEN total_collections END) AS total_collected_9_13,

        -------------------- TRANSITION COUNTS --------------------
        COUNT(DISTINCT CASE WHEN transition_type = 'Cure_1_2' THEN fbbid END) AS num_cured_1_2,
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

        -------------------- TRANSITION OS --------------------
        SUM(CASE WHEN transition_type = 'Cure_1_2' THEN outstanding_principal_due END) AS os_cured_1_2,
        SUM(CASE WHEN transition_type = 'Stay_1_2' THEN outstanding_principal_due END) AS os_stayed_1_2,
        SUM(CASE WHEN transition_type = 'Worsen_1_2' THEN outstanding_principal_due END) AS os_worsened_1_2,

        SUM(CASE WHEN transition_type = 'Cure_3_8' THEN outstanding_principal_due END) AS os_cured_3_8,
        SUM(CASE WHEN transition_type = 'Improve_3_8' THEN outstanding_principal_due END) AS os_improved_3_8,
        SUM(CASE WHEN transition_type = 'Stay_3_8' THEN outstanding_principal_due END) AS os_stayed_3_8,
        SUM(CASE WHEN transition_type = 'Worsen_3_8' THEN outstanding_principal_due END) AS os_worsened_3_8,

        SUM(CASE WHEN transition_type = 'Cure_9_13' THEN outstanding_principal_due END) AS os_cured_9_13,
        SUM(CASE WHEN transition_type = 'Improve_9_13' THEN outstanding_principal_due END) AS os_improved_9_13,
        SUM(CASE WHEN transition_type = 'Stay_9_13' THEN outstanding_principal_due END) AS os_stayed_9_13,
        SUM(CASE WHEN transition_type = 'Worsen_9_13' THEN outstanding_principal_due END) AS os_worsened_9_13

    FROM with_transitions 
    GROUP BY collection_week
    ORDER BY collection_week
)

SELECT 
    week_start_date,
    week_end_date,
    -------------------- G.24: # CUSTOMERS --------------------
    num_cust_1_2,
    num_cust_3_8,
    num_cust_9_13,
    num_cust_cured_1_2,
    num_cust_cured_3_8,
    num_cust_cured_9_13,
    num_cust_delinq_wed_1_2,
    num_cust_delinq_wed_3_8,
    num_cust_delinq_wed_9_13,

    -- Added: Roll/Improvement Customer Counts
    num_cust_roll_wed_1_2,
    num_cust_roll_wed_3_8,
    num_cust_roll_wed_9_13,
    num_cust_Improved_wed_3_8,
    num_cust_Improved_wed_9_13,
    
    -------------------- G.25: $ OUTSTANDING PRINCIPAL --------------------
    sum_os_1_2,
    sum_os_3_8,
    sum_os_9_13,
    sum_os_cured_1_2,
    sum_os_cured_3_8,
    sum_os_cured_9_13,
    sum_os_delinq_wed_1_2,
    sum_os_delinq_wed_3_8,
    sum_os_delinq_wed_9_13,

    -- Added: Roll/Improvement OS
    sum_os_roll_wed_1_2,
    sum_os_roll_wed_3_8,
    sum_os_roll_wed_9_13,
    sum_os_Improved_wed_3_8,
    sum_os_Improved_wed_9_13,
    
    -------------------- G.26: % OUTSTANDING PRINCIPAL --------------------
    COALESCE(sum_os_cured_1_2 / NULLIF(sum_os_1_2, 0), 0) AS pct_os_cured_1_2,
    COALESCE(sum_os_delinq_wed_1_2 / NULLIF(sum_os_1_2, 0), 0) AS pct_os_delinq_wed_1_2,
    COALESCE(sum_os_cured_3_8 / NULLIF(sum_os_3_8, 0), 0) AS pct_os_cured_3_8,
    COALESCE(sum_os_delinq_wed_3_8 / NULLIF(sum_os_3_8, 0), 0) AS pct_os_delinq_wed_3_8,
    COALESCE(sum_os_cured_9_13 / NULLIF(sum_os_9_13, 0), 0) AS pct_os_cured_9_13,
    COALESCE(sum_os_delinq_wed_9_13 / NULLIF(sum_os_9_13, 0), 0) AS pct_os_delinq_wed_9_13,
    
    -------------------- G.27: $ OVERDUE BALANCE --------------------
    sum_odb_1_2,
    sum_odb_3_8,
    sum_odb_9_13,
    sum_odb_cured_1_2,
    sum_odb_cured_3_8,
    sum_odb_cured_9_13,
    sum_odb_delinq_wed_1_2,
    sum_odb_delinq_wed_3_8,
    sum_odb_delinq_wed_9_13,

    -- Added: Roll/Improvement Overdue Balance
    sum_odb_roll_wed_1_2,
    sum_odb_roll_wed_3_8,
    sum_odb_roll_wed_9_13,
    sum_odb_Improved_wed_3_8,
    sum_odb_Improved_wed_9_13,
    
    -------------------- G.28: % OVERDUE BALANCE --------------------
    COALESCE(sum_odb_1_2 / NULLIF(sum_os_1_2, 0), 0) AS pct_odb_1_2,
    COALESCE(sum_odb_3_8 / NULLIF(sum_os_3_8, 0), 0) AS pct_odb_3_8,
    COALESCE(sum_odb_9_13 / NULLIF(sum_os_9_13, 0), 0) AS pct_odb_9_13,
    
    -------------------- G.29-G.31: % ROLL RATES (Count based) --------------------
    -- % that became current this week
    COALESCE(num_cured_1_2 / NULLIF(num_cust_1_2, 0), 0) AS cure_rate_1_2_pct,
    COALESCE(num_cured_3_8 / NULLIF(num_cust_3_8, 0), 0) AS cure_rate_3_8_pct,
    COALESCE(num_cured_9_13 / NULLIF(num_cust_9_13, 0), 0) AS cure_rate_9_13_pct,
    
    -- % that stayed in the same bucket group
    COALESCE(num_stayed_1_2 / NULLIF(num_cust_1_2, 0), 0) AS stay_rate_1_2_pct,
    COALESCE(num_stayed_3_8 / NULLIF(num_cust_3_8, 0), 0) AS stay_rate_3_8_pct,
    COALESCE(num_stayed_9_13 / NULLIF(num_cust_9_13, 0), 0) AS stay_rate_9_13_pct,
    
    -- % that moved into a higher bucket group or CO
    COALESCE(num_worsened_1_2 / NULLIF(num_cust_1_2, 0), 0) AS worsen_rate_1_2_pct,
    COALESCE(num_worsened_3_8 / NULLIF(num_cust_3_8, 0), 0) AS worsen_rate_3_8_pct,
    COALESCE(num_worsened_9_13 / NULLIF(num_cust_9_13, 0), 0) AS worsen_rate_9_13_pct,


    COALESCE(num_improved_3_8 / NULLIF(num_cust_3_8, 0), 0) AS improve_rate_3_8_pct,
    COALESCE(num_improved_9_13 / NULLIF(num_cust_9_13, 0), 0) AS improve_rate_9_13_pct,
    
    
    -------------------- G.32-G.34: $ ROLL RATES (OS based) --------------------
    -- $ that became current this week
    COALESCE(os_cured_1_2 / NULLIF(sum_os_1_2, 0), 0) AS cure_rate_1_2_pct_os,
    COALESCE(os_cured_3_8 / NULLIF(sum_os_3_8, 0), 0) AS cure_rate_3_8_pct_os,
    COALESCE(os_cured_9_13 / NULLIF(sum_os_9_13, 0), 0) AS cure_rate_9_13_pct_os,
    
    -- $ that stayed in the same bucket group
    COALESCE(os_stayed_1_2 / NULLIF(sum_os_1_2, 0), 0) AS stay_rate_1_2_pct_os,
    COALESCE(os_stayed_3_8 / NULLIF(sum_os_3_8, 0), 0) AS stay_rate_3_8_pct_os,
    COALESCE(os_stayed_9_13 / NULLIF(sum_os_9_13, 0), 0) AS stay_rate_9_13_pct_os,
    
    -- $ that moved into a higher bucket group or CO
    COALESCE(os_worsened_1_2 / NULLIF(sum_os_1_2, 0), 0) AS worsen_rate_1_2_pct_os,
    COALESCE(os_worsened_3_8 / NULLIF(sum_os_3_8, 0), 0) AS worsen_rate_3_8_pct_os,
    COALESCE(os_worsened_9_13 / NULLIF(sum_os_9_13, 0), 0) AS worsen_rate_9_13_pct_os,
    
    COALESCE(os_Improved_3_8 / NULLIF(sum_os_3_8, 0), 0) AS Improve_rate_3_8_pct_os,
    COALESCE(os_improved_9_13 / NULLIF(sum_os_9_13, 0), 0) AS Improve_rate_9_13_pct_os,
    
    -------------------- G.35: $ PAYMENTS COLLECTED --------------------
    past_due_collected_1_2,
    past_due_collected_3_8,
    past_due_collected_9_13,
    total_collected_1_2,
    total_collected_3_8,
    total_collected_9_13,
    
    -------------------- G.36: % PAYMENTS COLLECTED --------------------
    COALESCE(past_due_collected_1_2 / NULLIF(sum_os_1_2, 0), 0) AS past_due_collection_rate_1_2_pct,
    COALESCE(past_due_collected_3_8 / NULLIF(sum_os_3_8, 0), 0) AS past_due_collection_rate_3_8_pct,
    COALESCE(past_due_collected_9_13 / NULLIF(sum_os_9_13, 0), 0) AS past_due_collection_rate_9_13_pct,
    COALESCE(total_collected_1_2 / NULLIF(sum_os_1_2, 0), 0) AS total_collection_rate_1_2_pct,
    COALESCE(total_collected_3_8 / NULLIF(sum_os_3_8, 0), 0) AS total_collection_rate_3_8_pct,
    COALESCE(total_collected_9_13 / NULLIF(sum_os_9_13, 0), 0) AS total_collection_rate_9_13_pct

FROM final_aggregated a
left join Total_os b 
on a.week_end_date = b.collection_week;


---Settlement 
CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_settlements AS
WITH all_offers AS (
    SELECT 
        cfs.fbbid,
        DATE_TRUNC('WEEK', cfs.edate::DATE + 4)::DATE + 2 AS week_end_date,
        DATEADD(day,-6, DATE_TRUNC('WEEK', cfs.edate::DATE + 4)::DATE + 2) AS week_start_date,
        cfs.edate,
        cfs.status_name,
        CASE WHEN cfs.status_name = 'SETTLEMENT_STATUS' 
                  AND cfs.status_value = 'FUNDED' THEN cfs.status_value END AS settlement_status_funded,
        CASE WHEN cfs.status_name = 'FINAL_SETTLEMENT_AMOUNT' 
                  THEN cfs.status_value::NUMERIC * COALESCE(fcu.fx_rate, 1.0) END AS settlement_amount
    FROM bi.finance.customer_finance_statuses cfs
    LEFT JOIN INDUS.PUBLIC.FX_CUSTOMER_UNIFIED fcu
        ON fcu.FBBID = cfs.FBBID
        AND fcu.EXCHANGE_DATE = cfs.EDATE::DATE
    WHERE cfs.status_group = 'DISCOUNTED_SETTLEMENT'
      AND (
            cfs.status_name = 'FINAL_SETTLEMENT_AMOUNT'
         OR cfs.status_name = 'SETTLEMENT_STATUS'
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


-- ============================================================================================================
-- PART 4: COMBINED FINAL TABLE
-- ============================================================================================================
CREATE OR REPLACE TABLE analytics.credit.km_collections_preco_final_weekly AS
SELECT 
    -- Use snapshot table as base for week dates
    COALESCE(s.week_start_date, c.week_start_date) AS week_start_date,
    COALESCE(s.week_end_date, c.week_end_date) AS week_end_date,
    
    -------------------- SNAPSHOT LEVEL METRICS (Loan Level) --------------------
    s.num_cust_1_13              AS snapshot_total_delinq_cust,
    s.pre_co_total_os_snapshot   AS snapshot_total_os_1_90,
    s.total_odb_due_snapshot     AS snapshot_total_odb_1_90,
    s.total_os_due_snapshot,
    
    -- Snapshot OS Splits
    s.sum_os_1_2_snapshot,
    s.sum_os_3_8_snapshot,
    s.sum_os_9_13_snapshot,

    -- Snapshot ODB Splits
    s.sum_odb_1_2_snapshot,
    s.sum_odb_3_8_snapshot,
    s.sum_odb_9_13_snapshot,
    
    -------------------- CUSTOMER LEVEL METRICS (Weekly Roll Rate) --------------------
    c.num_cust_1_2,
    c.num_cust_3_8,
    c.num_cust_9_13,
    c.num_cust_cured_1_2,
    c.num_cust_cured_3_8,
    c.num_cust_cured_9_13,
    c.num_cust_delinq_wed_1_2,
    c.num_cust_delinq_wed_3_8,
    c.num_cust_delinq_wed_9_13,

    -- Added: Roll/Improvement Customer Counts
    num_cust_roll_wed_1_2,
    num_cust_roll_wed_3_8,
    num_cust_roll_wed_9_13,
    num_cust_Improved_wed_3_8,
    num_cust_Improved_wed_9_13,
    
    -------------------- G.25: $ OUTSTANDING PRINCIPAL --------------------
    sum_os_1_2,
    sum_os_3_8,
    sum_os_9_13,
    sum_os_cured_1_2,
    sum_os_cured_3_8,
    sum_os_cured_9_13,
    sum_os_delinq_wed_1_2,
    sum_os_delinq_wed_3_8,
    sum_os_delinq_wed_9_13,

    -- Added: Roll/Improvement OS
    sum_os_roll_wed_1_2,
    sum_os_roll_wed_3_8,
    sum_os_roll_wed_9_13,
    sum_os_Improved_wed_3_8,
    sum_os_Improved_wed_9_13,

    sum_odb_1_2,
    sum_odb_3_8,
    sum_odb_9_13,
    sum_odb_cured_1_2,
    sum_odb_cured_3_8,
    sum_odb_cured_9_13,
    sum_odb_delinq_wed_1_2,
    sum_odb_delinq_wed_3_8,
    sum_odb_delinq_wed_9_13,

    -- Added: Roll/Improvement Overdue Balance
    sum_odb_roll_wed_1_2,
    sum_odb_roll_wed_3_8,
    sum_odb_roll_wed_9_13,
    sum_odb_Improved_wed_3_8,
    sum_odb_Improved_wed_9_13,
    
    -- % Overdue Balance
    c.pct_odb_1_2,
    c.pct_odb_3_8,
    c.pct_odb_9_13,
    
    -- Roll Rates (Count based)
    c.cure_rate_1_2_pct,
    c.cure_rate_3_8_pct,
    c.cure_rate_9_13_pct,
    c.stay_rate_1_2_pct,
    c.stay_rate_3_8_pct,
    c.stay_rate_9_13_pct,
    c.worsen_rate_1_2_pct,
    c.worsen_rate_3_8_pct,
    c.worsen_rate_9_13_pct,
    c.improve_rate_3_8_pct,
    c.improve_rate_9_13_pct,


    
    
    -- Roll Rates (OS based)
    c.cure_rate_1_2_pct_os,
    c.cure_rate_3_8_pct_os,
    c.cure_rate_9_13_pct_os,
    c.stay_rate_1_2_pct_os,
    c.stay_rate_3_8_pct_os,
    c.stay_rate_9_13_pct_os,
    c.worsen_rate_1_2_pct_os,
    c.worsen_rate_3_8_pct_os,
    c.worsen_rate_9_13_pct_os,
    c.improve_rate_3_8_pct_os,
    c.improve_rate_9_13_pct_os,
    
    -- Collections
    c.past_due_collected_1_2,
    c.past_due_collected_3_8,
    c.past_due_collected_9_13,
    c.total_collected_1_2,
    c.total_collected_3_8,
    c.total_collected_9_13,
    
    -- Collection Rates
    c.past_due_collection_rate_1_2_pct,
    c.past_due_collection_rate_3_8_pct,
    c.past_due_collection_rate_9_13_pct,
    c.total_collection_rate_1_2_pct,
    c.total_collection_rate_3_8_pct,
    c.total_collection_rate_9_13_pct,

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

FROM analytics.credit.km_snapshot_preco_metrics s
Inner JOIN analytics.credit.km_customer_roll_metrics_weekly c ON s.week_end_date = c.week_end_date
inner JOIN analytics.credit.km_collections_preco_settlements a2 on s.week_end_date = a2.week_end_date
ORDER BY week_end_date;

-- -- Select * from analytics.credit.km_collections_preco_final_weekly
-- order by week_end_date desc;

-------------------------------------------- Post Charge-Off Placements and Collections --------------------------------------------
-- Single table with all CTEs consolidated
-- Tracks placements, recoveries, and vendor attribution

--Select * from analytics.credit.mk_postco_collections_metrics
CREATE OR REPLACE TABLE analytics.credit.mk_postco_collections_metrics AS

WITH 
--------------------------------------------------------------------------------
-- Exclude cancelled loans
--------------------------------------------------------------------------------
first_table AS (
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

--------------------------------------------------------------------------------
-- Track ALL placement events (when accounts are placed with vendors)
--------------------------------------------------------------------------------
placements AS (
    SELECT 
        dacd.fbbid,
        dacd.edate AS transfer_date,
        DATE_TRUNC('WEEK', dacd.edate::DATE + 4)::DATE + 2 AS transfer_week_end,
        dacd.recovery_suggested_state,
        dacd.recovery_suggested_substate,
        dacd.outstanding_principal * COALESCE(fcu_p.fx_rate, 1.0) AS outstanding_principal,
        dacd.fees_due * COALESCE(fcu_p.fx_rate, 1.0) AS fees_due,
        dacd.discount_pending * COALESCE(fcu_p.fx_rate, 1.0) AS discount_pending,
        (dacd.outstanding_principal + dacd.fees_due - dacd.discount_pending) * COALESCE(fcu_p.fx_rate, 1.0) AS transfer_balance,
        CASE 
            WHEN dacd.recovery_suggested_substate IN ('3RD_P_SOLD') THEN 'SCJ'
            WHEN dacd.recovery_suggested_substate IN ('ASPIRE_LAW') THEN 'ASPIRE_LAW'
            WHEN dacd.recovery_suggested_substate IN ('BK_BL') THEN 'BK_BL'
            WHEN dacd.recovery_suggested_substate IN ('EVANS_MUL') THEN 'EVANS_MUL' 
            WHEN dacd.recovery_suggested_substate IN ('LP_HARVEST') THEN 'Harvest'
            WHEN dacd.recovery_suggested_substate IN ('LP_WELTMAN') THEN 'Weltman'
            WHEN dacd.recovery_suggested_substate IN ('MRS_PRIM', 'MRS_SEC') THEN 'MRS'
            WHEN dacd.recovery_suggested_substate IN ('PB_CAP_PR', 'PB_CAPITAL') THEN 'PB_Capital'
            WHEN dacd.recovery_suggested_substate IN ('SEQ_PRIM', 'SEQ_SEC') THEN 'SEQ'
            ELSE 'Other'
        END AS placed_vendor
    FROM bi.public.daily_approved_customers_data dacd
    LEFT JOIN INDUS.PUBLIC.FX_CUSTOMER_UNIFIED fcu_p
        ON fcu_p.FBBID = dacd.FBBID
        AND fcu_p.EXCHANGE_DATE = dacd.EDATE::DATE
    WHERE dacd.recovery_suggested_state = 'ELR' 
      AND dacd.recovery_suggested_substate NOT IN ('3RD_P_HOLD')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dacd.fbbid, dacd.recovery_suggested_substate ORDER BY dacd.edate ASC) = 1
),

-- Select * from bi.public.daily_approved_customers_data
-- where fbbid = 278809 and edate = current_date

--------------------------------------------------------------------------------
-- Weekly charge-off data from FMD
--------------------------------------------------------------------------------
co_weekly AS (
    SELECT 
        fmd.edate AS week_end_date,
        fmd.edate - 6 AS week_start_date,
        fmd.fbbid,
        fmd.loan_key,
        fmd.is_charged_off,
        fmd.charge_off_date,
        fmd.outstanding_principal_due * COALESCE(flu_co.loan_fx_rate, 1.0) AS os_91
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_co ON flu_co.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND DAYOFWEEK(fmd.edate) = 3
      AND fmd.edate >= '2020-12-01'
      AND fmd.is_charged_off = 1
),

--------------------------------------------------------------------------------
-- Aggregate to fbbid level per week
--------------------------------------------------------------------------------
co_fbbid_weekly AS (
    SELECT 
        week_end_date,
        week_start_date,
        fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(os_91) AS total_os_91,
        CASE 
            WHEN MIN(charge_off_date) BETWEEN DATEADD(YEAR, -5, week_end_date) AND week_end_date 
            THEN 1 ELSE 0 
        END AS is_within_last_5_yrs,
        CASE 
            WHEN MIN(charge_off_date) BETWEEN week_start_date AND week_end_date 
            THEN 1 ELSE 0 
        END AS is_new_co_this_week
    FROM co_weekly
    GROUP BY week_end_date, week_start_date, fbbid
)
--Select top 100* from co_fbbid_weekly;

--------------------------------------------------------------------------------
-- Placement status for each fbbid on each Wednesday
--------------------------------------------------------------------------------
,placement_weekly AS (
    SELECT 
        fbbid,
        edate AS week_end_date,
        CASE 
            WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
            WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_status,
        CASE 
            WHEN recovery_suggested_substate IN ('3RD_P_SOLD') THEN 'SCJ'
            WHEN recovery_suggested_substate IN ('ASPIRE_LAW') THEN 'ASPIRE_LAW'
            WHEN recovery_suggested_substate IN ('BK_BL') THEN 'BK_BL'
            WHEN recovery_suggested_substate IN ('EVANS_MUL') THEN 'EVANS_MUL' 
            WHEN recovery_suggested_substate IN ('LP_HARVEST') THEN 'Harvest'
            WHEN recovery_suggested_substate IN ('LP_WELTMAN') THEN 'Weltman'
            WHEN recovery_suggested_substate IN ('MRS_PRIM', 'MRS_SEC') THEN 'MRS'
            WHEN recovery_suggested_substate IN ('PB_CAP_PR', 'PB_CAPITAL') THEN 'PB_Capital'
            WHEN recovery_suggested_substate IN ('SEQ_PRIM', 'SEQ_SEC') THEN 'SEQ'
            ELSE NULL
        END AS current_vendor
    FROM bi.public.daily_approved_customers_data
    WHERE DAYOFWEEK(edate) = 3
      AND edate >= '2020-12-01'
),

--------------------------------------------------------------------------------
-- Weekly snapshot: charged-off accounts with placement status
--------------------------------------------------------------------------------
weekly_snapshot AS (
    SELECT 
        c.week_end_date,
        c.week_start_date,
        c.fbbid,
        c.charge_off_date,
        c.total_os_91,
        c.is_within_last_5_yrs,
        c.is_new_co_this_week,
        COALESCE(p.placement_status, 'Internal') AS placement_status,
        p.current_vendor
    FROM co_fbbid_weekly c
    LEFT JOIN placement_weekly p 
        ON c.fbbid = p.fbbid 
        AND c.week_end_date = p.week_end_date
)
-- Select placement_status,
-- current_vendor,
-- is_within_last_5_yrs,
-- sum(total_os_91) as os,
-- count(*) as ct
-- from weekly_snapshot where week_end_date = '2026-02-11'
-- group by all;


--------------------------------------------------------------------------------
-- Raw payments after charge-off
--------------------------------------------------------------------------------
,ranked_transfers AS (
Select l.*,
    lead(transfer_date,1,'2090-01-01') over(partition by fbbid order by transfer_date) as next_transfer_date,
    lead(recovery_suggested_substate,1,null) over(partition by fbbid order by transfer_date) as next_recovery_agency
from 
 (   SELECT 
            dacd_rt.fbbid,
            dacd_rt.edate AS transfer_date,
            date(dacd_rt.CHARGEOFF_TIME) as charge_off_date,
            dacd_rt.recovery_suggested_substate AS recovery_suggested_substate,
            dacd_rt.outstanding_principal * COALESCE(fcu_rt.fx_rate, 1.0) AS outstanding_principal,
            dacd_rt.fees_due * COALESCE(fcu_rt.fx_rate, 1.0) AS fees_due,
            dacd_rt.discount_pending * COALESCE(fcu_rt.fx_rate, 1.0) AS discount_pending,
            (dacd_rt.outstanding_principal + dacd_rt.fees_due - dacd_rt.discount_pending) * COALESCE(fcu_rt.fx_rate, 1.0) AS transfer_balance,
            ROW_NUMBER() OVER(PARTITION BY dacd_rt.fbbid,dacd_rt.recovery_suggested_substate ORDER BY dacd_rt.edate ASC) as assignment_rank
        FROM bi.public.daily_approved_customers_data dacd_rt
        LEFT JOIN INDUS.PUBLIC.FX_CUSTOMER_UNIFIED fcu_rt
            ON fcu_rt.FBBID = dacd_rt.FBBID
            AND fcu_rt.EXCHANGE_DATE = dacd_rt.EDATE::DATE
        WHERE dacd_rt.recovery_suggested_state = 'ELR' 
        AND dacd_rt.recovery_suggested_substate not in ('3RD_P_HOLD')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY dacd_rt.fbbid, dacd_rt.recovery_suggested_substate ORDER BY dacd_rt.edate ASC) = 1
    )l 
),
Base_data as
(
select *,
    case 
    when recovery_suggested_substate in ('3RD_P_SOLD') then 'SCJ'
    when recovery_suggested_substate in ('ASPIRE_LAW') then 'ASPIRE_LAW'
    when recovery_suggested_substate in ('BK_BL')      then 'BK_BL'
    when recovery_suggested_substate in ('EVANS_MUL') then 'EVANS_MUL' 
    when recovery_suggested_substate in ('LP_HARVEST') then 'Harvest'
    when recovery_suggested_substate in ('LP_WELTMAN') then 'Weltman'
    when recovery_suggested_substate in ('MRS_PRIM','MRS_SEC') then 'MRS'
    when recovery_suggested_substate in ('PB_CAP_PR','PB_CAPITAL') then 'PB_Capital'
    when recovery_suggested_substate in ('SEQ_PRIM','SEQ_SEC') then 'SEQ'
    else 'other'
    end as Vendor_name
from ranked_transfers
)
-- Select * from base_data 
-- where vendor_name = 'PB_Capital';

--Select top 1000* from base_data;
,
Payments as
(
Select 
pm.FBBID,
pm.originator,
date(pm.payment_event_time) as payment_event_time,
sum(TO_DOUBLE(pm.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) * COALESCE(flu_pay.loan_fx_rate, 1.0)) AS payment_amount
from bi.finance.payments_model pm
LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_pay ON flu_pay.LOAN_KEY = pm.LOAN_KEY
where pm.payment_status = 'FUND' 
and pm.payment_event_time >='2020-01-01' and pm.parent_payment_id is not null
group by pm.FBBID, pm.originator, date(pm.payment_event_time)
)
-- where originator ilike 'SEQ' and last_day(payment_event_time) = '2025-07-31';

,
with_payments as
(
    Select a.*,
    b.originator,
    b.payment_event_time,
    b.payment_amount
    from base_data a
    left join payments b
    on a.fbbid = b.fbbid 
    and (b.payment_event_time > a.transfer_date and b.payment_event_time<= a.next_transfer_date)
)
--select * from with_payments where fbbid = 529351;
,
final_data as
(
    select 
    fbbid,
    transfer_date,
    last_day(transfer_date) as transfer_month,
    charge_off_date,
    Outstanding_principal,fees_due,discount_pending,transfer_balance,
    vendor_name,
    originator,
    DATE_TRUNC('WEEK', payment_event_time::DATE + 4)::DATE + 2 AS week_end_date,
    case 
    when originator in ('B&L','BL') then 'BL'
    when originator ilike '%HARVEST%' then 'Harvest'
    when originator ilike '%MRS%' then 'MRS'
    when Originator ilike '%P&B%' then 'PB_Capital'
    when originator ilike '%SEQ%' then 'SEQ'
    when originator ilike '%WWR%' or originator ilike '%Weltman%' then 'Weltman'
    else 'other'
    end as payment_vendor,
    payment_event_time,
    last_day(payment_event_time) as payment_month,
    datediff('day',transfer_date,payment_event_time)as days_between_transfer_and_payment,
    floor(days_between_transfer_and_payment/30) mob,
    row_number() OVER (PARTITION BY fbbid,vendor_name ORDER BY payment_event_time) as row_num,
    Case 
    when vendor_name in ('BK_BL') and payment_vendor = 'BL' then payment_amount
    when vendor_name in ('Harvest') and payment_vendor in ('Harvest') then payment_amount
    when vendor_name in ('MRS') and payment_vendor in ('MRS') then payment_amount
    when vendor_name in ('PB_Capital') and payment_vendor in ('PB_Capital') then payment_amount
    when vendor_name in ('SEQ') and payment_vendor in ('SEQ') then payment_amount
    when vendor_name in ('Weltman') and payment_vendor in ('Weltman') then payment_amount
    when vendor_name in ('ASPIRE_LAW') then payment_amount
    when vendor_name in ('EVANS_MUL') then payment_amount
    when vendor_name in ('SCJ') and transfer_date<='2026-01-31' then round((transfer_balance * 0.06), 2) 
    when vendor_name in ('SCJ') and transfer_date>='2026-02-01' then round((transfer_balance * 0.07), 2) 
    else 0 end as payment_amount
    from with_payments
)
-- select * from final_data where week_end_date = '2026-01-28' 
,
weekly_vendor_payments 
as
(select 
week_end_date,
vendor_name,
payment_vendor,
sum(payment_amount) as Payment_amount
from final_data
group by all
)

--------------------------------------------------------------------------------
-- Internal Payments 
--------------------------------------------------------------------------------
, ranked_transfers_internal AS (
Select l.*,
    lead(transfer_date,1,'2090-01-01') over(partition by fbbid order by transfer_date) as next_transfer_date,
    lead(placement_status,1,null) over(partition by fbbid order by transfer_date) as next_transfer
from 
 (   SELECT 
            dacd_ri.fbbid,
            dacd_ri.edate AS transfer_date,
            date(dacd_ri.CHARGEOFF_TIME) as charge_off_date,
            dacd_ri.recovery_suggested_substate AS recovery_suggested_substate,
            dacd_ri.outstanding_principal * COALESCE(fcu_ri.fx_rate, 1.0) AS outstanding_principal,
            dacd_ri.fees_due * COALESCE(fcu_ri.fx_rate, 1.0) AS fees_due,
            dacd_ri.discount_pending * COALESCE(fcu_ri.fx_rate, 1.0) AS discount_pending,
            (dacd_ri.outstanding_principal + dacd_ri.fees_due - dacd_ri.discount_pending) * COALESCE(fcu_ri.fx_rate, 1.0) AS transfer_balance,
            CASE 
            WHEN dacd_ri.RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR dacd_ri.RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
            WHEN dacd_ri.RECOVERY_SUGGESTED_STATE IN ('ELR') THEN 'External'
            when dacd_ri.recovery_suggested_state in ('PROLIT', 'TR_LR') then 'External_pro_tr'
            ELSE 'Unknown'
        END AS placement_status,
            LAG(placement_status) OVER (PARTITION BY dacd_ri.fbbid ORDER BY dacd_ri.edate ASC) AS prev_status
        FROM bi.public.daily_approved_customers_data dacd_ri
        LEFT JOIN INDUS.PUBLIC.FX_CUSTOMER_UNIFIED fcu_ri
            ON fcu_ri.FBBID = dacd_ri.FBBID
            AND fcu_ri.EXCHANGE_DATE = dacd_ri.EDATE::DATE
        where dacd_ri.CHARGEOFF_TIME is not null 
    )l 
    WHERE (l.prev_status IS NULL OR l.placement_status != l.prev_status)
)


,
Payments_internal as
(
Select 
pm_i.FBBID,
pm_i.originator,
date(pm_i.payment_event_time) as payment_event_time,
sum(TO_DOUBLE(pm_i.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) * COALESCE(flu_pi.loan_fx_rate, 1.0)) AS payment_amount
from bi.finance.payments_model pm_i
LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_pi ON flu_pi.LOAN_KEY = pm_i.LOAN_KEY
where pm_i.payment_status = 'FUND' 
and pm_i.payment_event_time >='2020-01-01' and pm_i.parent_payment_id is not null
group by pm_i.FBBID, pm_i.originator, date(pm_i.payment_event_time)
)
,
with_payments_internal as
(
    Select a.*,
    b.payment_event_time,
    b.payment_amount
    from ranked_transfers_internal a
    left join Payments_internal b
    on a.fbbid = b.fbbid 
    and (b.payment_event_time > a.transfer_date and b.payment_event_time<= a.next_transfer_date)
)
,
final_data_internal as
(
    select 
    fbbid,
    transfer_date,
    DATE_TRUNC('WEEK', payment_event_time::DATE + 4)::DATE + 2 AS week_end_date,
    Outstanding_principal,fees_due,discount_pending,transfer_balance,
    payment_event_time,
    placement_status,
    datediff('day',transfer_date,payment_event_time)as days_between_transfer_and_payment,
    Case 
    when placement_status = 'Internal' then Payment_amount 
    when placement_status = 'External' then payment_amount
    when placement_status = 'External_pro_tr' then Payment_amount
    else 0 end as payment_amount
    from with_payments_internal
)
,
weekly_vendor_payments_internal 
as
(
select 
week_end_date,
placement_status,
sum(payment_amount) as Payment_amount
from final_data_internal
group by week_end_date,placement_status
)

--------------------------------------------------------------------------------
-- New placements this week (from actual transfer events)
--------------------------------------------------------------------------------
,placement_events_agg AS (
    SELECT 
        transfer_week_end AS week_end_date,
        
        COUNT(DISTINCT CASE WHEN placed_vendor = 'SCJ' THEN fbbid END) AS new_scj_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'ASPIRE_LAW' THEN fbbid END) AS new_aspire_law_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'BK_BL' THEN fbbid END) AS new_bk_bl_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'EVANS_MUL' THEN fbbid END) AS new_evans_mul_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'Harvest' THEN fbbid END) AS new_harvest_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'Weltman' THEN fbbid END) AS new_weltman_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'MRS' THEN fbbid END) AS new_mrs_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'PB_Capital' THEN fbbid END) AS new_pb_capital_placements,
        COUNT(DISTINCT CASE WHEN placed_vendor = 'SEQ' THEN fbbid END) AS new_seq_placements,
        
        SUM(CASE WHEN placed_vendor = 'SCJ' THEN transfer_balance END) AS new_scj_placed_os,
        SUM(CASE WHEN placed_vendor = 'ASPIRE_LAW' THEN transfer_balance END) AS new_aspire_law_placed_os,
        SUM(CASE WHEN placed_vendor = 'BK_BL' THEN transfer_balance END) AS new_bk_bl_placed_os,
        SUM(CASE WHEN placed_vendor = 'EVANS_MUL' THEN transfer_balance END) AS new_evans_mul_placed_os,
        SUM(CASE WHEN placed_vendor = 'Harvest' THEN transfer_balance END) AS new_harvest_placed_os,
        SUM(CASE WHEN placed_vendor = 'Weltman' THEN transfer_balance END) AS new_weltman_placed_os,
        SUM(CASE WHEN placed_vendor = 'MRS' THEN transfer_balance END) AS new_mrs_placed_os,
        SUM(CASE WHEN placed_vendor = 'PB_Capital' THEN transfer_balance END) AS new_pb_capital_placed_os,
        SUM(CASE WHEN placed_vendor = 'SEQ' THEN transfer_balance END) AS new_seq_placed_os,
        
        COUNT(DISTINCT fbbid) AS new_external_placements,
        SUM(transfer_balance) AS new_external_placed_os
        
    FROM placements
    GROUP BY transfer_week_end
),

--------------------------------------------------------------------------------
-- Option A: Cumulative placements track ALL placement events
-- When account moves from Vendor A to B, counts in BOTH vendors' cumulative totals
-- Uses running sums of new placements week over week
--------------------------------------------------------------------------------
cumulative_placements AS (
    SELECT 
        week_end_date,
        
        -- Running totals of placement counts (all-time placements to each vendor)
        SUM(COALESCE(new_scj_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_scj_placements,
        SUM(COALESCE(new_aspire_law_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_aspire_law_placements,
        SUM(COALESCE(new_bk_bl_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_bk_bl_placements,
        SUM(COALESCE(new_evans_mul_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_evans_mul_placements,
        SUM(COALESCE(new_harvest_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_harvest_placements,
        SUM(COALESCE(new_weltman_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weltman_placements,
        SUM(COALESCE(new_mrs_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_mrs_placements,
        SUM(COALESCE(new_pb_capital_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pb_capital_placements,
        SUM(COALESCE(new_seq_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_seq_placements,
        
        -- Running totals of placed OS (transfer balance at time of placement)
        SUM(COALESCE(new_scj_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_scj_placed_os,
        SUM(COALESCE(new_aspire_law_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_aspire_law_placed_os,
        SUM(COALESCE(new_bk_bl_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_bk_bl_placed_os,
        SUM(COALESCE(new_evans_mul_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_evans_mul_placed_os,
        SUM(COALESCE(new_harvest_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_harvest_placed_os,
        SUM(COALESCE(new_weltman_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weltman_placed_os,
        SUM(COALESCE(new_mrs_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_mrs_placed_os,
        SUM(COALESCE(new_pb_capital_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pb_capital_placed_os,
        SUM(COALESCE(new_seq_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_seq_placed_os,
        
        -- Cumulative Transfer Balance (tb) at time of placement - same as placed_os for Option A
        SUM(COALESCE(new_scj_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_scj_placed_tb,
        SUM(COALESCE(new_aspire_law_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_aspire_law_placed_tb,
        SUM(COALESCE(new_bk_bl_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_bk_bl_placed_tb,
        SUM(COALESCE(new_evans_mul_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_evans_mul_placed_tb,
        SUM(COALESCE(new_harvest_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_harvest_placed_tb,
        SUM(COALESCE(new_weltman_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weltman_placed_tb,
        SUM(COALESCE(new_mrs_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_mrs_placed_tb,
        SUM(COALESCE(new_pb_capital_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pb_capital_placed_tb,
        SUM(COALESCE(new_seq_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_seq_placed_tb,
        
        -- Total external cumulative (all vendors combined)
        SUM(COALESCE(new_external_placements, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_external_placements,
        SUM(COALESCE(new_external_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_external_placed_os,
        SUM(COALESCE(new_external_placed_os, 0)) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_external_placed_tb
        
    FROM placement_events_agg
),

--------------------------------------------------------------------------------
-- Snapshot aggregation (total CO accounts)
--------------------------------------------------------------------------------
snapshot_agg AS (
    SELECT 
        week_end_date,
        week_start_date,
        
        COUNT(DISTINCT CASE WHEN is_within_last_5_yrs = 1 THEN fbbid END) AS total_num_cust_co,
        COUNT(DISTINCT CASE WHEN is_within_last_5_yrs = 1 AND placement_status = 'Internal' THEN fbbid END) AS internal_num_cust_co,
        COUNT(DISTINCT CASE WHEN is_within_last_5_yrs = 1 AND placement_status = 'External' THEN fbbid END) AS external_num_cust_co,
        COUNT(DISTINCT CASE WHEN is_new_co_this_week = 1 THEN fbbid END) AS new_num_cust_co,
        
        SUM(CASE WHEN is_within_last_5_yrs = 1 THEN total_os_91 END) AS total_sum_os_co,
        SUM(CASE WHEN is_within_last_5_yrs = 1 AND placement_status = 'Internal' THEN total_os_91 END) AS internal_sum_os_co,
        SUM(CASE WHEN is_within_last_5_yrs = 1 AND placement_status = 'External' THEN total_os_91 END) AS external_sum_os_co,
        SUM(CASE WHEN is_new_co_this_week = 1 THEN total_os_91 END) AS new_sum_os_co
        
    FROM weekly_snapshot
    GROUP BY week_end_date, week_start_date
),

--------------------------------------------------------------------------------
-- Current Vendor Snapshot: As of this week, which accounts are with which vendor
-- This is a point-in-time view (vertical metrics)
--------------------------------------------------------------------------------
current_vendor_snapshot AS (
    SELECT 
        week_end_date,
        
        -- Current # of accounts with each vendor as of this week
        COUNT(DISTINCT CASE WHEN current_vendor = 'SCJ' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_scj_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'ASPIRE_LAW' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_aspire_law_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'BK_BL' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_bk_bl_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'EVANS_MUL' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_evans_mul_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'Harvest' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_harvest_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'Weltman' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_weltman_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'MRS' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_mrs_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'PB_Capital' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_pb_capital_num,
        COUNT(DISTINCT CASE WHEN current_vendor = 'SEQ' AND is_within_last_5_yrs = 1 THEN fbbid END) AS curr_seq_num,
        
        -- Current OS with each vendor as of this week
        SUM(CASE WHEN current_vendor = 'SCJ' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_scj_os,
        SUM(CASE WHEN current_vendor = 'ASPIRE_LAW' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_aspire_law_os,
        SUM(CASE WHEN current_vendor = 'BK_BL' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_bk_bl_os,
        SUM(CASE WHEN current_vendor = 'EVANS_MUL' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_evans_mul_os,
        SUM(CASE WHEN current_vendor = 'Harvest' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_harvest_os,
        SUM(CASE WHEN current_vendor = 'Weltman' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_weltman_os,
        SUM(CASE WHEN current_vendor = 'MRS' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_mrs_os,
        SUM(CASE WHEN current_vendor = 'PB_Capital' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_pb_capital_os,
        SUM(CASE WHEN current_vendor = 'SEQ' AND is_within_last_5_yrs = 1 THEN total_os_91 END) AS curr_seq_os
        
    FROM weekly_snapshot
    GROUP BY week_end_date
),

--------------------------------------------------------------------------------
-- Recovery aggregation
--------------------------------------------------------------------------------
recovery_agg AS (
    SELECT 
        week_end_date, 
        SUM(CASE WHEN vendor_name = 'ASPIRE_LAW' THEN payment_amount END) AS ext_aspire_law_recoveries,
        SUM(CASE WHEN vendor_name = 'BK_BL' THEN payment_amount END) AS ext_bk_bl_recoveries,
        SUM(CASE WHEN vendor_name = 'EVANS_MUL' THEN payment_amount END) AS ext_evans_mul_recoveries,
        SUM(CASE WHEN vendor_name = 'Harvest' THEN payment_amount END) AS ext_harvest_recoveries,
        SUM(CASE WHEN vendor_name = 'Weltman' THEN payment_amount END) AS ext_weltman_recoveries,
        SUM(CASE WHEN vendor_name = 'MRS' THEN payment_amount END) AS ext_mrs_recoveries,
        SUM(CASE WHEN vendor_name = 'PB_Capital' THEN payment_amount END) AS ext_pb_capital_recoveries,
        SUM(CASE WHEN vendor_name = 'SEQ' THEN payment_amount END) AS ext_seq_recoveries,
        SUM(CASE WHEN vendor_name = 'SCJ' THEN payment_amount END) AS ext_scj_recoveries
        
    FROM weekly_vendor_payments
    WHERE payment_amount IS NOT NULL
    GROUP BY week_end_date
),

recovery_agg_internal as
(
select 
week_end_date,
sum(Payment_amount) as total_recoveries,
sum(case when placement_status = 'Internal'then payment_amount end) as internal_recoveries,
sum(case when placement_status = 'External'then payment_amount end) as external_recoveries
from weekly_vendor_payments_internal
where payment_amount is not null
group by all
)
--------------------------------------------------------------------------------
-- Combine all aggregations
--------------------------------------------------------------------------------
,combined AS (
    SELECT 
        s.*,
        
        COALESCE(pe.new_external_placements, 0) AS new_external_placements,
        COALESCE(pe.new_external_placed_os, 0) AS new_external_placed_os,
        COALESCE(pe.new_scj_placements, 0) AS new_scj_placements,
        COALESCE(pe.new_scj_placed_os, 0) AS new_scj_placed_os,
        COALESCE(pe.new_aspire_law_placements, 0) AS new_aspire_law_placements,
        COALESCE(pe.new_aspire_law_placed_os, 0) AS new_aspire_law_placed_os,
        COALESCE(pe.new_bk_bl_placements, 0) AS new_bk_bl_placements,
        COALESCE(pe.new_bk_bl_placed_os, 0) AS new_bk_bl_placed_os,
        COALESCE(pe.new_evans_mul_placements, 0) AS new_evans_mul_placements,
        COALESCE(pe.new_evans_mul_placed_os, 0) AS new_evans_mul_placed_os,
        COALESCE(pe.new_harvest_placements, 0) AS new_harvest_placements,
        COALESCE(pe.new_harvest_placed_os, 0) AS new_harvest_placed_os,
        COALESCE(pe.new_weltman_placements, 0) AS new_weltman_placements,
        COALESCE(pe.new_weltman_placed_os, 0) AS new_weltman_placed_os,
        COALESCE(pe.new_mrs_placements, 0) AS new_mrs_placements,
        COALESCE(pe.new_mrs_placed_os, 0) AS new_mrs_placed_os,
        COALESCE(pe.new_pb_capital_placements, 0) AS new_pb_capital_placements,
        COALESCE(pe.new_pb_capital_placed_os, 0) AS new_pb_capital_placed_os,
        COALESCE(pe.new_seq_placements, 0) AS new_seq_placements,
        COALESCE(pe.new_seq_placed_os, 0) AS new_seq_placed_os,
        
        COALESCE(cp.cum_external_placements, 0) AS cum_external_placements,
        COALESCE(cp.cum_external_placed_os, 0) AS cum_external_placed_os,
        COALESCE(cp.cum_scj_placements, 0) AS cum_scj_placements,
        COALESCE(cp.cum_scj_placed_os, 0) AS cum_scj_placed_os,
        COALESCE(cp.cum_aspire_law_placements, 0) AS cum_aspire_law_placements,
        COALESCE(cp.cum_aspire_law_placed_os, 0) AS cum_aspire_law_placed_os,
        COALESCE(cp.cum_bk_bl_placements, 0) AS cum_bk_bl_placements,
        COALESCE(cp.cum_bk_bl_placed_os, 0) AS cum_bk_bl_placed_os,
        COALESCE(cp.cum_evans_mul_placements, 0) AS cum_evans_mul_placements,
        COALESCE(cp.cum_evans_mul_placed_os, 0) AS cum_evans_mul_placed_os,
        COALESCE(cp.cum_harvest_placements, 0) AS cum_harvest_placements,
        COALESCE(cp.cum_harvest_placed_os, 0) AS cum_harvest_placed_os,
        COALESCE(cp.cum_weltman_placements, 0) AS cum_weltman_placements,
        COALESCE(cp.cum_weltman_placed_os, 0) AS cum_weltman_placed_os,
        COALESCE(cp.cum_mrs_placements, 0) AS cum_mrs_placements,
        COALESCE(cp.cum_mrs_placed_os, 0) AS cum_mrs_placed_os,
        COALESCE(cp.cum_pb_capital_placements, 0) AS cum_pb_capital_placements,
        COALESCE(cp.cum_pb_capital_placed_os, 0) AS cum_pb_capital_placed_os,
        COALESCE(cp.cum_seq_placements, 0) AS cum_seq_placements,
        COALESCE(cp.cum_seq_placed_os, 0) AS cum_seq_placed_os,
        
        -- Cumulative Transfer Balance (tb)
        COALESCE(cp.cum_scj_placed_tb, 0) AS cum_scj_placed_tb,
        COALESCE(cp.cum_aspire_law_placed_tb, 0) AS cum_aspire_law_placed_tb,
        COALESCE(cp.cum_bk_bl_placed_tb, 0) AS cum_bk_bl_placed_tb,
        COALESCE(cp.cum_evans_mul_placed_tb, 0) AS cum_evans_mul_placed_tb,
        COALESCE(cp.cum_harvest_placed_tb, 0) AS cum_harvest_placed_tb,
        COALESCE(cp.cum_weltman_placed_tb, 0) AS cum_weltman_placed_tb,
        COALESCE(cp.cum_mrs_placed_tb, 0) AS cum_mrs_placed_tb,
        COALESCE(cp.cum_pb_capital_placed_tb, 0) AS cum_pb_capital_placed_tb,
        COALESCE(cp.cum_seq_placed_tb, 0) AS cum_seq_placed_tb,
        COALESCE(cp.cum_external_placed_tb, 0) AS cum_external_placed_tb,
        
        -- Current Vendor Snapshot (as of this week)
        COALESCE(cvs.curr_scj_num, 0) AS curr_scj_num,
        COALESCE(cvs.curr_aspire_law_num, 0) AS curr_aspire_law_num,
        COALESCE(cvs.curr_bk_bl_num, 0) AS curr_bk_bl_num,
        COALESCE(cvs.curr_evans_mul_num, 0) AS curr_evans_mul_num,
        COALESCE(cvs.curr_harvest_num, 0) AS curr_harvest_num,
        COALESCE(cvs.curr_weltman_num, 0) AS curr_weltman_num,
        COALESCE(cvs.curr_mrs_num, 0) AS curr_mrs_num,
        COALESCE(cvs.curr_pb_capital_num, 0) AS curr_pb_capital_num,
        COALESCE(cvs.curr_seq_num, 0) AS curr_seq_num,
        COALESCE(cvs.curr_scj_os, 0) AS curr_scj_os,
        COALESCE(cvs.curr_aspire_law_os, 0) AS curr_aspire_law_os,
        COALESCE(cvs.curr_bk_bl_os, 0) AS curr_bk_bl_os,
        COALESCE(cvs.curr_evans_mul_os, 0) AS curr_evans_mul_os,
        COALESCE(cvs.curr_harvest_os, 0) AS curr_harvest_os,
        COALESCE(cvs.curr_weltman_os, 0) AS curr_weltman_os,
        COALESCE(cvs.curr_mrs_os, 0) AS curr_mrs_os,
        COALESCE(cvs.curr_pb_capital_os, 0) AS curr_pb_capital_os,
        COALESCE(cvs.curr_seq_os, 0) AS curr_seq_os,
        
        COALESCE(ri.total_recoveries, 0) AS post_co_recoveries,
        COALESCE(ri.internal_recoveries, 0) AS internal_post_co_recoveries,
        COALESCE(ri.external_recoveries, 0) AS external_post_co_recoveries,

        COALESCE(r.ext_aspire_law_recoveries, 0) AS ext_aspire_law_recoveries,
        COALESCE(r.ext_bk_bl_recoveries, 0) AS ext_bk_bl_recoveries,
        COALESCE(r.ext_evans_mul_recoveries, 0) AS ext_evans_mul_recoveries,
        COALESCE(r.ext_harvest_recoveries, 0) AS ext_harvest_recoveries,
        COALESCE(r.ext_weltman_recoveries, 0) AS ext_weltman_recoveries,
        COALESCE(r.ext_mrs_recoveries, 0) AS ext_mrs_recoveries,
        COALESCE(r.ext_pb_capital_recoveries, 0) AS ext_pb_capital_recoveries,
        COALESCE(r.ext_seq_recoveries, 0) AS ext_seq_recoveries,
        COALESCE(r.ext_scj_recoveries, 0) AS ext_scj_recoveries
        
    FROM snapshot_agg s
    LEFT JOIN placement_events_agg pe ON s.week_end_date = pe.week_end_date
    LEFT JOIN cumulative_placements cp ON s.week_end_date = cp.week_end_date
    LEFT JOIN current_vendor_snapshot cvs ON s.week_end_date = cvs.week_end_date
    LEFT JOIN recovery_agg r ON s.week_end_date = r.week_end_date
    left join recovery_agg_internal ri on s.week_end_date = ri.week_end_date
),

--------------------------------------------------------------------------------
-- Add running totals for recoveries
--------------------------------------------------------------------------------
with_running AS (
    SELECT 
        *,
        SUM(post_co_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_recoveries,
        SUM(internal_post_co_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_internal_recoveries,
        SUM(external_post_co_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_external_recoveries,
        SUM(ext_aspire_law_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_aspire_law,
        SUM(ext_bk_bl_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_bk_bl,
        SUM(ext_evans_mul_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_evans_mul,
        SUM(ext_harvest_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_harvest,
        SUM(ext_weltman_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_weltman,
        SUM(ext_mrs_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_mrs,
        SUM(ext_pb_capital_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_pb_capital,
        SUM(ext_seq_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_seq,
        SUM(ext_scj_recoveries) OVER (ORDER BY week_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ext_scj
    FROM combined
)

--------------------------------------------------------------------------------
-- Final output
--------------------------------------------------------------------------------
SELECT 
    week_start_date,
    week_end_date,
    
    -- Total CO Metrics
    total_num_cust_co,
    internal_num_cust_co,
    external_num_cust_co,
    new_num_cust_co,
    total_sum_os_co,
    internal_sum_os_co,
    external_sum_os_co,
    new_sum_os_co,
    
    -- New Placements This Week
    new_external_placements,
    new_external_placed_os,
    new_scj_placements,
    new_scj_placed_os,
    new_aspire_law_placements,
    new_aspire_law_placed_os,
    new_bk_bl_placements,
    new_bk_bl_placed_os,
    new_evans_mul_placements,
    new_evans_mul_placed_os,
    new_harvest_placements,
    new_harvest_placed_os,
    new_weltman_placements,
    new_weltman_placed_os,
    new_mrs_placements,
    new_mrs_placed_os,
    new_pb_capital_placements,
    new_pb_capital_placed_os,
    new_seq_placements,
    new_seq_placed_os,
    
    -- Cumulative Placements (currently with vendor)
    cum_external_placements,
    cum_external_placed_os,
    cum_scj_placements,
    cum_scj_placed_os,
    cum_aspire_law_placements,
    cum_aspire_law_placed_os,
    cum_bk_bl_placements,
    cum_bk_bl_placed_os,
    cum_evans_mul_placements,
    cum_evans_mul_placed_os,
    cum_harvest_placements,
    cum_harvest_placed_os,
    cum_weltman_placements,
    cum_weltman_placed_os,
    cum_mrs_placements,
    cum_mrs_placed_os,
    cum_pb_capital_placements,
    cum_pb_capital_placed_os,
    cum_seq_placements,
    cum_seq_placed_os,
    
    -- Cumulative Transfer Balance (tb) at time of placement
    cum_scj_placed_tb,
    cum_aspire_law_placed_tb,
    cum_bk_bl_placed_tb,
    cum_evans_mul_placed_tb,
    cum_harvest_placed_tb,
    cum_weltman_placed_tb,
    cum_mrs_placed_tb,
    cum_pb_capital_placed_tb,
    cum_seq_placed_tb,
    cum_external_placed_tb,
    
    -- Current Vendor Snapshot: As of this week (vertical view)
    curr_scj_num,
    curr_aspire_law_num,
    curr_bk_bl_num,
    curr_evans_mul_num,
    curr_harvest_num,
    curr_weltman_num,
    curr_mrs_num,
    curr_pb_capital_num,
    curr_seq_num,
    curr_scj_os,
    curr_aspire_law_os,
    curr_bk_bl_os,
    curr_evans_mul_os,
    curr_harvest_os,
    curr_weltman_os,
    curr_mrs_os,
    curr_pb_capital_os,
    curr_seq_os,
    
    -- Weekly Recoveries
    post_co_recoveries,
    internal_post_co_recoveries,
    external_post_co_recoveries,
    ext_aspire_law_recoveries,
    ext_bk_bl_recoveries,
    ext_evans_mul_recoveries,
    ext_harvest_recoveries,
    ext_weltman_recoveries,
    ext_mrs_recoveries,
    ext_pb_capital_recoveries,
    ext_seq_recoveries,
    ext_scj_recoveries,

    -- Running Recoveries
    running_total_recoveries,
    running_internal_recoveries,
    running_external_recoveries,
    running_ext_aspire_law,
    running_ext_bk_bl,
    running_ext_evans_mul,
    running_ext_harvest,
    running_ext_weltman,
    running_ext_mrs,
    running_ext_pb_capital,
    running_ext_seq,
    running_ext_scj,

    -- Recovery Rates
    ROUND(running_total_recoveries / NULLIF(total_sum_os_co, 0), 4) AS cumulative_recovery_rate,
    ROUND(running_internal_recoveries / NULLIF(internal_sum_os_co, 0), 4) AS internal_cumulative_rate,
    ROUND(running_external_recoveries / NULLIF(cum_external_placed_os, 0), 4) AS external_cumulative_rate,

    -- Vendor Recovery Rates
    ROUND(running_ext_aspire_law / NULLIF(cum_aspire_law_placed_os, 0), 4) AS aspire_law_recovery_rate,
    ROUND(running_ext_bk_bl / NULLIF(cum_bk_bl_placed_os, 0), 4) AS bk_bl_recovery_rate,
    ROUND(running_ext_evans_mul / NULLIF(cum_evans_mul_placed_os, 0), 4) AS evans_mul_recovery_rate,
    ROUND(running_ext_harvest / NULLIF(cum_harvest_placed_os, 0), 4) AS harvest_recovery_rate,
    ROUND(running_ext_weltman / NULLIF(cum_weltman_placed_os, 0), 4) AS weltman_recovery_rate,
    ROUND(running_ext_mrs / NULLIF(cum_mrs_placed_os, 0), 4) AS mrs_recovery_rate,
    ROUND(running_ext_pb_capital / NULLIF(cum_pb_capital_placed_os, 0), 4) AS pb_capital_recovery_rate,
    ROUND(running_ext_seq / NULLIF(cum_seq_placed_os, 0), 4) AS seq_recovery_rate,
    ROUND(running_ext_scj / NULLIF(cum_scj_placed_os, 0), 4) AS scj_recovery_rate

FROM with_running
ORDER BY week_end_date;




----------------------------------------------- Post-CO Settlements ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.mk_postco_settlements_weekly AS
WITH all_offers AS (
    SELECT 
        cfs2.fbbid,
        DATE_TRUNC('WEEK', cfs2.edate::DATE + 4)::DATE + 2 AS week_end_date,
        DATEADD(DAY, -6, DATE_TRUNC('WEEK', cfs2.edate::DATE + 4)::DATE + 2) AS week_start_date,
        cfs2.edate,
        cfs2.status_name,
        CASE WHEN cfs2.status_name = 'SETTLEMENT_STATUS' 
                  AND cfs2.status_value = 'FUNDED' THEN cfs2.status_value END AS settlement_status_funded,
        CASE WHEN cfs2.status_name = 'FINAL_SETTLEMENT_AMOUNT' 
                  THEN cfs2.status_value::NUMERIC * COALESCE(fcu_pco.fx_rate, 1.0) END AS settlement_amount
    FROM bi.finance.customer_finance_statuses cfs2
    LEFT JOIN INDUS.PUBLIC.FX_CUSTOMER_UNIFIED fcu_pco
        ON fcu_pco.FBBID = cfs2.FBBID
        AND fcu_pco.EXCHANGE_DATE = cfs2.EDATE::DATE
    WHERE cfs2.status_group = 'DISCOUNTED_SETTLEMENT'
      AND (cfs2.status_name = 'FINAL_SETTLEMENT_AMOUNT' OR cfs2.status_name = 'SETTLEMENT_STATUS')
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fa.fbbid, fu.edate ORDER BY fa.edate DESC) = 1
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
            WHEN fs.fbbid IS NOT NULL AND a1.edate = fs.final_amount_edate THEN 1
            ELSE 0
        END AS is_completed
    FROM all_offers a1
    LEFT JOIN bi.public.daily_approved_customers_data a2 
           ON a1.fbbid = a2.fbbid AND a1.edate = a2.edate - 1
    LEFT JOIN funded_settlements fs 
           ON a1.fbbid = fs.fbbid AND a1.edate = fs.final_amount_edate
    WHERE a1.settlement_amount IS NOT NULL
),

placement_status AS (
    SELECT 
        fbbid,
        edate,
        CASE 
            WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
            WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS postco_placement
    FROM bi.public.daily_approved_customers_data
)

SELECT 
    week_start_date,
    week_end_date,

    -- Offered settlements
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' THEN a1.fbbid END) AS postco_num_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' THEN settlement_amount END), 0) AS postco_amt_settlements,
    
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'Internal' THEN a1.fbbid END) AS internal_postco_num_settlements,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'External' THEN a1.fbbid END) AS external_postco_num_settlements,
    
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'Internal' THEN settlement_amount END), 0) AS internal_postco_amt_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND postco_placement = 'External' THEN settlement_amount END), 0) AS external_postco_amt_settlements,

    -- Completed settlements
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 THEN a1.fbbid END) AS postco_num_completed_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 THEN settlement_amount END), 0) AS postco_amt_completed_settlements,

    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'Internal' THEN a1.fbbid END) AS internal_postco_num_completed_settlements,
    COUNT(DISTINCT CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'External' THEN a1.fbbid END) AS external_postco_num_completed_settlements,

    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'Internal' THEN settlement_amount END), 0) AS internal_postco_amt_completed_settlements,
    COALESCE(SUM(CASE WHEN dpd_bucket_group = '04. CHOF' AND is_completed = 1 AND postco_placement = 'External' THEN settlement_amount END), 0) AS external_postco_amt_completed_settlements

FROM offers_agg a1
LEFT JOIN placement_status a2 ON a1.fbbid = a2.fbbid AND a1.edate = a2.edate 
GROUP BY 1, 2
ORDER BY 1, 2;


----------------------------------------------- Final Post-Chargeoff Combined Table ----------------------------------------------------

CREATE OR REPLACE TABLE analytics.credit.mk_postco_final_weekly AS
WITH base AS (
    SELECT 
        a1.week_start_date,
        a1.week_end_date,
        
        -- Total CO Metrics
        a1.total_num_cust_co,
        a1.internal_num_cust_co,
        a1.external_num_cust_co,
        a1.new_num_cust_co,
        a1.total_sum_os_co,
        a1.internal_sum_os_co,
        a1.external_sum_os_co,
        a1.new_sum_os_co,
        
        -- New Placements This Week
        a1.new_external_placements,
        a1.new_external_placed_os,
        a1.new_scj_placements,
        a1.new_scj_placed_os,
        a1.new_aspire_law_placements,
        a1.new_aspire_law_placed_os,
        a1.new_bk_bl_placements,
        a1.new_bk_bl_placed_os,
        a1.new_evans_mul_placements,
        a1.new_evans_mul_placed_os,
        a1.new_harvest_placements,
        a1.new_harvest_placed_os,
        a1.new_weltman_placements,
        a1.new_weltman_placed_os,
        a1.new_mrs_placements,
        a1.new_mrs_placed_os,
        a1.new_pb_capital_placements,
        a1.new_pb_capital_placed_os,
        a1.new_seq_placements,
        a1.new_seq_placed_os,
        
        -- Cumulative Placements (currently with vendor)
        a1.cum_external_placements,
        a1.cum_external_placed_os,
        a1.cum_scj_placements,
        a1.cum_scj_placed_os,
        a1.cum_aspire_law_placements,
        a1.cum_aspire_law_placed_os,
        a1.cum_bk_bl_placements,
        a1.cum_bk_bl_placed_os,
        a1.cum_evans_mul_placements,
        a1.cum_evans_mul_placed_os,
        a1.cum_harvest_placements,
        a1.cum_harvest_placed_os,
        a1.cum_weltman_placements,
        a1.cum_weltman_placed_os,
        a1.cum_mrs_placements,
        a1.cum_mrs_placed_os,
        a1.cum_pb_capital_placements,
        a1.cum_pb_capital_placed_os,
        a1.cum_seq_placements,
        a1.cum_seq_placed_os,
        
        a1.cum_scj_placed_Tb,
        a1.cum_aspire_law_placed_tb,
        a1.cum_bk_bl_placed_tb,
        a1.cum_evans_mul_placed_tb,
        a1.cum_harvest_placed_tb,
        a1.cum_weltman_placed_tb,
        a1.cum_mrs_placed_tb, 
        a1.cum_pb_capital_placed_tb,
        a1.cum_seq_placed_tb,
        a1.cum_external_placed_tb,
        
        -- Current Vendor Snapshot (as of this week)
        a1.curr_scj_num,
        a1.curr_aspire_law_num,
        a1.curr_bk_bl_num,
        a1.curr_evans_mul_num,
        a1.curr_harvest_num,
        a1.curr_weltman_num,
        a1.curr_mrs_num,
        a1.curr_pb_capital_num,
        a1.curr_seq_num,
        a1.curr_scj_os,
        a1.curr_aspire_law_os,
        a1.curr_bk_bl_os,
        a1.curr_evans_mul_os,
        a1.curr_harvest_os,
        a1.curr_weltman_os,
        a1.curr_mrs_os,
        a1.curr_pb_capital_os,
        a1.curr_seq_os,

        -- Weekly Recoveries
        a1.post_co_recoveries,
        a1.internal_post_co_recoveries,
        a1.external_post_co_recoveries,
        a1.ext_aspire_law_recoveries,
        a1.ext_bk_bl_recoveries,
        a1.ext_evans_mul_recoveries,
        a1.ext_harvest_recoveries,
        a1.ext_weltman_recoveries,
        a1.ext_mrs_recoveries,
        a1.ext_pb_capital_recoveries,
        a1.ext_seq_recoveries,
        a1.ext_scj_recoveries,

        -- Running Recoveries
        a1.running_total_recoveries,
        a1.running_internal_recoveries,
        a1.running_external_recoveries,
        a1.running_ext_aspire_law,
        a1.running_ext_bk_bl,
        a1.running_ext_evans_mul,
        a1.running_ext_harvest,
        a1.running_ext_weltman,
        a1.running_ext_mrs,
        a1.running_ext_pb_capital,
        a1.running_ext_seq,
        a1.running_ext_scj,

        -- Recovery Rates
        a1.cumulative_recovery_rate,
        a1.internal_cumulative_rate,
        a1.external_cumulative_rate,
        a1.aspire_law_recovery_rate,
        a1.bk_bl_recovery_rate,
        a1.evans_mul_recovery_rate,
        a1.harvest_recovery_rate,
        a1.weltman_recovery_rate,
        a1.mrs_recovery_rate,
        a1.pb_capital_recovery_rate,
        a1.seq_recovery_rate,
        a1.scj_recovery_rate,

        -- Settlements
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
        a2.external_postco_amt_completed_settlements

    FROM analytics.credit.mk_postco_collections_metrics a1
    LEFT JOIN analytics.credit.mk_postco_settlements_weekly a2 
        ON a1.week_end_date = a2.week_end_date
),

net_os_co AS (
    SELECT 
        *,
        total_sum_os_co - running_total_recoveries AS net_total_sum_os_co,
        internal_sum_os_co - running_internal_recoveries AS net_internal_sum_os_co,
        external_sum_os_co - running_external_recoveries AS net_external_sum_os_co
    FROM base
),

final AS (
    SELECT 
        *,
        LAG(net_total_sum_os_co) OVER (ORDER BY week_end_date ASC) AS lag_net_total_sum_os_co,
        LAG(net_internal_sum_os_co) OVER (ORDER BY week_end_date ASC) AS lag_net_internal_sum_os_co,
        LAG(net_external_sum_os_co) OVER (ORDER BY week_end_date ASC) AS lag_net_external_sum_os_co
    FROM net_os_co
)

SELECT 
    *,
    -- Weekly Recovery Percentages
    ROUND(post_co_recoveries / NULLIF(lag_net_total_sum_os_co, 0), 4) AS perc_recovered_this_week,
    ROUND(running_total_recoveries / NULLIF(total_sum_os_co, 0), 4) AS perc_recovered_cumulative,
    
    -- Internal Weekly Recovery Percentage
    ROUND(internal_post_co_recoveries / NULLIF(lag_net_internal_sum_os_co, 0), 4) AS internal_perc_recovered_this_week,
    
    -- External Weekly Recovery Percentage
    ROUND(external_post_co_recoveries / NULLIF(lag_net_external_sum_os_co, 0), 4) AS external_perc_recovered_this_week
    
FROM final
ORDER BY week_end_date;

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
        bi.finance.payments_model pd, ReportingCutoffDate rcd -- Cross join to get the cutoff date
    WHERE
        pd.PAYMENT_PLANNED_TRANSMISSION_DATE <= rcd.last_wednesday_date
        AND pd.DIRECTION = 'D'
        AND pd.PAYMENT_STATUS = 'FUND'
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
        bi.finance.payments_model pd
            ON lcwp.LOAN_KEY = pd.LOAN_KEY
            AND pd.DIRECTION = 'D'
            AND pd.PAYMENT_STATUS = 'FUND'
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
        SELECT A1.*,
            flu_cr.loan_fx_rate
        FROM BI.FINANCE.payments_model A1
        LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY A2 
            ON A1.LOAN_KEY = A2.LOAN_KEY
        LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_cr ON flu_cr.LOAN_KEY = A1.LOAN_KEY
        WHERE
            A2.PRODUCT_TYPE <> 'Flexpay'
            AND A2.LOAN_CREATED_DATE = A2.EDATE
            AND A1.PAYMENT_STATUS = 'FUND'
            AND A1.DIRECTION = 'D'
            AND A1.PAYMENT_EVENT_TIME::DATE >= '2020-12-01'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY PAYMENT_ID, A1.LOAN_KEY
            ORDER BY 
                PAYMENT_EVENT_TIME DESC
        ) = 1
    ),
    base_collected AS (
        SELECT
            LOAN_KEY,
            PAYMENT_EVENT_TIME::DATE AS PAYMENT_DATE,
            sum(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) * COALESCE(loan_fx_rate, 1.0)) as total_collections
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


CREATE OR REPLACE TABLE analytics.credit.km_collections_entry_weekly AS
WITH first_table AS (
    -- Get latest loan operational status to exclude cancelled loans
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

-- Get ALL daily data (not just Wednesday snapshots)
daily_data AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.edate,
        fmd.dpd_days,
        fmd.dpd_bucket,
        fmd.outstanding_principal_due * COALESCE(flu_ent.loan_fx_rate, 1.0) AS outstanding_principal_due,
        fmd.is_charged_off,
        DATE_TRUNC('WEEK', fmd.edate::DATE + 4)::DATE + 2 AS collection_week
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_ent ON flu_ent.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.edate >= '2025-01-01'
)
-- select * from daily_data 
-- where fbbid = 2372800 and loan_key = 1811931 order by edate desc;
,weekly_data as 
(
Select l.*,
lag(min_dpd_bucket,1,null) over(partition by loan_key order by collection_week) as prev_week_min_dpd_bucket
from 
    (   select 
        loan_key,
        fbbid,
        collection_week,
        max(outstanding_principal_due) as outstanding_principal_due,
        min(dpd_bucket) as min_dpd_bucket,
        max(dpd_bucket) as max_dpd_bucket,
        from daily_data
        where is_charged_off = 0
        group by all
    )l
)
-- Select * from weekly_data 
-- where fbbid = 260198 and loan_key = 1750434 order by collection_week desc;

,entry_to_dpd as
(
select *,
case when (prev_week_min_dpd_bucket = 0 or prev_week_min_dpd_bucket is null) and min_dpd_bucket>0 then 1 else 0 
end as is_new_delin
from weekly_data
)

--------------------------------------------------------------------------------
-- Final Aggregation
--------------------------------------------------------------------------------
SELECT
    collection_week - 6 AS week_start_date,
    collection_week AS week_end_date,
        -- Total entries to collections
    COUNT(DISTINCT fbbid) AS num_cust_entering_collections,
    SUM(outstanding_principal_due) AS sum_os_entering_collections

FROM entry_to_dpd
where is_new_delin = 1
GROUP BY collection_week
ORDER BY collection_week;



CREATE OR REPLACE TABLE analytics.credit.km_collections_cured_weekly AS
WITH first_table AS (
    -- Get latest loan operational status to exclude cancelled loans
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

-- Get ALL daily data (not just Wednesday snapshots)
daily_data AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.edate,
        fmd.dpd_days,
        fmd.dpd_bucket,
        fmd.outstanding_principal_due * COALESCE(flu_cur.loan_fx_rate, 1.0) AS outstanding_principal_due,
        fmd.is_charged_off,
        DATE_TRUNC('WEEK', fmd.edate::DATE + 4)::DATE + 2 AS collection_week
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu_cur ON flu_cur.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.edate >= '2025-01-01'
)
-- select * from daily_data 
-- where fbbid = 2372800 and loan_key = 1811931 order by edate desc;
,weekly_data as 
(
Select l.*,
lag(max_dpd_bucket,1,null) over(partition by loan_key order by collection_week) as prev_week_max_dpd_bucket
from 
    (   select 
        loan_key,
        fbbid,
        collection_week,
        max(outstanding_principal_due) as outstanding_principal_due,
        min(dpd_bucket) as min_dpd_bucket,
        max(dpd_bucket) as max_dpd_bucket,
        from daily_data
        where is_charged_off = 0
        group by all
    )l
)
-- Select * from weekly_data 
-- where fbbid = 260198 and loan_key = 1750434 order by collection_week desc;

,entry_to_dpd as
(
select *,
case when (prev_week_max_dpd_bucket > 0 ) and max_dpd_bucket=0 then 1 else 0 
end as is_cured
from weekly_data
)

--------------------------------------------------------------------------------
-- Final Aggregation
--------------------------------------------------------------------------------
SELECT
    collection_week - 6 AS week_start_date,
    collection_week AS week_end_date,
        -- Total entries to collections
    COUNT(DISTINCT fbbid) AS num_cust_entering_collections,
    SUM(outstanding_principal_due) AS sum_os_entering_collections

FROM entry_to_dpd
where is_cured = 1
GROUP BY collection_week
ORDER BY collection_week desc;


----------------------------------------------- Master Consolidated Weekly Collections Table ----------------------------------------------------
-- Joins all underlying collections tables into a single dashboard-ready table
-- Source Tables:
--   a1: km_collections_preco_final_weekly (Pre-CO metrics, settlements, transitions)
--   a2: mk_postco_final_weekly (Post-CO placements, vendor recoveries)
--   a3: km_collections_ops_final (Operations metrics: FPD, Bankruptcy)
--   a4: km_collections_chof_payment_flow_rates (Cohort-based flow rates)
--   a5: km_collections_entry_weekly (Weekly delinquency entries)
--   a6: km_collections_cured_weekly (Weekly cure events)

CREATE OR REPLACE TABLE analytics.credit.km_collections_combined_rp_weekly AS
SELECT 
    -- ==================== BASE: Pre-CO Metrics (a1) ====================
    a1.week_start_date,
    a1.week_end_date,

    a1.snapshot_total_delinq_cust as dpd_1_13_num_snapshot,
    total_os_due_snapshot,
    (post_co_recoveries+total_collected_1_2+total_collected_3_8+total_collected_9_13) as total_recoveries,
    (total_collected_1_2+total_collected_3_8+total_collected_9_13) as preco_recoveries,

    (snapshot_total_delinq_cust+total_num_cust_co) as total_delinquent_inventory,

    (snapshot_total_os_1_90+total_sum_os_co) as Total_OS_1_co,
    
    -- Customer Counts
    ZEROIFNULL(a1.num_cust_1_2) AS num_cust_1_2,
    ZEROIFNULL(a1.num_cust_3_8) AS num_cust_3_8,
    ZEROIFNULL(a1.num_cust_9_13) AS num_cust_9_13,
    
    -- Outstanding Balances
    ZEROIFNULL(a1.sum_os_1_2) AS sum_os_1_2,
    ZEROIFNULL(a1.sum_os_3_8) AS sum_os_3_8,
    ZEROIFNULL(a1.sum_os_9_13) AS sum_os_9_13,
    ZEROIFNULL(a1.sum_odb_1_2) AS sum_odb_1_2,
    ZEROIFNULL(a1.sum_odb_3_8) AS sum_odb_3_8,
    ZEROIFNULL(a1.sum_odb_9_13) AS sum_odb_9_13,
    
    
    -- ==================== Snapshot Level Metrics ====================
    ZEROIFNULL(a1.snapshot_total_delinq_cust) AS snapshot_total_delinq_cust,
    ZEROIFNULL(a1.snapshot_total_os_1_90) AS snapshot_total_os_1_90,
    ZEROIFNULL(a1.snapshot_total_odb_1_90) AS snapshot_total_odb_1_90,
    ZEROIFNULL(a1.sum_os_1_2_snapshot) AS sum_os_1_2_snapshot,
    ZEROIFNULL(a1.sum_os_3_8_snapshot) AS sum_os_3_8_snapshot,
    ZEROIFNULL(a1.sum_os_9_13_snapshot) AS sum_os_9_13_snapshot,
    ZEROIFNULL(a1.sum_odb_1_2_snapshot) AS sum_odb_1_2_snapshot,
    ZEROIFNULL(a1.sum_odb_3_8_snapshot) AS sum_odb_3_8_snapshot,
    ZEROIFNULL(a1.sum_odb_9_13_snapshot) AS sum_odb_9_13_snapshot,
    
    -- ==================== Customer Cured/Delinq/Roll Counts ====================
    ZEROIFNULL(a1.num_cust_cured_1_2) AS num_cust_cured_1_2,
    ZEROIFNULL(a1.num_cust_cured_3_8) AS num_cust_cured_3_8,
    ZEROIFNULL(a1.num_cust_cured_9_13) AS num_cust_cured_9_13,
    ZEROIFNULL(a1.num_cust_delinq_wed_1_2) AS num_cust_delinq_wed_1_2,
    ZEROIFNULL(a1.num_cust_delinq_wed_3_8) AS num_cust_delinq_wed_3_8,
    ZEROIFNULL(a1.num_cust_delinq_wed_9_13) AS num_cust_delinq_wed_9_13,
    ZEROIFNULL(a1.num_cust_roll_wed_1_2) AS num_cust_roll_wed_1_2,
    ZEROIFNULL(a1.num_cust_roll_wed_3_8) AS num_cust_roll_wed_3_8,
    ZEROIFNULL(a1.num_cust_roll_wed_9_13) AS num_cust_roll_wed_9_13,
    ZEROIFNULL(a1.num_cust_Improved_wed_3_8) AS num_cust_improved_wed_3_8,
    ZEROIFNULL(a1.num_cust_Improved_wed_9_13) AS num_cust_improved_wed_9_13,
    
    -- ==================== OS Cured/Delinq/Roll/Improved ====================
    ZEROIFNULL(a1.sum_os_cured_1_2) AS sum_os_cured_1_2,
    ZEROIFNULL(a1.sum_os_cured_3_8) AS sum_os_cured_3_8,
    ZEROIFNULL(a1.sum_os_cured_9_13) AS sum_os_cured_9_13,
    ZEROIFNULL(a1.sum_os_delinq_wed_1_2) AS sum_os_delinq_wed_1_2,
    ZEROIFNULL(a1.sum_os_delinq_wed_3_8) AS sum_os_delinq_wed_3_8,
    ZEROIFNULL(a1.sum_os_delinq_wed_9_13) AS sum_os_delinq_wed_9_13,
    ZEROIFNULL(a1.sum_os_roll_wed_1_2) AS sum_os_roll_wed_1_2,
    ZEROIFNULL(a1.sum_os_roll_wed_3_8) AS sum_os_roll_wed_3_8,
    ZEROIFNULL(a1.sum_os_roll_wed_9_13) AS sum_os_roll_wed_9_13,
    ZEROIFNULL(a1.sum_os_Improved_wed_3_8) AS sum_os_improved_wed_3_8,
    ZEROIFNULL(a1.sum_os_Improved_wed_9_13) AS sum_os_improved_wed_9_13,
    
    -- ==================== ODB Cured/Delinq/Roll/Improved ====================
    ZEROIFNULL(a1.sum_odb_cured_1_2) AS sum_odb_cured_1_2,
    ZEROIFNULL(a1.sum_odb_cured_3_8) AS sum_odb_cured_3_8,
    ZEROIFNULL(a1.sum_odb_cured_9_13) AS sum_odb_cured_9_13,
    ZEROIFNULL(a1.sum_odb_delinq_wed_1_2) AS sum_odb_delinq_wed_1_2,
    ZEROIFNULL(a1.sum_odb_delinq_wed_3_8) AS sum_odb_delinq_wed_3_8,
    ZEROIFNULL(a1.sum_odb_delinq_wed_9_13) AS sum_odb_delinq_wed_9_13,
    ZEROIFNULL(a1.sum_odb_roll_wed_1_2) AS sum_odb_roll_wed_1_2,
    ZEROIFNULL(a1.sum_odb_roll_wed_3_8) AS sum_odb_roll_wed_3_8,
    ZEROIFNULL(a1.sum_odb_roll_wed_9_13) AS sum_odb_roll_wed_9_13,
    ZEROIFNULL(a1.sum_odb_Improved_wed_3_8) AS sum_odb_improved_wed_3_8,
    ZEROIFNULL(a1.sum_odb_Improved_wed_9_13) AS sum_odb_improved_wed_9_13,
    
    -- ==================== Percentage Overdue Balance ====================
    a1.pct_odb_1_2,
    a1.pct_odb_3_8,
    a1.pct_odb_9_13,
    
    -- ==================== Collections (Past Due and Total) ====================
    ZEROIFNULL(a1.past_due_collected_1_2) AS past_due_collected_1_2,
    ZEROIFNULL(a1.past_due_collected_3_8) AS past_due_collected_3_8,
    ZEROIFNULL(a1.past_due_collected_9_13) AS past_due_collected_9_13,
    ZEROIFNULL(a1.total_collected_1_2) AS collected_1_2,
    ZEROIFNULL(a1.total_collected_3_8) AS collected_3_8,
    ZEROIFNULL(a1.total_collected_9_13) AS collected_9_13,
    
    -- ==================== Collection Rates ====================
    a1.past_due_collection_rate_1_2_pct,
    a1.past_due_collection_rate_3_8_pct,
    a1.past_due_collection_rate_9_13_pct,
    a1.total_collection_rate_1_2_pct,
    a1.total_collection_rate_3_8_pct,
    a1.total_collection_rate_9_13_pct,
    
    -- Within-Week Transition Rates (already have COALESCE in source)
    a1.cure_rate_1_2_pct,
    a1.stay_rate_1_2_pct,
    a1.worsen_rate_1_2_pct,
    a1.cure_rate_3_8_pct,
    a1.improve_rate_3_8_pct,
    a1.stay_rate_3_8_pct,
    a1.worsen_rate_3_8_pct,
    a1.cure_rate_9_13_pct,
    a1.improve_rate_9_13_pct,
    a1.stay_rate_9_13_pct,
    a1.worsen_rate_9_13_pct,
    a1.cure_rate_1_2_pct_os,
    a1.stay_rate_1_2_pct_os,
    a1.worsen_rate_1_2_pct_os,
    a1.cure_rate_3_8_pct_os,
    a1.improve_rate_3_8_pct_os,
    a1.stay_rate_3_8_pct_os,
    a1.worsen_rate_3_8_pct_os,
    a1.cure_rate_9_13_pct_os,
    a1.improve_rate_9_13_pct_os,
    a1.stay_rate_9_13_pct_os,
    a1.worsen_rate_9_13_pct_os,

        -- Pre-CO Settlements
    ZEROIFNULL(a1.preco_num_settlements_1_2) AS preco_num_settlements_1_2,
    ZEROIFNULL(a1.preco_num_settlements_3_8) AS preco_num_settlements_3_8,
    ZEROIFNULL(a1.preco_num_settlements_9_13) AS preco_num_settlements_9_13,
    ZEROIFNULL(a1.preco_amt_settlements_1_2) AS preco_amt_settlements_1_2,
    ZEROIFNULL(a1.preco_amt_settlements_3_8) AS preco_amt_settlements_3_8,
    ZEROIFNULL(a1.preco_amt_settlements_9_13) AS preco_amt_settlements_9_13,
    ZEROIFNULL(a1.preco_num_completed_settlements_1_2) AS preco_num_completed_settlements_1_2,
    ZEROIFNULL(a1.preco_num_completed_settlements_3_8) AS preco_num_completed_settlements_3_8,
    ZEROIFNULL(a1.preco_num_completed_settlements_9_13) AS preco_num_completed_settlements_9_13,
    ZEROIFNULL(a1.preco_amt_completed_settlements_1_2) AS preco_amt_completed_settlements_1_2,
    ZEROIFNULL(a1.preco_amt_completed_settlements_3_8) AS preco_amt_completed_settlements_3_8,
    ZEROIFNULL(a1.preco_amt_completed_settlements_9_13) AS preco_amt_completed_settlements_9_13,

    -- ==================== Post-CO Metrics (a2) ====================
    -- Total CO Metrics
    ZEROIFNULL(a2.total_num_cust_co) AS total_num_cust_co,
    ZEROIFNULL(a2.internal_num_cust_co) AS internal_num_cust_co,
    ZEROIFNULL(a2.external_num_cust_co) AS external_num_cust_co,
    ZEROIFNULL(a2.new_num_cust_co) AS new_num_cust_co,
    ZEROIFNULL(a2.total_sum_os_co) AS total_sum_os_co,
    ZEROIFNULL(a2.internal_sum_os_co) AS internal_sum_os_co,
    ZEROIFNULL(a2.external_sum_os_co) AS external_sum_os_co,
    ZEROIFNULL(a2.new_sum_os_co) AS new_sum_os_co,
    
    -- New Placements This Week
    ZEROIFNULL(a2.new_external_placements) AS new_external_placements,
    ZEROIFNULL(a2.new_external_placed_os) AS new_external_placed_os,
    ZEROIFNULL(a2.new_scj_placements) AS new_scj_placements,
    ZEROIFNULL(a2.new_scj_placed_os) AS new_scj_placed_os,
    ZEROIFNULL(a2.new_aspire_law_placements) AS new_aspire_law_placements,
    ZEROIFNULL(a2.new_aspire_law_placed_os) AS new_aspire_law_placed_os,
    ZEROIFNULL(a2.new_bk_bl_placements) AS new_bk_bl_placements,
    ZEROIFNULL(a2.new_bk_bl_placed_os) AS new_bk_bl_placed_os,
    ZEROIFNULL(a2.new_evans_mul_placements) AS new_evans_mul_placements,
    ZEROIFNULL(a2.new_evans_mul_placed_os) AS new_evans_mul_placed_os,
    ZEROIFNULL(a2.new_harvest_placements) AS new_harvest_placements,
    ZEROIFNULL(a2.new_harvest_placed_os) AS new_harvest_placed_os,
    ZEROIFNULL(a2.new_weltman_placements) AS new_weltman_placements,
    ZEROIFNULL(a2.new_weltman_placed_os) AS new_weltman_placed_os,
    ZEROIFNULL(a2.new_mrs_placements) AS new_mrs_placements,
    ZEROIFNULL(a2.new_mrs_placed_os) AS new_mrs_placed_os,
    ZEROIFNULL(a2.new_pb_capital_placements) AS new_pb_capital_placements,
    ZEROIFNULL(a2.new_pb_capital_placed_os) AS new_pb_capital_placed_os,
    ZEROIFNULL(a2.new_seq_placements) AS new_seq_placements,
    ZEROIFNULL(a2.new_seq_placed_os) AS new_seq_placed_os,
    a2.new_harvest_placements+new_weltman_placements+new_mrs_placements as other_placements,
    (a2.new_harvest_placed_os+new_mrs_placed_os+new_weltman_placed_os) as other_palcement_os,
    
    
    -- Cumulative Placements (currently with vendor)
    ZEROIFNULL(a2.cum_external_placements) AS cum_external_placements,
    ZEROIFNULL(a2.cum_external_placed_os) AS cum_external_placed_os,
    ZEROIFNULL(a2.cum_scj_placements) AS cum_scj_placements,
    ZEROIFNULL(a2.cum_scj_placed_os) AS cum_scj_placed_os,
    ZEROIFNULL(a2.cum_aspire_law_placements) AS cum_aspire_law_placements,
    ZEROIFNULL(a2.cum_aspire_law_placed_os) AS cum_aspire_law_placed_os,
    ZEROIFNULL(a2.cum_bk_bl_placements) AS cum_bk_bl_placements,
    ZEROIFNULL(a2.cum_bk_bl_placed_os) AS cum_bk_bl_placed_os,
    ZEROIFNULL(a2.cum_evans_mul_placements) AS cum_evans_mul_placements,
    ZEROIFNULL(a2.cum_evans_mul_placed_os) AS cum_evans_mul_placed_os,
    ZEROIFNULL(a2.cum_harvest_placements) AS cum_harvest_placements,
    ZEROIFNULL(a2.cum_harvest_placed_os) AS cum_harvest_placed_os,
    ZEROIFNULL(a2.cum_weltman_placements) AS cum_weltman_placements,
    ZEROIFNULL(a2.cum_weltman_placed_os) AS cum_weltman_placed_os,
    ZEROIFNULL(a2.cum_mrs_placements) AS cum_mrs_placements,
    ZEROIFNULL(a2.cum_mrs_placed_os) AS cum_mrs_placed_os,
    ZEROIFNULL(a2.cum_pb_capital_placements) AS cum_pb_capital_placements,
    ZEROIFNULL(a2.cum_pb_capital_placed_os) AS cum_pb_capital_placed_os,
    ZEROIFNULL(a2.cum_seq_placements) AS cum_seq_placements,
    ZEROIFNULL(a2.cum_seq_placed_os) AS cum_seq_placed_os,
    (ZEROIFNULL(a2.cum_harvest_placements) + ZEROIFNULL(a2.cum_weltman_placements) + ZEROIFNULL(a2.cum_mrs_placements)) AS other_cum_placements,
    (ZEROIFNULL(a2.cum_harvest_placed_os) + ZEROIFNULL(a2.cum_weltman_placed_os) + ZEROIFNULL(a2.cum_mrs_placed_os)) AS other_cum_placed_os,


    
    
    -- Cumulative Total Balance Placed by Vendor
    ZEROIFNULL(a2.cum_scj_placed_tb) AS cum_scj_placed_tb,
    ZEROIFNULL(a2.cum_aspire_law_placed_tb) AS cum_aspire_law_placed_tb,
    ZEROIFNULL(a2.cum_bk_bl_placed_tb) AS cum_bk_bl_placed_tb,
    ZEROIFNULL(a2.cum_evans_mul_placed_tb) AS cum_evans_mul_placed_tb,
    ZEROIFNULL(a2.cum_harvest_placed_tb) AS cum_harvest_placed_tb,
    ZEROIFNULL(a2.cum_weltman_placed_tb) AS cum_weltman_placed_tb,
    ZEROIFNULL(a2.cum_mrs_placed_tb) AS cum_mrs_placed_tb,
    ZEROIFNULL(a2.cum_pb_capital_placed_tb) AS cum_pb_capital_placed_tb,
    ZEROIFNULL(a2.cum_seq_placed_tb) AS cum_seq_placed_tb,
    ZEROIFNULL(a2.cum_external_placed_tb) AS cum_external_placed_tb,
    (ZEROIFNULL(a2.cum_harvest_placed_tb) + ZEROIFNULL(a2.cum_weltman_placed_tb) + ZEROIFNULL(a2.cum_mrs_placed_tb)) AS other_external_vendor_tb,
    
    -- Current Vendor Snapshot (as of this week)
    ZEROIFNULL(a2.curr_scj_num) AS curr_scj_num,
    ZEROIFNULL(a2.curr_aspire_law_num) AS curr_aspire_law_num,
    ZEROIFNULL(a2.curr_bk_bl_num) AS curr_bk_bl_num,
    ZEROIFNULL(a2.curr_evans_mul_num) AS curr_evans_mul_num,
    ZEROIFNULL(a2.curr_harvest_num) AS curr_harvest_num,
    ZEROIFNULL(a2.curr_weltman_num) AS curr_weltman_num,
    ZEROIFNULL(a2.curr_mrs_num) AS curr_mrs_num,
    ZEROIFNULL(a2.curr_pb_capital_num) AS curr_pb_capital_num,
    ZEROIFNULL(a2.curr_seq_num) AS curr_seq_num,
    ZEROIFNULL(a2.curr_scj_os) AS curr_scj_os,
    ZEROIFNULL(a2.curr_aspire_law_os) AS curr_aspire_law_os,
    ZEROIFNULL(a2.curr_bk_bl_os) AS curr_bk_bl_os,
    ZEROIFNULL(a2.curr_evans_mul_os) AS curr_evans_mul_os,
    ZEROIFNULL(a2.curr_harvest_os) AS curr_harvest_os,
    ZEROIFNULL(a2.curr_weltman_os) AS curr_weltman_os,
    ZEROIFNULL(a2.curr_mrs_os) AS curr_mrs_os,
    ZEROIFNULL(a2.curr_pb_capital_os) AS curr_pb_capital_os,
    ZEROIFNULL(a2.curr_seq_os) AS curr_seq_os,
    (ZEROIFNULL(a2.curr_harvest_num) + ZEROIFNULL(a2.curr_weltman_num) + ZEROIFNULL(a2.curr_mrs_num)) AS other_curr_vendor_num,
    (ZEROIFNULL(a2.curr_harvest_os) + ZEROIFNULL(a2.curr_weltman_os) + ZEROIFNULL(a2.curr_mrs_os)) AS other_curr_vendor_os,
    
    
    -- Weekly Recoveries
    ZEROIFNULL(a2.post_co_recoveries) AS post_co_recoveries,
    ZEROIFNULL(a2.internal_post_co_recoveries) AS internal_post_co_recoveries,
    ZEROIFNULL(a2.external_post_co_recoveries) AS external_post_co_recoveries,
    ZEROIFNULL(a2.ext_aspire_law_recoveries) AS ext_aspire_law_recoveries,
    ZEROIFNULL(a2.ext_bk_bl_recoveries) AS ext_bk_bl_recoveries,
    ZEROIFNULL(a2.ext_evans_mul_recoveries) AS ext_evans_mul_recoveries,
    ZEROIFNULL(a2.ext_harvest_recoveries) AS ext_harvest_recoveries,
    ZEROIFNULL(a2.ext_weltman_recoveries) AS ext_weltman_recoveries,
    ZEROIFNULL(a2.ext_mrs_recoveries) AS ext_mrs_recoveries,
    ZEROIFNULL(a2.ext_pb_capital_recoveries) AS ext_pb_capital_recoveries,
    ZEROIFNULL(a2.ext_seq_recoveries) AS ext_seq_recoveries,
    ZEROIFNULL(a2.ext_scj_recoveries) AS ext_scj_recoveries,
    (ZEROIFNULL(a2.ext_harvest_recoveries) + ZEROIFNULL(a2.ext_weltman_recoveries) + ZEROIFNULL(a2.ext_mrs_recoveries)) AS other_ext_recoveries,

    -- Running Recoveries
    ZEROIFNULL(a2.running_total_recoveries) AS running_total_recoveries,
    ZEROIFNULL(a2.running_internal_recoveries) AS running_internal_recoveries,
    ZEROIFNULL(a2.running_external_recoveries) AS running_external_recoveries,
    ZEROIFNULL(a2.running_ext_aspire_law) AS running_ext_aspire_law,
    ZEROIFNULL(a2.running_ext_bk_bl) AS running_ext_bk_bl,
    ZEROIFNULL(a2.running_ext_evans_mul) AS running_ext_evans_mul,
    ZEROIFNULL(a2.running_ext_harvest) AS running_ext_harvest,
    ZEROIFNULL(a2.running_ext_weltman) AS running_ext_weltman,
    ZEROIFNULL(a2.running_ext_mrs) AS running_ext_mrs,
    ZEROIFNULL(a2.running_ext_pb_capital) AS running_ext_pb_capital,
    ZEROIFNULL(a2.running_ext_seq) AS running_ext_seq,
    ZEROIFNULL(a2.running_ext_scj) AS running_ext_scj,
    (ZEROIFNULL(a2.running_ext_harvest) + ZEROIFNULL(a2.running_ext_weltman) + ZEROIFNULL(a2.running_ext_mrs) ) AS other_running_ext_recoveries,
    
    -- Recovery Rates (keep as-is, already percentages)
    a2.cumulative_recovery_rate,
    a2.internal_cumulative_rate,
    a2.external_cumulative_rate,
    a2.aspire_law_recovery_rate,
    a2.bk_bl_recovery_rate,
    a2.evans_mul_recovery_rate,
    a2.harvest_recovery_rate,
    a2.weltman_recovery_rate,
    a2.mrs_recovery_rate,
    a2.pb_capital_recovery_rate,
    a2.seq_recovery_rate,
    a2.scj_recovery_rate,

    -- Post-CO Settlements
    ZEROIFNULL(a2.postco_num_settlements) AS postco_num_settlements,
    ZEROIFNULL(a2.postco_amt_settlements) AS postco_amt_settlements,
    ZEROIFNULL(a2.internal_postco_num_settlements) AS internal_postco_num_settlements,
    ZEROIFNULL(a2.external_postco_num_settlements) AS external_postco_num_settlements,
    ZEROIFNULL(a2.internal_postco_amt_settlements) AS internal_postco_amt_settlements,
    ZEROIFNULL(a2.external_postco_amt_settlements) AS external_postco_amt_settlements,
    ZEROIFNULL(a2.postco_num_completed_settlements) AS postco_num_completed_settlements,
    ZEROIFNULL(a2.postco_amt_completed_settlements) AS postco_amt_completed_settlements,
    ZEROIFNULL(a2.internal_postco_num_completed_settlements) AS internal_postco_num_completed_settlements,
    ZEROIFNULL(a2.external_postco_num_completed_settlements) AS external_postco_num_completed_settlements,
    ZEROIFNULL(a2.internal_postco_amt_completed_settlements) AS internal_postco_amt_completed_settlements,
    ZEROIFNULL(a2.external_postco_amt_completed_settlements) AS external_postco_amt_completed_settlements,
    
    -- Net Outstanding and Recovery Percentages
    ZEROIFNULL(a2.net_total_sum_os_co) AS net_total_sum_os_co,
--    ZEROIFNULL(a2.lag_net_total_sum_os_co) AS lag_net_total_sum_os_co,
    a2.perc_recovered_this_week,
  --  a2.perc_recovered_until_this_week,

    -- ==================== Operations Metrics (a3) ====================
    ZEROIFNULL(a3.total_loans_originated) AS total_loans_originated,
    ZEROIFNULL(a3.count_first_payment_default) AS count_first_payment_default,
    ZEROIFNULL(a3.count_first_two_payments_default) AS count_first_two_payments_default,
    ZEROIFNULL(a3.count_first_three_payments_default) AS count_first_three_payments_default,
    ZEROIFNULL(a3.count_no_funding_and_charge_off) AS count_no_funding_and_charge_off,
    ZEROIFNULL(a3.count_total_charge_off_for_metric4) AS count_total_charge_off_for_metric4,
    
    -- Bankruptcy Metrics
    ZEROIFNULL(a3.total_num_bk) AS total_num_bk,
    ZEROIFNULL(a3.total_sum_bk) AS total_sum_bk,
    ZEROIFNULL(a3.new_num_bk) AS new_num_bk,
    ZEROIFNULL(a3.new_sum_bk) AS new_sum_bk,

    -- ==================== Flow Rates (a4) ====================
    ZEROIFNULL(a4.total_entry_os) AS flow_total_entry_os,
    ZEROIFNULL(a4.total_chargeoff_os) AS flow_total_chargeoff_os,
    ZEROIFNULL(a4.total_collections) AS flow_total_collections,
    ZEROIFNULL(a4.total_collections_120) AS flow_total_collections_120,
    a4.chargeoff_flow_rate,
    a4.collection_flow_rate,
    a4.collection_flow_rate_120,
    a4.total_flow_rate,

    -- ==================== Entry Metrics (a5) ====================
    ZEROIFNULL(a5.num_cust_entering_collections) AS num_cust_entering_collections,
    ZEROIFNULL(a5.sum_os_entering_collections) AS sum_os_entering_collections,

    -- ==================== Cured Metrics (a6) ====================
    ZEROIFNULL(a6.num_cust_entering_collections) AS num_cust_cured,
    ZEROIFNULL(a6.sum_os_entering_collections) AS sum_os_cured,

    -- ==================== CALCULATED: Total Portfolio Cash Inflow ====================
    (ZEROIFNULL(a1.total_collected_1_2) + ZEROIFNULL(a1.total_collected_3_8) + ZEROIFNULL(a1.total_collected_9_13)) + ZEROIFNULL(a2.post_co_recoveries) AS      total_portfolio_cash_inflow

FROM analytics.credit.km_collections_preco_final_weekly a1
LEFT JOIN analytics.credit.mk_postco_final_weekly a2 ON a1.week_end_date = a2.week_end_date
LEFT JOIN analytics.credit.km_collections_ops_final a3 ON a1.week_end_date = a3.week_end_date
LEFT JOIN analytics.credit.km_collections_chof_payment_flow_rates a4 ON a1.week_end_date = a4.cohort_week
LEFT JOIN analytics.credit.km_collections_entry_weekly a5 ON a1.week_end_date = a5.week_end_date
LEFT JOIN analytics.credit.km_collections_cured_weekly a6 ON a1.week_end_date = a6.week_end_date
ORDER BY a1.week_end_date;

