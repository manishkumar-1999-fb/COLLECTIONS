/*
================================================================================
DPD 1-2 RESEARCH - ENHANCED DEEP DIVE ANALYSIS
================================================================================
Purpose: Understand why ~75% of customers stay stuck in early-stage delinquency 
         buckets (DPD 1-2) without curing or rolling forward.

Key Enhancements:
1. Payment Behavior Attribution (Partial Payers vs Silent/Non-Payers)
2. Dialer Gap Analysis (days to first collection attempt, >7 day flags)
3. Financial Health (Can_Afford_Cure boolean)
4. State Machine/Movement tracking (DPD 1 ↔ DPD 2 oscillation)
5. Reviewed join logic to prevent data loss

Analysis Date: 2025-09-17
================================================================================
*/

-- ============================================================================
-- SECTION 1: BASE DELINQUENT ACCOUNTS
-- ============================================================================

WITH relevant_accnts AS (
    SELECT 
        fbbid, 
        edate, 
        dpd_days::INTEGER AS dpd_days, 
        dpd_bucket::INTEGER AS dpd_bucket,  
        SUM(outstanding_balance_due) AS outstanding_balance_due
    FROM (
        SELECT 
            fmd.fbbid,
            fmd.edate,
            MAX(fmd.dpd_bucket) AS dpd_bucket,
            MAX(fmd.dpd_days) AS dpd_days,
            SUM(fmd.outstanding_balance_due) AS outstanding_balance_due
        FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
        INNER JOIN bi.public.customers_data cd
            ON fmd.fbbid = cd.fbbid
        WHERE fmd.dpd_days IS NOT NULL 
            AND fmd.is_charged_off = 0 
            AND fmd.edate = '2025-09-17'
        GROUP BY fmd.fbbid, fmd.edate
    ) t
    WHERE dpd_days >= 1 AND dpd_days <= 14
    GROUP BY fbbid, edate, dpd_days, dpd_bucket
),

-- Previous week snapshot for comparison
previous_week AS (
    SELECT 
        fbbid, 
        edate, 
        dpd_days::INTEGER AS dpd_days, 
        dpd_bucket::INTEGER AS dpd_bucket,  
        SUM(outstanding_balance_due) AS outstanding_balance_due
    FROM (
        SELECT 
            fmd.fbbid,
            fmd.edate,
            MAX(fmd.dpd_bucket) AS dpd_bucket,
            MAX(fmd.dpd_days) AS dpd_days,
            SUM(fmd.outstanding_balance_due) AS outstanding_balance_due
        FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
        INNER JOIN bi.public.customers_data cd
            ON fmd.fbbid = cd.fbbid
        WHERE fmd.dpd_days IS NOT NULL 
            AND fmd.is_charged_off = 0 
            AND fmd.edate = '2025-09-10'
        GROUP BY fmd.fbbid, fmd.edate
    ) t
    WHERE dpd_days >= 1 AND dpd_days <= 14
    GROUP BY fbbid, edate, dpd_days, dpd_bucket
),

-- ============================================================================
-- SECTION 2: TOTAL OVERDUE BALANCE CALCULATION
-- ============================================================================

total_overdue AS (
    SELECT
        T2.EDATE AS week_end_date,
        T1.FBBID,
        T1.LOAN_KEY,
        SUM(T1.STATUS_VALUE) AS total_overdue_balance
    FROM BI.FINANCE.LOAN_STATUSES T1
    INNER JOIN BI.INTERNAL.DATES T2
        ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
        AND DAYOFWEEK(T2.EDATE) = 3
        AND T2.EDATE <= CURRENT_DATE
    INNER JOIN (
        SELECT DISTINCT loan_key, edate
        FROM BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE loan_operational_status <> 'CNCL'
            AND is_charged_off = 0
    ) t3
        ON t3.loan_key = T1.loan_key
        AND t3.edate = T2.EDATE
    WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
    GROUP BY T2.EDATE, T1.FBBID, T1.LOAN_KEY
),

overdue_balance AS (
    SELECT 
        fbbid,
        week_end_date,
        SUM(total_overdue_balance) AS tob
    FROM total_overdue
    WHERE week_end_date = '2025-09-17'
    GROUP BY fbbid, week_end_date
),

-- ============================================================================
-- SECTION 3: BASE WITH WEEK-OVER-WEEK CLASSIFICATION
-- ============================================================================

base AS (
    SELECT 
        a.*,
        b.dpd_bucket AS previous_bucket,
        CASE 
            WHEN b.edate IS NULL THEN 'New_Entry'
            ELSE 'Roll_Over' 
        END AS cust_classification,
        CASE   
            WHEN b.dpd_bucket IS NULL THEN 'New_Entry'
            WHEN a.dpd_bucket > b.dpd_bucket THEN 'Rolled_Forward'
            WHEN a.dpd_bucket < b.dpd_bucket THEN 'Partially_Cured'
            WHEN a.dpd_bucket = b.dpd_bucket THEN 'Stuck_Same_Bucket'
        END AS roll_over_classification,
        ob.tob AS overdue_balance,
        ob.tob AS pd_amount  -- PD Amount = Past Due Amount from LOAN_STATUSES (APD_LOAN_TOTAL_AMOUNT)
    FROM relevant_accnts a
    LEFT JOIN previous_week b
        ON a.fbbid = b.fbbid
    LEFT JOIN overdue_balance ob 
        ON a.fbbid = ob.fbbid
),

-- ============================================================================
-- SECTION 4: DPD TRANSITIONS & STATE MACHINE TRACKING
-- ============================================================================

dpd_daily_max AS (
    SELECT
        FBBID,
        EDATE,
        MAX(DPD_DAYS) AS max_dpd_days,
        MAX(dpd_bucket) AS dpd_bucket
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate <= '2025-09-17'
    GROUP BY FBBID, EDATE
),

-- Track all DPD bucket transitions for oscillation analysis
dpd_transitions AS (
    SELECT
        FBBID,
        DATE(EDATE) AS transition_edate,
        DPD_BUCKET AS current_transition_bucket,
        LAG(DPD_BUCKET, 1) OVER (PARTITION BY FBBID ORDER BY EDATE) AS previous_dpd_bucket,
        LAG(DATE(EDATE), 1) OVER (PARTITION BY FBBID ORDER BY EDATE) AS previous_transition_edate,
        ROW_NUMBER() OVER (PARTITION BY FBBID ORDER BY EDATE DESC) AS transition_rank
    FROM (
        SELECT
            FBBID,
            EDATE,
            DPD_BUCKET,
            LAG(DPD_BUCKET, 1, NULL) OVER (PARTITION BY FBBID ORDER BY EDATE) AS PREV_DAILY_DPD_BUCKET
        FROM dpd_daily_max
    ) AS T
    WHERE (T.PREV_DAILY_DPD_BUCKET IS NULL)
        OR (T.DPD_BUCKET IS NOT NULL AND T.PREV_DAILY_DPD_BUCKET IS NOT NULL AND T.DPD_BUCKET != T.PREV_DAILY_DPD_BUCKET)
        OR (T.DPD_BUCKET IS NULL AND T.PREV_DAILY_DPD_BUCKET IS NOT NULL)
),

-- NEW: State Machine Analysis - Track DPD 1/2 oscillation frequency
dpd_oscillation_analysis AS (
    SELECT 
        fbbid,
        COUNT(*) AS total_bucket_transitions,
        SUM(CASE 
            WHEN (current_transition_bucket = 1 AND previous_dpd_bucket = 2)
              OR (current_transition_bucket = 2 AND previous_dpd_bucket = 1) 
            THEN 1 ELSE 0 
        END) AS dpd_1_2_oscillation_count,
        MAX(CASE 
            WHEN current_transition_bucket IN (1, 2) 
            THEN transition_edate 
        END) AS last_dpd_1_2_date,
        LISTAGG(
            COALESCE(current_transition_bucket::VARCHAR, 'NULL'), 
            ' -> '
        ) WITHIN GROUP (ORDER BY transition_edate) AS bucket_movement_path
    FROM dpd_transitions
    WHERE transition_edate >= DATEADD('day', -90, '2025-09-17')
    GROUP BY fbbid
),

latest_dpd_details AS (
    SELECT
        FBBID,
        transition_edate AS latest_dpd_edate,
        previous_transition_edate,
        previous_dpd_bucket
    FROM dpd_transitions
    WHERE transition_rank = 1
),

-- NEW: First Missed Payment Date (FMD) tracking
first_missed_payment AS (
    SELECT 
        fbbid,
        MIN(edate) AS first_missed_payment_date
    FROM dpd_daily_max
    WHERE dpd_bucket >= 1
    GROUP BY fbbid
),

with_latest AS (
    SELECT 
        b.*,
        ldd.latest_dpd_edate,
        ldd.previous_transition_edate,
        ldd.previous_dpd_bucket,
        fmp.first_missed_payment_date,
        DATEDIFF('day', ldd.latest_dpd_edate, b.edate) AS days_since_latest_dpd_change,
        DATEDIFF('day', ldd.previous_transition_edate, ldd.latest_dpd_edate) AS days_in_previous_dpd_state,
        DATEDIFF('day', fmp.first_missed_payment_date, b.edate) AS days_since_first_missed_payment,
        oa.total_bucket_transitions,
        oa.dpd_1_2_oscillation_count,
        oa.bucket_movement_path,
        CASE 
            WHEN oa.dpd_1_2_oscillation_count >= 3 THEN 'Perpetual_Lagger'
            WHEN oa.dpd_1_2_oscillation_count >= 1 THEN 'Occasional_Oscillator'
            ELSE 'Steady_State'
        END AS oscillation_behavior
    FROM base b
    LEFT JOIN latest_dpd_details ldd
        ON b.fbbid = ldd.fbbid
    LEFT JOIN first_missed_payment fmp
        ON b.fbbid = fmp.fbbid
    LEFT JOIN dpd_oscillation_analysis oa
        ON b.fbbid = oa.fbbid
),

-- ============================================================================
-- SECTION 5: ENHANCED PAYMENT BEHAVIOR ATTRIBUTION
-- ============================================================================

payments AS (
    SELECT 
        fbbid,
        payment_authorization_date,
        payment_planned_transmission_date,
        payment_status,
        DATE(payment_last_status_change_time) AS payment_last_status_change_time,
        product,
        is_refund,
        is_funded_payment,
        SUM(payment_total_amount) AS total_amount,
        SUM(payment_principal_amount) AS payment_principal_amount,
        SUM(transaction_amount) AS transaction_amount,
        SUM(payment_fees_total_amount) AS payment_fees_total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY fbbid, payment_planned_transmission_date 
            ORDER BY payment_planned_transmission_date
        ) AS rn
    FROM bi.finance.payments_data
    GROUP BY 
        fbbid,
        payment_authorization_date,
        payment_planned_transmission_date,
        payment_status,
        DATE(payment_last_status_change_time),
        product,
        is_refund,
        is_funded_payment
),

-- NEW: Enhanced payment behavior after FMD
payment_behavior_post_fmd AS (
    SELECT 
        wl.fbbid,
        wl.first_missed_payment_date,
        COUNT(DISTINCT CASE 
            WHEN p.payment_planned_transmission_date > wl.first_missed_payment_date 
                AND p.payment_status = 'FUND' 
            THEN p.payment_planned_transmission_date 
        END) AS successful_payments_post_fmd,
        COUNT(DISTINCT CASE 
            WHEN p.payment_planned_transmission_date > wl.first_missed_payment_date 
                AND p.payment_status IN ('DELQ', 'FAIL', 'DECL') 
            THEN p.payment_planned_transmission_date 
        END) AS failed_payments_post_fmd,
        COUNT(DISTINCT CASE 
            WHEN p.payment_planned_transmission_date > wl.first_missed_payment_date 
            THEN p.payment_planned_transmission_date 
        END) AS total_payment_attempts_post_fmd,
        SUM(CASE 
            WHEN p.payment_planned_transmission_date > wl.first_missed_payment_date 
                AND p.payment_status = 'FUND' 
            THEN p.total_amount ELSE 0 
        END) AS total_amount_paid_post_fmd
    FROM with_latest wl
    LEFT JOIN payments p 
        ON wl.fbbid = p.fbbid
    GROUP BY wl.fbbid, wl.first_missed_payment_date
),

payment_flags AS (
    SELECT 
        wl.*,
        pb.successful_payments_post_fmd,
        pb.failed_payments_post_fmd,
        pb.total_payment_attempts_post_fmd,
        pb.total_amount_paid_post_fmd,
        SUM(CASE 
            WHEN p.payment_planned_transmission_date > wl.latest_dpd_edate 
                AND p.payment_status = 'FUND' 
            THEN 1 ELSE 0 
        END) AS on_time_payments,
        SUM(CASE 
            WHEN p.payment_planned_transmission_date >= wl.edate 
            THEN 1 ELSE 0 
        END) AS pending_payments,
        -- NEW: Payment Behavior Attribution
        CASE 
            WHEN pb.successful_payments_post_fmd > 0 AND wl.dpd_days > 0 THEN 'Partial_Payer'
            WHEN pb.total_payment_attempts_post_fmd > 0 AND pb.successful_payments_post_fmd = 0 THEN 'Failed_Payer'
            WHEN pb.total_payment_attempts_post_fmd = 0 OR pb.total_payment_attempts_post_fmd IS NULL THEN 'Silent_Non_Payer'
            ELSE 'Other'
        END AS payment_behavior_category
    FROM with_latest wl
    LEFT JOIN payments p 
        ON wl.fbbid = p.fbbid
    LEFT JOIN payment_behavior_post_fmd pb
        ON wl.fbbid = pb.fbbid
    GROUP BY 
        wl.fbbid, wl.edate, wl.dpd_days, wl.dpd_bucket, wl.outstanding_balance_due, 
        wl.previous_bucket, wl.cust_classification, wl.roll_over_classification,
        wl.overdue_balance, wl.pd_amount, wl.latest_dpd_edate, wl.previous_transition_edate, 
        wl.previous_dpd_bucket, wl.first_missed_payment_date, wl.days_since_latest_dpd_change,
        wl.days_in_previous_dpd_state, wl.days_since_first_missed_payment, 
        wl.total_bucket_transitions, wl.dpd_1_2_oscillation_count, wl.bucket_movement_path,
        wl.oscillation_behavior, pb.successful_payments_post_fmd, pb.failed_payments_post_fmd,
        pb.total_payment_attempts_post_fmd, pb.total_amount_paid_post_fmd
),

-- ============================================================================
-- SECTION 6: ENHANCED DIALER GAP ANALYSIS
-- ============================================================================

-- Source 1: Five9 Call Log
source_1 AS (
    SELECT DISTINCT 
        FBBID,
        Date_call AS call_date,
        Disposition
    FROM analytics.credit.v_five9_call_log
    WHERE FBBID IS NOT NULL
),

-- Source 2: Settlement Master Table
source_3 AS (
    SELECT DISTINCT 
        fbbid,
        dispostion AS disposition,
        last_ob_attempt_date AS call_date
    FROM tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW t
    WHERE fbbid IS NOT NULL
),

-- Source 3: Salesforce
source_4 AS (
    SELECT DISTINCT 
        TRY_TO_NUMBER(fundbox_id__c) AS fbbid,
        calldisposition AS disposition,
        LASTMODIFIEDDATE::DATE AS call_date
    FROM external_data_sources.salesforce_nova.task sf
    WHERE TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
),

-- Unified dialer data
final_dialer AS (
    SELECT
        COALESCE(s4.fbbid, s3.fbbid, s1.fbbid) AS FBBID,
        COALESCE(s4.disposition, s3.disposition, s1.disposition) AS disposition,
        COALESCE(s4.call_date, s3.call_date, s1.call_date) AS date_call
    FROM source_1 s1 
    FULL OUTER JOIN source_3 s3
        ON s1.fbbid = s3.fbbid
        AND s1.call_date = s3.call_date
    FULL OUTER JOIN source_4 s4
        ON COALESCE(s3.fbbid, s1.fbbid) = s4.fbbid
        AND COALESCE(s3.call_date, s1.call_date) = s4.call_date
    WHERE COALESCE(s4.fbbid, s3.fbbid, s1.fbbid) IS NOT NULL
),

-- NEW: First collection attempt tracking for gap analysis
first_collection_attempt AS (
    SELECT 
        fbbid,
        MIN(date_call) AS first_call_date
    FROM final_dialer
    GROUP BY fbbid
),

customer_call_analysis AS (
    SELECT
        cd.FBBID,
        cd.latest_dpd_edate,
        d.date_call,
        d.disposition,
        DATEDIFF('DAY', cd.latest_dpd_edate, d.date_call) AS day_diff,
        CASE WHEN d.disposition = 'RPC' THEN 1 ELSE 0 END AS is_rpc
    FROM payment_flags cd
    LEFT JOIN final_dialer d
        ON cd.FBBID = d.FBBID
        AND d.date_call >= cd.latest_dpd_edate
),

dialer_data AS (
    SELECT
        cca.FBBID,
        cca.latest_dpd_edate,
        MAX(CASE WHEN cca.day_diff BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS flag_call_next_7_days,
        MAX(CASE WHEN cca.day_diff BETWEEN 8 AND 14 THEN 1 ELSE 0 END) AS flag_call_days_8_to_14,
        MAX(CASE WHEN cca.day_diff IS NOT NULL THEN 1 ELSE 0 END) AS flag_any_call_after_dpd,
        MAX(cca.is_rpc) AS flag_rpc,
        -- NEW: Dialer Gap Analysis fields
        MIN(CASE WHEN cca.day_diff >= 0 THEN cca.day_diff END) AS days_to_first_collection_attempt,
        fca.first_call_date AS first_collection_attempt_date
    FROM customer_call_analysis cca
    LEFT JOIN first_collection_attempt fca
        ON cca.fbbid = fca.fbbid
    GROUP BY cca.FBBID, cca.latest_dpd_edate, fca.first_call_date
),

with_dialer AS (
    SELECT 
        pf.*,
        dd.flag_call_next_7_days,
        dd.flag_call_days_8_to_14,
        dd.flag_any_call_after_dpd,
        dd.flag_rpc,
        dd.days_to_first_collection_attempt,
        dd.first_collection_attempt_date,
        -- NEW: Flag for dialer gap analysis
        CASE 
            WHEN dd.days_to_first_collection_attempt IS NULL 
                AND DATEDIFF('day', pf.latest_dpd_edate, pf.edate) > 7 
            THEN TRUE 
            ELSE FALSE 
        END AS flag_no_contact_over_7_days,
        CASE 
            WHEN dd.days_to_first_collection_attempt <= 3 THEN 'Early_Contact_0_3_Days'
            WHEN dd.days_to_first_collection_attempt BETWEEN 4 AND 7 THEN 'Standard_Contact_4_7_Days'
            WHEN dd.days_to_first_collection_attempt > 7 THEN 'Delayed_Contact_Over_7_Days'
            ELSE 'No_Contact_Attempted'
        END AS collection_response_tier
    FROM payment_flags pf 
    LEFT JOIN dialer_data dd
        ON pf.fbbid = dd.fbbid 
        AND pf.latest_dpd_edate = dd.latest_dpd_edate
),

-- ============================================================================
-- SECTION 7: FINANCIAL HEALTH & CAN_AFFORD_CURE ANALYSIS
-- ============================================================================

with_fii_data AS (
    SELECT 
        *,
        -- NEW: Can Afford Cure boolean
        CASE 
            WHEN FI_balance IS NULL OR pd_amount IS NULL THEN NULL
            WHEN FI_balance >= pd_amount THEN TRUE 
            ELSE FALSE 
        END AS can_afford_cure,
        -- Existing coverage categories
        CASE
            WHEN outstanding_balance_due IS NULL OR FI_balance IS NULL THEN 'Data Missing/Invalid'
            WHEN outstanding_balance_due = 0 THEN 'Outstanding is Zero'
            WHEN FI_balance >= outstanding_balance_due THEN '100% or More'
            WHEN FI_balance >= (0.75 * outstanding_balance_due) THEN '75% to < 100%'
            WHEN FI_balance >= (0.50 * outstanding_balance_due) THEN '50% to < 75%'
            WHEN FI_balance >= (0.25 * outstanding_balance_due) THEN '25% to < 50%'
            ELSE 'Less than 25%'
        END AS fii_balance_coverage_category,
        CASE
            WHEN overdue_balance IS NULL OR FI_balance IS NULL THEN 'Data Missing/Invalid'
            WHEN overdue_balance = 0 THEN 'Outstanding is Zero'
            WHEN FI_balance >= overdue_balance THEN '100% or More'
            WHEN FI_balance >= (0.75 * overdue_balance) THEN '75% to < 100%'
            WHEN FI_balance >= (0.50 * overdue_balance) THEN '50% to < 75%'
            WHEN FI_balance >= (0.25 * overdue_balance) THEN '25% to < 50%'
            ELSE 'Less than 25%'
        END AS overdue_to_fbbi_coverage_category,
        -- NEW: FI Balance to PD Amount ratio
        CASE 
            WHEN pd_amount IS NULL OR pd_amount = 0 THEN NULL
            ELSE ROUND(FI_balance / pd_amount, 2)
        END AS fi_to_pd_ratio
    FROM (
        SELECT 
            wd.*,
            dpcd.current_balance_fi_multiple_accounts AS FI_balance,
            dpcd.fi_data_update_to_time AS fi_as_of_date,
            b.last_good_debit_payment_date,
            CASE WHEN wd.on_time_payments > 0 THEN 1 ELSE 0 END AS on_time_payment_flag,
            CASE WHEN wd.pending_payments > 0 THEN 1 ELSE 0 END AS pending_payment_flag,
            kmf.termunits,
            kmf.partner AS kmf_partner,
            kmf.channel,
            kmf.tier,
            kmf.vantage4
        FROM with_dialer wd
        LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY b 
            ON wd.fbbid = b.fbbid 
            AND wd.edate = b.edate
        LEFT JOIN bi.public.daily_approved_customers_data dpcd
            ON wd.fbbid = dpcd.fbbid 
            AND wd.edate = dpcd.edate 
        LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 kmf
            ON wd.fbbid = kmf.fbbid
            AND wd.edate = kmf.edate
        QUALIFY ROW_NUMBER() OVER (PARTITION BY wd.fbbid ORDER BY wd.edate DESC) = 1
    )
),

-- ============================================================================
-- SECTION 8: CURE TRACKING
-- ============================================================================

cured AS (
    SELECT 
        a.*,
        b.edate AS cured_edate,
        b.dpd_bucket AS cured_dpd_bucket,
        DATEDIFF('day', a.latest_dpd_edate, b.edate) AS days_to_dpd_0
    FROM with_fii_data a
    LEFT JOIN (
        SELECT fbbid, edate, MAX(dpd_bucket) AS dpd_bucket
        FROM BI.FINANCE.FINANCE_METRICS_DAILY
        GROUP BY fbbid, edate
    ) b
        ON a.fbbid = b.fbbid 
        AND a.latest_dpd_edate < b.edate
        AND (b.dpd_bucket = 0 OR b.dpd_bucket IS NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.fbbid ORDER BY b.edate) = 1
),

-- ============================================================================
-- SECTION 9: LATEST DISPOSITION
-- ============================================================================

latest_disposition AS (
    SELECT 
        a.*,
        b.disposition AS latest_disposition,
        b.date_call AS latest_disposition_date
    FROM cured a
    LEFT JOIN final_dialer b 
        ON a.fbbid = b.fbbid 
        AND a.cured_edate >= b.date_call
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.fbbid ORDER BY b.date_call DESC) = 1
),

-- ============================================================================
-- SECTION 10: CUSTOMER PROFILE ENRICHMENT
-- ============================================================================

profile AS (
    SELECT 
        ld.*,
        fmd.loan_key,
        fmd.original_payment_plan_description,
        og.OG_BUCKET AS risk_grade,
        CASE 
            WHEN og.OG_BUCKET BETWEEN 1 AND 4 THEN '1. 1-4'
            WHEN og.OG_BUCKET BETWEEN 5 AND 7 THEN '2. 5-7'
            WHEN og.OG_BUCKET BETWEEN 8 AND 10 THEN '3. 8-10'
            WHEN og.OG_BUCKET BETWEEN 11 AND 12 THEN '4. 11-12'
            WHEN og.OG_BUCKET BETWEEN 13 AND 15 THEN '5. 13-15'
            ELSE 'NULL'
        END AS og_bucket_group,
        cd.first_approved_time,
        CASE 
            WHEN cd.first_approved_time::DATE < '2020-01-01' THEN '1. Pre-2020' 
            WHEN cd.first_approved_time::DATE BETWEEN '2020-01-01' AND '2021-12-31' THEN '2. 2020-2021'   
            WHEN cd.first_approved_time::DATE BETWEEN '2022-01-01' AND '2023-12-31' THEN '3. 2022-2023'
            WHEN cd.first_approved_time::DATE BETWEEN '2024-01-01' AND '2024-12-31' THEN '4. 2024'
            WHEN cd.first_approved_time::DATE BETWEEN '2025-01-01' AND '2025-12-31' THEN '5. 2025'
            ELSE NULL 
        END AS acq_cohort,
        CASE 
            WHEN DATEDIFF('day', dacd.credit_score_json:"VantageScore 4.0":"created_time"::TIMESTAMP, dacd.edate) <= 100
            THEN dacd.credit_score_json:"VantageScore 4.0":"score"::INT
            ELSE NULL
        END AS clean_vantage,
        CASE 
            WHEN clean_vantage < 600 THEN '1. <600'
            WHEN clean_vantage BETWEEN 600 AND 650 THEN '2. 600-650'
            WHEN clean_vantage BETWEEN 650 AND 700 THEN '3. 650-700'
            WHEN clean_vantage > 700 THEN '4. 700+'
            ELSE NULL 
        END AS vantage_score_bucket,
        f.partner,
        dacd.state,
        CASE 
            WHEN dacd.state IN ('CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT') THEN 'Northeast'
            WHEN dacd.state IN ('AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV') THEN 'Southeast'
            WHEN dacd.state IN ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI') THEN 'Midwest'
            WHEN dacd.state IN ('ID', 'MT', 'OR', 'WA', 'WY') THEN 'Northwest'
            WHEN dacd.state IN ('AZ', 'CO', 'NV', 'NM', 'OK', 'TX', 'UT', 'CA') THEN 'Southwest'
            WHEN dacd.state IN ('AK', 'HI', 'DC', 'PR', 'AS', 'GU', 'VI', 'FM', 'St Thomas') THEN 'Territory'
            WHEN dacd.state IN ('ON', 'QC', 'NS') THEN 'Non-U.S. Region'
            ELSE 'Unknown'
        END AS region,
        f.industry_type AS industry,
        CASE WHEN fmd.original_payment_plan_description LIKE '%Term Loan%' THEN 1 ELSE 0 END AS is_term_loan,
        CASE 
            WHEN fmd.original_payment_plan_description LIKE '%12%' THEN '12 Weeks'
            WHEN fmd.original_payment_plan_description LIKE '%24%' THEN '24 Weeks'
            WHEN fmd.original_payment_plan_description LIKE '%52%' THEN '52 Weeks' 
            ELSE 'Others' 
        END AS payment_plan
    FROM latest_disposition ld
    LEFT JOIN BI.FINANCE.FINANCE_METRICS_DAILY fmd
        ON ld.fbbid = fmd.fbbid 
        AND ld.edate = fmd.edate
    LEFT JOIN ANALYTICS.CREDIT.OG_MODEL_SCORES_RETROSCORED_V1_1 og
        ON fmd.fbbid = og.fbbid
        AND fmd.edate = og.edate
    LEFT JOIN BI.PUBLIC.CUSTOMERS_DATA cd
        ON fmd.fbbid = cd.fbbid
    LEFT JOIN bi.PUBLIC.daily_approved_customers_data dacd
        ON fmd.fbbid = dacd.fbbid 
        AND fmd.edate::DATE = dacd.edate
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 f
        ON fmd.fbbid = f.fbbid 
        AND fmd.edate::DATE = f.edate
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fmd.fbbid ORDER BY fmd.edate DESC) = 1
),

-- ============================================================================
-- SECTION 11: ACH RETURN INFORMATION
-- ============================================================================

pmt_txn AS (
    SELECT DISTINCT 
        PAYMENT_ID,
        TXN_ID
    FROM CDC_V2.PAYMENTS_HIST.PAYMENT_TRANSACTION_ASSN
    WHERE PROVIDER_TYPE = 'ACH'
),

rtn_status AS (
    SELECT 
        pmt_txn.TXN_ID,
        rtn.TRANSACTION_STATUS AS ach_return_description,
        psd.*
    FROM BI.FINANCE.PAYMENTS_STATUSES_DATA psd
    LEFT JOIN pmt_txn
        ON psd.PAYMENT_ID = pmt_txn.PAYMENT_ID
    LEFT JOIN CDC_V2.PAYMENTS.NACHA_RETURN_DESCRIPTORS rtn
        ON COALESCE(pmt_txn.TXN_ID, psd.TRANSACTION_ID) = rtn.TRANSACTION_ID
    WHERE psd.PAYMENT_STATUS = 'DELQ'
        AND PAYMENT_METHOD_TYPE = 'ACH'
),

with_transaction_info AS (
    SELECT 
        a.*,
        b.ach_return_description AS transaction_status
    FROM profile a
    LEFT JOIN rtn_status b
        ON a.fbbid = b.fbbid 
        AND DATE(a.latest_dpd_edate) = DATE(b.payment_status_change_event_time)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.fbbid ORDER BY a.edate) = 1
)

-- ============================================================================
-- FINAL OUTPUT WITH ALL PRD FIELDS
-- ============================================================================

SELECT 
    -- Core Identifiers
    fbbid,
    edate AS analysis_date,
    
    -- DPD Status
    dpd_days,
    dpd_bucket,
    previous_bucket,
    
    -- Financial Amounts
    outstanding_balance_due,
    pd_amount,
    overdue_balance,
    FI_balance,
    fi_as_of_date,
    
    -- Risk & Profile
    risk_grade,
    og_bucket_group,
    vantage_score_bucket,
    clean_vantage,
    tier,
    channel,
    partner,
    industry,
    state,
    region,
    acq_cohort,
    payment_plan,
    is_term_loan,
    
    -- Week-over-Week Classification
    cust_classification,
    roll_over_classification,
    
    -- DPD Transition Details
    latest_dpd_edate,
    first_missed_payment_date,
    days_since_latest_dpd_change,
    days_since_first_missed_payment,
    days_in_previous_dpd_state,
    
    -- NEW: State Machine / Oscillation Analysis
    total_bucket_transitions,
    dpd_1_2_oscillation_count,
    oscillation_behavior,
    bucket_movement_path,
    
    -- NEW: Payment Behavior Attribution
    payment_behavior_category,
    successful_payments_post_fmd,
    failed_payments_post_fmd,
    total_payment_attempts_post_fmd,
    total_amount_paid_post_fmd,
    on_time_payments,
    pending_payments,
    on_time_payment_flag,
    pending_payment_flag,
    last_good_debit_payment_date,
    
    -- NEW: Dialer Gap Analysis
    days_to_first_collection_attempt,
    first_collection_attempt_date,
    flag_no_contact_over_7_days,
    collection_response_tier,
    flag_call_next_7_days,
    flag_call_days_8_to_14,
    flag_any_call_after_dpd,
    flag_rpc,
    latest_disposition,
    latest_disposition_date,
    
    -- NEW: Financial Health / Can Afford Cure
    can_afford_cure,
    fi_to_pd_ratio,
    fii_balance_coverage_category,
    overdue_to_fbbi_coverage_category,
    
    -- Cure Tracking
    cured_edate,
    days_to_dpd_0,
    
    -- Transaction Info
    transaction_status
    
FROM with_transaction_info
ORDER BY fbbid;
