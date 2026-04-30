-- ============================================================
-- ANALYTICS.CREDIT.MUW_DQ_Analysis_2904  —  Full Refresh
-- UPDATE: risk_grade_today CTE added (Steps 9b-12 area).
--         All cap calculations now use risk_grade_today instead of risk_grade.
--         risk_grade (from cte_last_muw_grade) is still output as a reference column.
-- ============================================================

-- Select * from ANALYTICS.CREDIT.MUW_DQ_Analysis_2904

CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_DQ_Analysis_2904 AS (

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1 : Raw Salesforce loan records
-- ─────────────────────────────────────────────────────────────────────────────
WITH cte_loan_raw AS (
    SELECT 
        fundbox_id__c,
        recordtypeid,
        status__c,
        risk_level__c,
        review_complete_time__c,
        createddate,
        COALESCE(review_complete_time__c::date, auw_pre_doc_review_complete_time__c::date) AS review_complete_date_coalesce,
        CASE 
            WHEN underwriter__c = '0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN underwriter__c = '0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN underwriter__c = '005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN underwriter__c = '0054T0000010694QAA' THEN 'Neil Patel'
            WHEN underwriter__c = '005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN underwriter__c = '0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN underwriter__c = '0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN underwriter__c = '005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN underwriter__c = '005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN underwriter__c = '005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN underwriter__c = '005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN underwriter__c = '005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN underwriter__c = '005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN underwriter__c = '005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN underwriter__c = '005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            ELSE underwriter__c 
        END AS underwriter_name,
        CASE 
            WHEN approver__c = '0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN approver__c = '0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN approver__c = '005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN approver__c = '0054T0000010694QAA' THEN 'Neil Patel'
            WHEN approver__c = '005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN approver__c = '0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN approver__c = '0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN approver__c = '005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN approver__c = '005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN approver__c = '005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN approver__c = '005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN approver__c = '005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN approver__c = '005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN approver__c = '005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN approver__c = '005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            ELSE approver__c 
        END AS approver_name,
        approved_uw_credit_limit__c
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C
    WHERE recordtypeid IN (
        '012Rd000000AcjxIAC','012Rd000000jbbJIAQ','0124T000000DSMTQA4',
        '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS'
    )
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2 : Fill CL before / after decision per program
-- ─────────────────────────────────────────────────────────────────────────────
cte_loan_calculations AS (
    SELECT 
        base.*,
        CASE 
            WHEN base.recordtypeid = '012Rd000000AcjxIAC' THEN cd.first_approved_credit_limit
            WHEN base.recordtypeid = '0124T000000DSMTQA4' THEN sl.automated_cl
            WHEN base.recordtypeid = '012Rd000000jbbJIAQ' THEN l.automated_cl_pa
            WHEN base.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS') THEN t1.credit_limit
            ELSE dacd.credit_limit 
        END AS credit_limit_filled,
        CASE 
            WHEN base.recordtypeid = '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
            WHEN base.recordtypeid = '012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa
            WHEN (base.recordtypeid = '012Rd000002Dp5CIAS' AND base.status__c IN ('Close Account','RMR/Disable')) THEN 0
            WHEN base.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS') THEN t2.credit_limit
            ELSE COALESCE(base.approved_uw_credit_limit__c, 0) 
        END AS approved_uw_credit_limit
    FROM cte_loan_raw base
    LEFT JOIN bi.public.customers_data cd ON cd.fbbid = base.fundbox_id__c
    LEFT JOIN analytics.credit.second_look_accounts sl ON base.fundbox_id__c = sl.fbbid
    LEFT JOIN (
        SELECT fbbid, pre_approval_amount AS automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
        FROM bi.customers.leads_data
    ) l ON l.fbbid = base.fundbox_id__c AND base.recordtypeid = '012Rd000000jbbJIAQ'
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t1 
        ON base.fundbox_id__c = t1.fbbid
        AND base.review_complete_time__c = DATEADD(day, 1, t1.edate)
        AND base.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t2 
        ON base.fundbox_id__c = t2.fbbid
        AND base.review_complete_time__c = DATEADD(day, -1, t2.edate)
        AND base.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')
    LEFT JOIN bi.public.daily_approved_customers_data dacd 
        ON dacd.fbbid = base.fundbox_id__c AND base.createddate::date = dacd.edate
    WHERE base.review_complete_date_coalesce IS NOT NULL
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3 : Decision type + first/last row numbers
-- ─────────────────────────────────────────────────────────────────────────────
cte_loan_ranked AS (
    SELECT 
        *,
        CASE
            WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
                CASE 
                    WHEN status__c = 'Complete - Current CL'         THEN 'No change'
                    WHEN status__c = 'Reduce CL'                     THEN 'Decrease'
                    WHEN status__c IN ('Close Account','RMR/Disable') THEN 'Close/RMR/Disable'
                    ELSE status__c 
                END
            ELSE
                CASE 
                    WHEN approved_uw_credit_limit > credit_limit_filled                                   THEN 'Increase'
                    WHEN approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit != 0 THEN 'Decrease'
                    WHEN approved_uw_credit_limit = credit_limit_filled                                   THEN 'No change'
                    WHEN approved_uw_credit_limit = 0                                                     THEN 'Rejected'
                    ELSE 'Other' 
                END
        END AS decision_type,
        ROW_NUMBER() OVER (PARTITION BY fundbox_id__c ORDER BY review_complete_date_coalesce DESC) AS rn_desc,
        ROW_NUMBER() OVER (PARTITION BY fundbox_id__c ORDER BY review_complete_date_coalesce ASC)  AS rn_asc
    FROM cte_loan_calculations
),

cte_last_muw  AS (SELECT * FROM cte_loan_ranked WHERE rn_desc = 1),
cte_first_muw AS (SELECT * FROM cte_loan_ranked WHERE rn_asc  = 1),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4 : First DPD date
-- ─────────────────────────────────────────────────────────────────────────────
cte_finance_dpd AS (
    SELECT 
        FBBID,
        MIN(EDATE) AS FIRST_DPD_DATE
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE PRODUCT_TYPE <> 'Flexpay'
      AND (
            CASE 
                WHEN dpd_days IS NULL AND IS_CHARGED_OFF = 0 THEN 0
                WHEN dpd_days IS NULL AND IS_CHARGED_OFF = 1 THEN 98
                ELSE dpd_days
            END
          ) >= 1
    GROUP BY FBBID
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5 : Payment behaviour
-- ─────────────────────────────────────────────────────────────────────────────
cte_payment_behaviour AS (
    SELECT
        fbbid,
        COUNT_IF(payment_status = 'FUND' AND direction = 'D')
            AS total_successful_payments,
        COUNT_IF(payment_status = 'FUND' AND direction = 'D'
            AND payment_event_time >= DATEADD('day', -90, CURRENT_DATE))
            AS successful_payments_last_90d,
        COUNT_IF(direction = 'D'
            AND (payment_status IN ('DLMS','DELQ','CHOF')
                 OR (payment_status = 'CANC' AND ach_return_code IS NOT NULL AND ach_return_code != '')))
            AS total_failed_payments,
        COUNT_IF(direction = 'D'
            AND payment_event_time >= DATEADD('day', -90, CURRENT_DATE)
            AND (payment_status IN ('DLMS','DELQ','CHOF')
                 OR (payment_status = 'CANC' AND ach_return_code IS NOT NULL AND ach_return_code != '')))
            AS failed_payments_last_90d,
        COUNT_IF(direction = 'D' AND ach_return_code IN ('R01','R09'))
            AS nsf_payments_total,
        COUNT_IF(direction = 'D' AND ach_return_code IN ('R01','R09')
            AND payment_event_time >= DATEADD('day', -90, CURRENT_DATE))
            AS nsf_payments_last_90d,
        ROUND(
            COUNT_IF(payment_status = 'FUND' AND direction = 'D') * 100.0
            / NULLIF(
                COUNT_IF(payment_status = 'FUND' AND direction = 'D')
                + COUNT_IF(direction = 'D'
                    AND (payment_status IN ('DLMS','DELQ','CHOF')
                         OR (payment_status = 'CANC' AND ach_return_code IS NOT NULL AND ach_return_code != ''))),
              0),
        1) AS payment_success_rate_pct
    FROM bi.finance.payments_model
    WHERE direction = 'D'
      AND payment_description IN ('Scheduled Repayment','Repayment','Pay Early','Payoff')
    GROUP BY fbbid
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 6 : Current customer state from daily_approved (today)
-- ─────────────────────────────────────────────────────────────────────────────
cte_customer_state_today AS (
    SELECT
        fbbid,
        DATEDIFF('day', last_increase_time::date, CURRENT_DATE) AS days_since_clip_today,
        DATEDIFF('day', last_decrease_time::date, CURRENT_DATE) AS days_since_cld_today,
        current_balance_fi                                       AS bank_balance_current_today,
        available_balance_fi                                     AS bank_balance_available_today,
        days_since_missed_payment,
        max_dpd_days,
        max_dpd_bucket,
        is_in_breather,
        breather_count,
        credit_utilization,
        active_loans,
        consec_fund_payments_after_delq
    FROM bi.public.daily_approved_customers_data
    WHERE edate = CURRENT_DATE
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 7 : Profile as-of TODAY (Vantage, FICO, Revenue, OG bucket)
-- ─────────────────────────────────────────────────────────────────────────────
 cte_profile_today AS (
    SELECT
        fbbid,
        vantage4                      AS vantage_today,
        fico                          AS fico_today,
        customer_annual_revenue       AS revenue_today,
        OG_SCORE                      AS og_score_today,
        OG_BUCKET                     AS og_bucket_today,
        OG_BUCKET_GROUP               AS og_bucket_group_today,
        customer_annual_revenue_group AS customer_annual_revenue_group_today
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2
    WHERE edate = current_date -2
    
    --DATEADD(day, -7, NEXT_DAY(CURRENT_DATE(), 'WED'))
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 8 : Alt lender count (distinct, last 12 months)
-- ─────────────────────────────────────────────────────────────────────────────
cte_alt_lender_count AS (
    SELECT
        yt.fbbid,
        COUNT(DISTINCT lender_val.value::STRING) AS num_alt_lenders_last_12m
    FROM cdc_v2.fi_connect.yodlee_transactions yt
    JOIN data_science.yodlee_transactions_features.features f
        ON yt.id = f.transaction_primary_id
    JOIN LATERAL FLATTEN(input => f.alternative_lenders_v7) lender_val
    WHERE (f.IS_BUSINESS_ALTERNATIVE_LOAN_V1 = 1 OR f.IS_CONSUMER_ALTERNATIVE_LOAN_V1 = 1)
      AND yt.transaction_date >= DATEADD('year', -1, CURRENT_DATE)
      AND lender_val.value::STRING != ''
    GROUP BY yt.fbbid
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 9 : Monthly revenue from INDUS
--   Most recent non-null customer_annual_revenue within last 90 days.
-- ─────────────────────────────────────────────────────────────────────────────
cte_indus_revenue AS (
    SELECT
        fbbid,
        customer_annual_revenue        AS annual_revenue,
        customer_annual_revenue / 12.0 AS monthly_revenue,
        edate                          AS revenue_edate
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2
    WHERE customer_annual_revenue IS NOT NULL
      AND customer_annual_revenue > 0
      AND edate >= CURRENT_DATE - 90
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 10 : 90-day avg daily bank balance
-- ─────────────────────────────────────────────────────────────────────────────
cte_avg_balance_90d AS (
    SELECT
        fbbid,
        COUNT(DISTINCT edate)                                                            AS balance_days_sampled,
        ROUND(AVG(current_balance_fi), 2)                                                AS avg_daily_balance_90d_primary,
        ROUND(AVG(current_balance_fi_multiple_accounts), 2)                              AS avg_daily_balance_90d_all_accts,
        ROUND(AVG(COALESCE(NULLIF(current_balance_fi_multiple_accounts, 0),
                           current_balance_fi)), 2)                                      AS avg_daily_balance_90d
    FROM BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA
    WHERE edate >= CURRENT_DATE - 90
    GROUP BY fbbid
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 11 : Risk grade (A-G) + fee rate from last MUW review
--   risk_grade is kept as a reference/audit column.
--   All cap calculations use risk_grade_today (Step 11b) instead.
-- ─────────────────────────────────────────────────────────────────────────────
cte_last_muw_grade AS (
    SELECT
        fundbox_id__c::NUMBER   AS fbbid,
        underwriting_risk_grade__c AS risk_grade,
        COALESCE(
            CASE
                WHEN fee_rates_76w__c IS NOT NULL AND fee_rates_76w__c > 0 THEN fee_rates_76w__c / 100.0
                WHEN fee_rates_52w__c IS NOT NULL AND fee_rates_52w__c > 0 THEN fee_rates_52w__c / 100.0
                WHEN fee_rates_24w__c IS NOT NULL AND fee_rates_24w__c > 0 THEN fee_rates_24w__c / 100.0
                WHEN fee_rates_12w__c IS NOT NULL AND fee_rates_12w__c > 0 THEN fee_rates_12w__c / 100.0
            END,
            CASE
                WHEN TRY_PARSE_JSON(dynamic_fee_rate__c)['76'] IS NOT NULL
                    THEN TRY_PARSE_JSON(dynamic_fee_rate__c)['76']['percentage']::FLOAT / 100.0
                WHEN TRY_PARSE_JSON(dynamic_fee_rate__c)['52'] IS NOT NULL
                    THEN TRY_PARSE_JSON(dynamic_fee_rate__c)['52']['percentage']::FLOAT / 100.0
                WHEN TRY_PARSE_JSON(dynamic_fee_rate__c)['24'] IS NOT NULL
                    THEN TRY_PARSE_JSON(dynamic_fee_rate__c)['24']['percentage']::FLOAT / 100.0
                WHEN TRY_PARSE_JSON(dynamic_fee_rate__c)['12'] IS NOT NULL
                    THEN TRY_PARSE_JSON(dynamic_fee_rate__c)['12']['percentage']::FLOAT / 100.0
            END,
            CASE underwriting_risk_grade__c
                WHEN 'A' THEN 0.416 WHEN 'B' THEN 0.280 WHEN 'C' THEN 0.200
                WHEN 'D' THEN 0.155 WHEN 'E' THEN 0.127 WHEN 'F' THEN 0.127
                WHEN 'G' THEN 0.127 ELSE NULL
            END
        ) AS fee_rate,
        review_complete_time__c::DATE AS grade_review_date
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C
    WHERE recordtypeid IN (
        '012Rd000000AcjxIAC','012Rd000000jbbJIAQ','0124T000000DSMTQA4',
        '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS'
    )
      AND review_complete_time__c IS NOT NULL
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fundbox_id__c ORDER BY review_complete_time__c DESC) = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 11b : NEW — risk_grade_today
--   Step 1: Latest OG score from INDUS (most recent available).
--   Step 2: Map OG score → automated base grade (B–F; A/G are manual-only).
--       0.00–0.04 → B | 0.05–0.07 → C | 0.08–0.10 → D | 0.11–0.12 → E | 0.13+ → F
--   Step 3: Most recent risk_level__c from any MUW review in Salesforce.
--   Step 4: Apply adjustment (Level 0 → +2, I → +1, II → 0, III → -1, IV → -2, V → -3).
--           Capped B(2)–F(6); A and G are assigned only through manual procedure.
-- ─────────────────────────────────────────────────────────────────────────────
cte_og_scores_latest AS (
    SELECT fbbid, OG_SCORE
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2
    WHERE OG_SCORE IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
),

cte_base_grade AS (
    SELECT
        fbbid,
        OG_SCORE,
        CASE
            WHEN OG_SCORE <= 0.04 THEN 'B'
            WHEN OG_SCORE <= 0.07 THEN 'C'
            WHEN OG_SCORE <= 0.10 THEN 'D'
            WHEN OG_SCORE <= 0.12 THEN 'E'
            ELSE                       'F'
        END AS auto_grade
    FROM cte_og_scores_latest
),

cte_latest_risk_level AS (
    SELECT fundbox_id__c AS fbbid, risk_level__c
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C
    WHERE recordtypeid IN (
        '012Rd000000AcjxIAC','012Rd000000jbbJIAQ','0124T000000DSMTQA4',
        '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS'
    )
      AND review_complete_time__c IS NOT NULL
      AND risk_level__c IS NOT NULL
      AND risk_level__c != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fundbox_id__c ORDER BY review_complete_time__c DESC) = 1
),

cte_risk_grade_today AS (
    SELECT
        fbbid,
        CASE
            GREATEST(2, LEAST(6,
                CASE auto_grade
                    WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4
                    WHEN 'E' THEN 5 WHEN 'F' THEN 6 ELSE 4
                END
                + CASE risk_level__c
                    WHEN 'Risk Level 0'   THEN -2
                    WHEN 'Risk Level I'   THEN -1
                    WHEN 'Risk Level II'  THEN  0
                    WHEN 'Risk Level III' THEN  1
                    WHEN 'Risk Level IV'  THEN  2
                    WHEN 'Risk Level V'   THEN  3
                    ELSE 0
                  END
            ))
            WHEN 2 THEN 'B'
            WHEN 3 THEN 'C'
            WHEN 4 THEN 'D'
            WHEN 5 THEN 'E'
            WHEN 6 THEN 'F'
        END AS risk_grade_today
    FROM cte_base_grade
    LEFT JOIN cte_latest_risk_level USING (fbbid)
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 12 : Payment plan (most recent week, for s_pmt signal)
-- ─────────────────────────────────────────────────────────────────────────────
cte_payment_plan AS (
    SELECT fbbid, payment_plan
    FROM analytics.credit.loan_level_data_pb
    WHERE sub_product <> 'mca'
      AND week_end_date = DATEADD(day, -7, NEXT_DAY(CURRENT_DATE(), 'WED'))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY week_end_date DESC) = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 13 : Main join
-- ─────────────────────────────────────────────────────────────────────────────
final_data AS (
    SELECT 
        a.fbbid, 
        a.is_charged_off, 
        a.charge_off_date, 
        a.termunits       AS channel_split, 
        a.partner, 
        a.industry_type,
        f.fico            AS fico,
        f.vantage4        AS vantage,
        CASE
            WHEN f.vantage4 IS NULL THEN NULL WHEN f.vantage4 < 550 THEN NULL
            WHEN f.vantage4 < 600  THEN '550-600' WHEN f.vantage4 < 650 THEN '600-650'
            WHEN f.vantage4 < 700  THEN '650-700' WHEN f.vantage4 < 750 THEN '700-750'
            WHEN f.vantage4 < 800  THEN '750-800' WHEN f.vantage4 <= 850 THEN '800-850'
            ELSE NULL
        END AS vantage_buckets,
        f.customer_annual_revenue_group AS rev_bands,
        pt.vantage_today,
        CASE
            WHEN pt.vantage_today IS NULL THEN NULL WHEN pt.vantage_today < 550 THEN NULL
            WHEN pt.vantage_today < 600  THEN '550-600' WHEN pt.vantage_today < 650 THEN '600-650'
            WHEN pt.vantage_today < 700  THEN '650-700' WHEN pt.vantage_today < 750 THEN '700-750'
            WHEN pt.vantage_today < 800  THEN '750-800' WHEN pt.vantage_today <= 850 THEN '800-850'
            ELSE NULL
        END AS vantage_buckets_today,
        pt.fico_today,
        pt.revenue_today,
        pt.og_score_today,
        pt.og_bucket_today,
        pt.og_bucket_group_today,
        pt.customer_annual_revenue_group_today,
        SUM(a.originated_amount)         AS sum_originations,
        SUM(a.OUTSTANDING_PRINCIPAL_DUE) AS sum_outstanding_principal, 
        MAX(dpd_days_corrected)          AS max_dpd, 
        COUNT(DISTINCT loan_key)         AS num_loans,
        sum_originations / num_loans     AS avg_loan_size,
        MIN(loan_created_date)           AS first_loan_date, 
        eff_dt                           AS effective_date,
        c_first.review_complete_date_coalesce  AS first_MUW_date,  
        c_first.underwriter_name               AS first_UW,
        c_first.approver_name                  AS first_secondary_UW,
        c_first.recordtypeid                   AS first_program,
        c_first.credit_limit_filled            AS first_auto_credit_limit,
        c_first.approved_uw_credit_limit       AS first_manual_credit_limit,
        c_first.decision_type                  AS first_decision_type, 
        c_first.status__c                      AS first_decision_status,
        c_first.risk_level__c                  AS first_risk_level,
        b.review_complete_date_coalesce        AS last_MUW_date,
        b.underwriter_name                     AS last_UW,
        b.approver_name                        AS last_secondary_UW,
        b.recordtypeid                         AS last_program,
        b.credit_limit_filled                  AS last_auto_credit_limit,
        b.approved_uw_credit_limit             AS last_manual_credit_limit,
        b.decision_type                        AS last_decision_type, 
        b.status__c                            AS last_decision_status,
        b.risk_level__c                        AS last_risk_level,
        CASE WHEN eff_dt = c_first.review_complete_date_coalesce THEN 1 ELSE 0 END AS first_uw_flag,
        CASE WHEN eff_dt = b.review_complete_date_coalesce       THEN 1 ELSE 0 END AS last_uw_flag,
        f.ob_bucket_group_retro, 
        f.og_bucket_group,
        fmd.FIRST_DPD_DATE,
        n.FIRST_APPROVED_TIME::date      AS first_approved_date,
        ROUND((CURRENT_DATE::date - n.FIRST_APPROVED_TIME::date) / 7.0, 1)       AS current_customer_tenure_weeks,
        ROUND((fmd.FIRST_DPD_DATE       - n.FIRST_APPROVED_TIME::date) / 7.0, 1) AS customer_tenure_at_first_dpd_weeks,
        da.credit_limit                  AS credit_limit_before,
        da1.credit_limit                 AS credit_limit_after,
        cst.days_since_clip_today,
        cst.days_since_cld_today,
        DATEDIFF('day', da_muw.last_increase_time::date, b.review_complete_date_coalesce) AS days_since_clip_at_last_muw,
        DATEDIFF('day', da_muw.last_decrease_time::date, b.review_complete_date_coalesce) AS days_since_cld_at_last_muw,
        cst.bank_balance_current_today,
        cst.bank_balance_available_today,
        da_muw.current_balance_fi        AS bank_balance_current_at_last_muw,
        da_muw.available_balance_fi      AS bank_balance_available_at_last_muw,
        cst.days_since_missed_payment,
        cst.max_dpd_days,
        cst.max_dpd_bucket,
        cst.is_in_breather,
        cst.breather_count,
        cst.credit_utilization,
        cst.active_loans,
        cst.consec_fund_payments_after_delq
    FROM ANALYTICS.CREDIT.auw_base_join_deduped a
    LEFT JOIN cte_last_muw  b       ON a.fbbid = b.fundbox_id__c
    LEFT JOIN cte_first_muw c_first ON a.fbbid = c_first.fundbox_id__c
    LEFT JOIN cte_finance_dpd fmd   ON a.fbbid = fmd.fbbid
    LEFT JOIN bi.public.customers_data n ON n.fbbid = a.fbbid
    LEFT JOIN bi.public.daily_approved_customers_data da
        ON da.fbbid = a.fbbid AND b.review_complete_date_coalesce = DATEADD(day, 1, da.edate)
    LEFT JOIN bi.public.daily_approved_customers_data da1
        ON da1.fbbid = a.fbbid AND b.review_complete_date_coalesce = DATEADD(day, -1, da1.edate)
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 f 
        ON a.fbbid = f.fbbid AND b.review_complete_date_coalesce = f.edate
    LEFT JOIN bi.public.daily_approved_customers_data da_muw
        ON da_muw.fbbid = a.fbbid AND da_muw.edate = b.review_complete_date_coalesce
    LEFT JOIN cte_customer_state_today cst ON cst.fbbid = a.fbbid
    LEFT JOIN cte_profile_today pt         ON pt.fbbid  = a.fbbid
    LEFT JOIN ANALYTICS.CREDIT.DEBT_PAYMENTS dp ON a.fbbid = dp.fbbid
    GROUP BY ALL
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 14 : Debt payments + DSCR + alt lenders + payment behaviour
-- ─────────────────────────────────────────────────────────────────────────────
with_debt_payments AS (
    SELECT 
        a.*, 
        b.total_debt_payments_last_12_months, 
        b.repayment_bucket_last_12_months, 
        b.alt_loan_payments_last_12_months,
        b.alt_loan_payment_bucket,
        b.revenue              AS revenue_at_muw, 
        b.dscr * 100           AS dscr_percent,
        CASE 
            WHEN (b.dscr * 100) <= 5  THEN '1. 0-5'   WHEN (b.dscr * 100) <= 10 THEN '2. 5-10'
            WHEN (b.dscr * 100) <= 20 THEN '3. 10-20' WHEN (b.dscr * 100) <= 30 THEN '4. 20-30'
            WHEN (b.dscr * 100) <= 40 THEN '5. 30-40' WHEN (b.dscr * 100) <= 50 THEN '6. 40-50'
            WHEN (b.dscr * 100) > 50  THEN '7. >50'   ELSE NULL
        END AS dscr_bucket,
        COALESCE(alc.num_alt_lenders_last_12m, 0)        AS num_alt_lenders_last_12m,
        COALESCE(pb.total_successful_payments, 0)         AS total_successful_payments,
        COALESCE(pb.successful_payments_last_90d, 0)      AS successful_payments_last_90d,
        COALESCE(pb.total_failed_payments, 0)             AS total_failed_payments,
        COALESCE(pb.failed_payments_last_90d, 0)          AS failed_payments_last_90d,
        COALESCE(pb.nsf_payments_total, 0)                AS nsf_payments_total,
        COALESCE(pb.nsf_payments_last_90d, 0)             AS nsf_payments_last_90d,
        pb.payment_success_rate_pct
    FROM final_data a
    LEFT JOIN ANALYTICS.CREDIT.DEBT_PAYMENTS b  ON a.fbbid = b.fbbid
    LEFT JOIN cte_alt_lender_count alc           ON a.fbbid = alc.fbbid
    LEFT JOIN cte_payment_behaviour pb           ON a.fbbid = pb.fbbid
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 15 : Industry (NAICS today)
-- ─────────────────────────────────────────────────────────────────────────────
with_industry AS (
    SELECT a.*, b.industry_naics_code_edate, LEFT(b.industry_naics_code_edate, 4) AS industry_code
    FROM with_debt_payments a
    LEFT JOIN bi.public.daily_approved_customers_data b ON a.fbbid = b.fbbid AND b.edate = CURRENT_DATE
),

final_data_industry AS (
    SELECT a.*, d.naics_title, e.naics_title AS industry
    FROM with_industry a 
    LEFT JOIN CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING d ON a.industry_code = d.naics_code
    LEFT JOIN CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING e ON a.industry_naics_code_edate = e.naics_code
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 16 : Last payment date
-- ─────────────────────────────────────────────────────────────────────────────
with_last_payment AS (
    SELECT 
        a.*,
        DATEDIFF('day', DATE(b.payment_event_time), CURRENT_DATE) AS days_since_payment,
        DATE(b.payment_event_time) AS payment_date
    FROM final_data_industry a
    LEFT JOIN bi.finance.payments_model b 
        ON a.fbbid = b.fbbid AND b.payment_status = 'FUND' AND b.direction = 'D'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.fbbid ORDER BY b.payment_event_time DESC) = 1
)

-- ─────────────────────────────────────────────────────────────────────────────
-- FINAL SELECT
-- NOTE: All cap calculations (revenue_factor, balance_factor, target_debt_load_pct,
--       longest_term_weeks, cap_revenue, cap_balance, cap_credit_limit,
--       max_debt_capacity, available_after_3p_debt, cap_debt_load,
--       policy_sugg_cl, policy_sugg_inc, binding_cap) now use rgt.risk_grade_today
--       instead of g.risk_grade.
--       g.risk_grade is still output as a reference/audit column.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 

    -- ── IDENTITY ──────────────────────────────────────────────────────────────
    w.fbbid,
    w.is_charged_off,
    w.charge_off_date,
    w.channel_split,
    w.partner,
    w.industry_type,

    -- ── PROFILE as-of LAST MUW ────────────────────────────────────────────────
    w.fico,
    w.vantage,
    w.vantage_buckets,
    w.rev_bands,

    -- ── PROFILE as-of TODAY ───────────────────────────────────────────────────
    w.fico_today,
    w.vantage_today,
    w.vantage_buckets_today,
    w.revenue_today,
    w.og_score_today,
    w.og_bucket_today,
    w.og_bucket_group_today,
    w.customer_annual_revenue_group_today,

    -- ── ORIGINATION / BALANCE METRICS ────────────────────────────────────────
    w.sum_originations,
    w.sum_outstanding_principal,
    CASE WHEN w.max_dpd > 0        THEN w.sum_outstanding_principal ELSE 0 END AS DPD_OS,
    CASE WHEN w.is_charged_off = 1 THEN w.sum_outstanding_principal ELSE 0 END AS CO_OS,
    w.max_dpd,
    w.num_loans,
    CASE WHEN w.num_loans > 5 THEN '5+' ELSE w.num_loans::VARCHAR END          AS draw_history,
    w.avg_loan_size,
    CASE 
        WHEN w.avg_loan_size IS NULL   THEN ''
        WHEN w.avg_loan_size <= 5000   THEN '1. 0-5000'
        WHEN w.avg_loan_size <= 10000  THEN '2. 5000-10000'
        WHEN w.avg_loan_size <= 15000  THEN '3. 10000-15000'
        WHEN w.avg_loan_size <= 20000  THEN '4. 15000-20000'
        WHEN w.avg_loan_size <= 25000  THEN '5. 20000-25000'
        WHEN w.avg_loan_size <= 50000  THEN '6. 25000-50000'
        WHEN w.avg_loan_size <= 100000 THEN '7. 50000-100000'
        ELSE '8. >100000'
    END AS loan_size_bucket,
    w.first_loan_date,
    w.effective_date,

    -- ── FIRST MUW ─────────────────────────────────────────────────────────────
    w.first_MUW_date,
    w.first_UW,
    w.first_secondary_UW,
    w.first_program,
    CASE w.first_program
        WHEN '012Rd000000AcjxIAC' THEN 'OB AUW'         WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval'
        WHEN '0124T000000DSMTQA4' THEN 'SL'              WHEN '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
        WHEN '012Rd000001B2txIAC' THEN 'OG AUW'          WHEN '012Rd000002EppVIAS' THEN 'HVC'
        ELSE 'Others'
    END AS first_program_name,
    w.first_auto_credit_limit,
    w.first_manual_credit_limit,
    w.first_decision_type,
    w.first_decision_status,
    w.first_risk_level,

    -- ── LAST MUW ──────────────────────────────────────────────────────────────
    w.last_MUW_date,
    TO_CHAR(w.last_MUW_date, 'YYYY-MM') AS year_month,
    w.last_UW,
    w.last_secondary_UW,
    w.last_program,
    CASE w.last_program
        WHEN '012Rd000000AcjxIAC' THEN 'OB AUW'         WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval'
        WHEN '0124T000000DSMTQA4' THEN 'SL'              WHEN '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
        WHEN '012Rd000001B2txIAC' THEN 'OG AUW'          WHEN '012Rd000002EppVIAS' THEN 'HVC'
        ELSE 'Others'
    END AS last_program_name,
    w.last_auto_credit_limit,
    w.last_manual_credit_limit,
    w.last_decision_type,
    w.last_decision_status,
    w.last_risk_level,
    w.first_uw_flag,
    w.last_uw_flag,

    -- ── TENURE ────────────────────────────────────────────────────────────────
    w.ob_bucket_group_retro,
    w.og_bucket_group,
    w.first_dpd_date,
    w.first_approved_date,
    w.current_customer_tenure_weeks,
    CASE 
        WHEN w.current_customer_tenure_weeks <= 10  THEN '1. 0-10'
        WHEN w.current_customer_tenure_weeks <= 20  THEN '2. 10-20'
        WHEN w.current_customer_tenure_weeks <= 30  THEN '3. 20-30'
        WHEN w.current_customer_tenure_weeks <= 50  THEN '4. 30-50'
        WHEN w.current_customer_tenure_weeks <= 100 THEN '5. 50-100'
        ELSE '6. 100+'
    END AS current_customer_tenure_weeks_buckets,
    w.customer_tenure_at_first_dpd_weeks,
    w.credit_limit_before,
    w.credit_limit_after,

    -- ── DAYS SINCE CLIP / CLD ─────────────────────────────────────────────────
    w.days_since_clip_today,
    w.days_since_cld_today,
    w.days_since_clip_at_last_muw,
    w.days_since_cld_at_last_muw,

    -- ── BANK BALANCE ──────────────────────────────────────────────────────────
    w.bank_balance_current_today,
    w.bank_balance_available_today,
    w.bank_balance_current_at_last_muw,
    w.bank_balance_available_at_last_muw,

    -- ── ALT LENDERS ───────────────────────────────────────────────────────────
    w.num_alt_lenders_last_12m,

    -- ── PAYMENT BEHAVIOUR ─────────────────────────────────────────────────────
    w.total_successful_payments,
    w.successful_payments_last_90d,
    w.total_failed_payments,
    w.failed_payments_last_90d,
    w.nsf_payments_total,
    w.nsf_payments_last_90d,
    w.payment_success_rate_pct,

    -- ── RISK STATE TODAY ──────────────────────────────────────────────────────
    w.days_since_missed_payment,
    w.max_dpd_days,
    w.max_dpd_bucket,
    w.is_in_breather,
    w.breather_count,
    w.credit_utilization,
    w.active_loans,
    w.consec_fund_payments_after_delq,

    -- ── LEGACY ────────────────────────────────────────────────────────────────
    'null'                                                                       AS decision_type,
    1                                                                            AS accnt_flag,
    CASE WHEN w.max_dpd > 0 THEN 1 ELSE 0 END                                   AS dpd_flag,
    w.total_debt_payments_last_12_months,
    w.repayment_bucket_last_12_months                                            AS total_debt_payments_bucket,
    w.revenue_at_muw                                                             AS revenue,
    w.dscr_percent,
    w.dscr_bucket,
    w.alt_loan_payments_last_12_months,
    w.alt_loan_payment_bucket,
    w.industry_code,
    w.naics_title                                                                AS industry_4_digit,
    w.days_since_payment,
    w.payment_date,

    -- ══════════════════════════════════════════════════════════════════════════
    -- CLIP SCORECARD
    -- ══════════════════════════════════════════════════════════════════════════

    -- ── RISK GRADE (historical reference — from last MUW review) ──────────────
    g.risk_grade,
    g.fee_rate,
    CASE g.risk_grade
        WHEN 'A' THEN 76 WHEN 'B' THEN 52 WHEN 'C' THEN 52 WHEN 'D' THEN 24 ELSE 12
    END                                                                          AS longest_term_weeks,
    g.grade_review_date,

    -- ── RISK GRADE TODAY (current — drives all cap calculations below) ─────────
    rgt.risk_grade_today,

    -- ── REVENUE ───────────────────────────────────────────────────────────────
    r.annual_revenue,
    r.monthly_revenue,
    r.revenue_edate,

    -- ── 90-DAY AVG BANK BALANCE ───────────────────────────────────────────────
    b90.avg_daily_balance_90d,
    b90.avg_daily_balance_90d_primary,
    b90.avg_daily_balance_90d_all_accts,
    b90.balance_days_sampled,

    -- ── CURRENT CL (today-1) ──────────────────────────────────────────────────
    c.credit_limit                                                               AS curr_cl,

    -- ── HARD GATES ────────────────────────────────────────────────────────────
    CASE WHEN w.is_charged_off = 0 THEN 1 ELSE 0 END                            AS g_co,
    CASE WHEN w.max_dpd = 0        THEN 1 ELSE 0 END                            AS g_dpd,
    CASE WHEN w.first_dpd_date IS NULL
              OR DATEDIFF('day', w.first_dpd_date, CURRENT_DATE) > 90
                                   THEN 1 ELSE 0 END                            AS g_rdpd,
    CASE WHEN w.days_since_payment <= 45 OR w.days_since_payment IS NULL
                                   THEN 1 ELSE 0 END                            AS g_pmt,
    CASE WHEN w.is_charged_off = 0
          AND w.max_dpd = 0
          AND (w.first_dpd_date IS NULL OR DATEDIFF('day', w.first_dpd_date, CURRENT_DATE) > 90)
          AND (w.days_since_payment <= 45 OR w.days_since_payment IS NULL)
                                   THEN 1 ELSE 0 END                            AS gate,

    -- ── SIGNAL SCORES ─────────────────────────────────────────────────────────
    CASE WHEN w.sum_outstanding_principal / NULLIF(c.credit_limit,0) >= 0.75 THEN 2
         WHEN w.sum_outstanding_principal / NULLIF(c.credit_limit,0) >= 0.50 THEN 1
         ELSE 0 END                                                              AS s_util,
    CASE WHEN w.num_loans BETWEEN 2 AND 4 THEN 1 ELSE 0 END                     AS s_draws,
    CASE WHEN w.days_since_payment <= 14 THEN 2
         WHEN w.days_since_payment BETWEEN 15 AND 30
              THEN CASE WHEN pp.payment_plan = '12 Week' THEN 1 ELSE 0 END
         ELSE 0 END                                                              AS s_pmt,
    CASE WHEN w.dscr_percent < 5 THEN 2 WHEN w.dscr_percent < 10 THEN 1 ELSE 0 END
                                                                                 AS s_dscr,
    CASE WHEN w.vantage::NUMBER >= 700 THEN 2
         WHEN w.vantage::NUMBER >= 650 THEN 1 ELSE 0 END                        AS s_vantage,
    CASE WHEN w.last_risk_level IN ('Risk Level 0','Risk Level I','Risk Level II') THEN 2
         WHEN w.last_risk_level = 'Risk Level III' THEN 1 ELSE 0 END            AS s_risk,
    CASE WHEN w.alt_loan_payments_last_12_months = 0
              OR w.alt_loan_payments_last_12_months IS NULL THEN 1 ELSE 0 END   AS s_alt,
    CASE WHEN w.current_customer_tenure_weeks >= 52 THEN 2
         WHEN w.current_customer_tenure_weeks >= 26 THEN 1 ELSE 0 END           AS s_tenure,
    CASE WHEN w.og_bucket_group IN ('OG: 1-4','OG: 5-7')    THEN 2
         WHEN w.og_bucket_group IN ('OG: 8-10','OG: 11-12') THEN 1 ELSE 0 END  AS s_og_bucket,
    CASE WHEN CASE w.last_program WHEN '012Rd000002EppVIAS' THEN 'HVC' WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval' WHEN '012Rd000001B2txIAC' THEN 'OG AUW' ELSE 'Others' END IN ('HVC','Pre-approval','OG AUW') THEN 1 ELSE 0 END
                                                                                 AS s_channel,

    -- ── COMPOSITE SCORE (max 17) ──────────────────────────────────────────────
    (
          CASE WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.75 THEN 2 WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.50 THEN 1 ELSE 0 END
        + CASE WHEN w.num_loans BETWEEN 2 AND 4 THEN 1 ELSE 0 END
        + CASE WHEN w.days_since_payment<=14 THEN 2 WHEN w.days_since_payment BETWEEN 15 AND 30 THEN CASE WHEN pp.payment_plan='12 Week' THEN 1 ELSE 0 END ELSE 0 END
        + CASE WHEN w.dscr_percent<5 THEN 2 WHEN w.dscr_percent<10 THEN 1 ELSE 0 END
        + CASE WHEN w.vantage::NUMBER>=700 THEN 2 WHEN w.vantage::NUMBER>=650 THEN 1 ELSE 0 END
        + CASE WHEN w.last_risk_level IN ('Risk Level 0','Risk Level I','Risk Level II') THEN 2 WHEN w.last_risk_level='Risk Level III' THEN 1 ELSE 0 END
        + CASE WHEN w.alt_loan_payments_last_12_months=0 OR w.alt_loan_payments_last_12_months IS NULL THEN 1 ELSE 0 END
        + CASE WHEN w.current_customer_tenure_weeks>=52 THEN 2 WHEN w.current_customer_tenure_weeks>=26 THEN 1 ELSE 0 END
        + CASE WHEN w.og_bucket_group IN ('OG: 1-4','OG: 5-7') THEN 2 WHEN w.og_bucket_group IN ('OG: 8-10','OG: 11-12') THEN 1 ELSE 0 END
        + CASE WHEN CASE w.last_program WHEN '012Rd000002EppVIAS' THEN 'HVC' WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval' WHEN '012Rd000001B2txIAC' THEN 'OG AUW' ELSE 'Others' END IN ('HVC','Pre-approval','OG AUW') THEN 1 ELSE 0 END
    )                                                                              AS total_score,

    -- ── TIER ──────────────────────────────────────────────────────────────────
    CASE
        WHEN NOT (
            w.is_charged_off = 0 AND w.max_dpd = 0
            AND (w.first_dpd_date IS NULL OR DATEDIFF('day', w.first_dpd_date, CURRENT_DATE) > 90)
            AND (w.days_since_payment <= 45 OR w.days_since_payment IS NULL)
        ) THEN 'Not Eligible'
        WHEN (
              CASE WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.75 THEN 2 WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.50 THEN 1 ELSE 0 END
            + CASE WHEN w.num_loans BETWEEN 2 AND 4 THEN 1 ELSE 0 END
            + CASE WHEN w.days_since_payment<=14 THEN 2 WHEN w.days_since_payment BETWEEN 15 AND 30 THEN CASE WHEN pp.payment_plan='12 Week' THEN 1 ELSE 0 END ELSE 0 END
            + CASE WHEN w.dscr_percent<5 THEN 2 WHEN w.dscr_percent<10 THEN 1 ELSE 0 END
            + CASE WHEN w.vantage::NUMBER>=700 THEN 2 WHEN w.vantage::NUMBER>=650 THEN 1 ELSE 0 END
            + CASE WHEN w.last_risk_level IN ('Risk Level 0','Risk Level I','Risk Level II') THEN 2 WHEN w.last_risk_level='Risk Level III' THEN 1 ELSE 0 END
            + CASE WHEN w.alt_loan_payments_last_12_months=0 OR w.alt_loan_payments_last_12_months IS NULL THEN 1 ELSE 0 END
            + CASE WHEN w.current_customer_tenure_weeks>=52 THEN 2 WHEN w.current_customer_tenure_weeks>=26 THEN 1 ELSE 0 END
            + CASE WHEN w.og_bucket_group IN ('OG: 1-4','OG: 5-7') THEN 2 WHEN w.og_bucket_group IN ('OG: 8-10','OG: 11-12') THEN 1 ELSE 0 END
            + CASE WHEN CASE w.last_program WHEN '012Rd000002EppVIAS' THEN 'HVC' WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval' WHEN '012Rd000001B2txIAC' THEN 'OG AUW' ELSE 'Others' END IN ('HVC','Pre-approval','OG AUW') THEN 1 ELSE 0 END
        ) >= 13 THEN 'Strong'
        WHEN (
              CASE WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.75 THEN 2 WHEN w.sum_outstanding_principal/NULLIF(c.credit_limit,0)>=0.50 THEN 1 ELSE 0 END
            + CASE WHEN w.num_loans BETWEEN 2 AND 4 THEN 1 ELSE 0 END
            + CASE WHEN w.days_since_payment<=14 THEN 2 WHEN w.days_since_payment BETWEEN 15 AND 30 THEN CASE WHEN pp.payment_plan='12 Week' THEN 1 ELSE 0 END ELSE 0 END
            + CASE WHEN w.dscr_percent<5 THEN 2 WHEN w.dscr_percent<10 THEN 1 ELSE 0 END
            + CASE WHEN w.vantage::NUMBER>=700 THEN 2 WHEN w.vantage::NUMBER>=650 THEN 1 ELSE 0 END
            + CASE WHEN w.last_risk_level IN ('Risk Level 0','Risk Level I','Risk Level II') THEN 2 WHEN w.last_risk_level='Risk Level III' THEN 1 ELSE 0 END
            + CASE WHEN w.alt_loan_payments_last_12_months=0 OR w.alt_loan_payments_last_12_months IS NULL THEN 1 ELSE 0 END
            + CASE WHEN w.current_customer_tenure_weeks>=52 THEN 2 WHEN w.current_customer_tenure_weeks>=26 THEN 1 ELSE 0 END
            + CASE WHEN w.og_bucket_group IN ('OG: 1-4','OG: 5-7') THEN 2 WHEN w.og_bucket_group IN ('OG: 8-10','OG: 11-12') THEN 1 ELSE 0 END
            + CASE WHEN CASE w.last_program WHEN '012Rd000002EppVIAS' THEN 'HVC' WHEN '012Rd000000jbbJIAQ' THEN 'Pre-approval' WHEN '012Rd000001B2txIAC' THEN 'OG AUW' ELSE 'Others' END IN ('HVC','Pre-approval','OG AUW') THEN 1 ELSE 0 END
        ) >= 8 THEN 'Moderate'
        ELSE 'Weak'
    END                                                                          AS tier,

    -- ══════════════════════════════════════════════════════════════════════════
    -- FOUR-CAP CL SIZING — ALL DRIVEN BY risk_grade_today (rgt.risk_grade_today)
    -- ══════════════════════════════════════════════════════════════════════════

    -- Policy grid factors — now using risk_grade_today
    CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END
                                                                                 AS revenue_factor,
    CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0
                              WHEN 'D' THEN 4.5  WHEN 'E' THEN 3.0 ELSE 2.0 END AS balance_factor,
    CASE rgt.risk_grade_today
        WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200
        WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130
        WHEN 'G' THEN 0.125 ELSE NULL
    END                                                                          AS target_debt_load_pct,

    -- 3rd-party monthly debt proxy
    ROUND(COALESCE(w.alt_loan_payments_last_12_months, 0) / 12.0, 2)            AS third_party_monthly_debt,

    -- CAP 1 — Revenue Cap = Revenue Factor × Monthly Revenue  [uses risk_grade_today]
    ROUND(
        CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END
        * COALESCE(r.monthly_revenue, 0),
    -3)                                                                          AS cap_revenue,

    -- CAP 2 — Balance Cap = Balance Factor × Avg Daily Balance (90-day)  [uses risk_grade_today]
    ROUND(
        CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0
                                  WHEN 'D' THEN 4.5  WHEN 'E' THEN 3.0 ELSE 2.0 END
        * COALESCE(b90.avg_daily_balance_90d, 0),
    -3)                                                                          AS cap_balance,

    -- CAP 3 — Hard dollar ceiling by risk_grade_today + channel
    CASE
        WHEN rgt.risk_grade_today IN ('A','B') THEN 250000
        WHEN rgt.risk_grade_today = 'C'        THEN 150000
        WHEN rgt.risk_grade_today = 'D'        THEN CASE WHEN w.channel_split = 'Intuit' THEN NULL ELSE 80000 END
        WHEN rgt.risk_grade_today = 'E'        THEN CASE WHEN w.channel_split = 'Intuit' THEN NULL ELSE 30000 END
        WHEN rgt.risk_grade_today = 'F'        THEN 10000
        WHEN rgt.risk_grade_today = 'G'        THEN 1000
        ELSE NULL
    END                                                                          AS cap_credit_limit,

    -- CAP 4 intermediates  [both use risk_grade_today]
    ROUND(
        COALESCE(r.monthly_revenue, 0)
        * CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200
                                    WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130
                                    WHEN 'G' THEN 0.125 ELSE 0 END,
    2)                                                                           AS max_debt_capacity,

    ROUND(
        GREATEST(
            COALESCE(r.monthly_revenue, 0)
            * CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200
                                        WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130
                                        WHEN 'G' THEN 0.125 ELSE 0 END
            - COALESCE(w.alt_loan_payments_last_12_months, 0) / 12.0,
        0),
    2)                                                                           AS available_after_3p_debt,

    -- CAP 4 — Debt Load Cap  [uses risk_grade_today for both debt load % and term weeks]
    ROUND(
        GREATEST(
            COALESCE(r.monthly_revenue, 0)
            * CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200
                                        WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130
                                        WHEN 'G' THEN 0.125 ELSE 0 END
            - COALESCE(w.alt_loan_payments_last_12_months, 0) / 12.0,
        0)
        / NULLIF(
            (1 + COALESCE(g.fee_rate, 0))
            / NULLIF(CASE rgt.risk_grade_today WHEN 'A' THEN 76 WHEN 'B' THEN 52 WHEN 'C' THEN 52
                                               WHEN 'D' THEN 24 ELSE 12 END, 0)
            * 4.33,
        0),
    -3)                                                                          AS cap_debt_load,

    -- FINAL SUGGESTED CL = MIN of all four caps  [all four caps use risk_grade_today]
    LEAST(
        COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END * COALESCE(r.monthly_revenue,0),-3), 999999999),
        COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0 WHEN 'D' THEN 4.5 WHEN 'E' THEN 3.0 ELSE 2.0 END * COALESCE(b90.avg_daily_balance_90d,0),-3), 999999999),
        COALESCE(CASE WHEN rgt.risk_grade_today IN ('A','B') THEN 250000 WHEN rgt.risk_grade_today='C' THEN 150000 WHEN rgt.risk_grade_today='D' THEN 80000 WHEN rgt.risk_grade_today='E' THEN 30000 WHEN rgt.risk_grade_today='F' THEN 10000 WHEN rgt.risk_grade_today='G' THEN 1000 ELSE NULL END, 999999999),
        COALESCE(ROUND(GREATEST(COALESCE(r.monthly_revenue,0)*CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200 WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130 WHEN 'G' THEN 0.125 ELSE 0 END - COALESCE(w.alt_loan_payments_last_12_months,0)/12.0,0)/NULLIF((1+COALESCE(g.fee_rate,0))/NULLIF(CASE rgt.risk_grade_today WHEN 'A' THEN 76 WHEN 'B' THEN 52 WHEN 'C' THEN 52 WHEN 'D' THEN 24 ELSE 12 END,0)*4.33,0),-3), 999999999)
    )                                                                            AS policy_sugg_cl,

    -- Suggested increase = policy_sugg_cl − curr_cl (floored at 0)
    GREATEST(
        LEAST(
            COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END * COALESCE(r.monthly_revenue,0),-3), 999999999),
            COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0 WHEN 'D' THEN 4.5 WHEN 'E' THEN 3.0 ELSE 2.0 END * COALESCE(b90.avg_daily_balance_90d,0),-3), 999999999),
            COALESCE(CASE WHEN rgt.risk_grade_today IN ('A','B') THEN 250000 WHEN rgt.risk_grade_today='C' THEN 150000 WHEN rgt.risk_grade_today='D' THEN 80000 WHEN rgt.risk_grade_today='E' THEN 30000 WHEN rgt.risk_grade_today='F' THEN 10000 WHEN rgt.risk_grade_today='G' THEN 1000 ELSE NULL END, 999999999),
            COALESCE(ROUND(GREATEST(COALESCE(r.monthly_revenue,0)*CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200 WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130 WHEN 'G' THEN 0.125 ELSE 0 END - COALESCE(w.alt_loan_payments_last_12_months,0)/12.0,0)/NULLIF((1+COALESCE(g.fee_rate,0))/NULLIF(CASE rgt.risk_grade_today WHEN 'A' THEN 76 WHEN 'B' THEN 52 WHEN 'C' THEN 52 WHEN 'D' THEN 24 ELSE 12 END,0)*4.33,0),-3), 999999999)
        ) - COALESCE(c.credit_limit, 0),
    0)                                                                           AS policy_sugg_inc,

    -- Which cap is binding  [uses risk_grade_today]
    CASE
        LEAST(
            COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END * COALESCE(r.monthly_revenue,0),-3), 999999999),
            COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0 WHEN 'D' THEN 4.5 WHEN 'E' THEN 3.0 ELSE 2.0 END * COALESCE(b90.avg_daily_balance_90d,0),-3), 999999999),
            COALESCE(CASE WHEN rgt.risk_grade_today IN ('A','B') THEN 250000 WHEN rgt.risk_grade_today='C' THEN 150000 WHEN rgt.risk_grade_today='D' THEN 80000 WHEN rgt.risk_grade_today='E' THEN 30000 WHEN rgt.risk_grade_today='F' THEN 10000 WHEN rgt.risk_grade_today='G' THEN 1000 ELSE NULL END, 999999999),
            COALESCE(ROUND(GREATEST(COALESCE(r.monthly_revenue,0)*CASE rgt.risk_grade_today WHEN 'A' THEN 0.250 WHEN 'B' THEN 0.225 WHEN 'C' THEN 0.200 WHEN 'D' THEN 0.175 WHEN 'E' THEN 0.150 WHEN 'F' THEN 0.130 WHEN 'G' THEN 0.125 ELSE 0 END - COALESCE(w.alt_loan_payments_last_12_months,0)/12.0,0)/NULLIF((1+COALESCE(g.fee_rate,0))/NULLIF(CASE rgt.risk_grade_today WHEN 'A' THEN 76 WHEN 'B' THEN 52 WHEN 'C' THEN 52 WHEN 'D' THEN 24 ELSE 12 END,0)*4.33,0),-3), 999999999)
        )
        WHEN COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 2.0 WHEN 'B' THEN 2.0 WHEN 'C' THEN 1.5 ELSE 1.0 END * COALESCE(r.monthly_revenue,0),-3), 999999999)                                                                                                                                                                                                                                                                                                                                                                                                                                                          THEN 'Revenue Cap'
        WHEN COALESCE(ROUND(CASE rgt.risk_grade_today WHEN 'A' THEN 10.0 WHEN 'B' THEN 8.0 WHEN 'C' THEN 6.0 WHEN 'D' THEN 4.5 WHEN 'E' THEN 3.0 ELSE 2.0 END * COALESCE(b90.avg_daily_balance_90d,0),-3), 999999999)                                                                                                                                                                                                                                                                                                                                                                                                                THEN 'Balance Cap'
        WHEN COALESCE(CASE WHEN rgt.risk_grade_today IN ('A','B') THEN 250000 WHEN rgt.risk_grade_today='C' THEN 150000 WHEN rgt.risk_grade_today='D' THEN 80000 WHEN rgt.risk_grade_today='E' THEN 30000 WHEN rgt.risk_grade_today='F' THEN 10000 WHEN rgt.risk_grade_today='G' THEN 1000 ELSE NULL END, 999999999)                                                                                                                                                                                                                                                                                                                  THEN 'Credit Limit Cap'
        ELSE 'Debt Load Cap'
    END                                                                          AS binding_cap

FROM with_last_payment w

-- ── Joins for CLIP fields ─────────────────────────────────────────────────────
LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA c
    ON w.fbbid = c.fbbid AND c.edate = CURRENT_DATE - 1

LEFT JOIN cte_avg_balance_90d b90
    ON w.fbbid = b90.fbbid

LEFT JOIN cte_indus_revenue r
    ON w.fbbid = r.fbbid

LEFT JOIN cte_last_muw_grade g
    ON w.fbbid = g.fbbid

-- ── NEW join: risk_grade_today ────────────────────────────────────────────────
LEFT JOIN cte_risk_grade_today rgt
    ON w.fbbid = rgt.fbbid

LEFT JOIN cte_payment_plan pp
    ON w.fbbid = pp.fbbid

);