-- AUW DPD 1-7 Roll Rate Analysis (March 2026)
-- Purpose: Analyze AUW customers entering DPD 1-7 each week since 11/2025
-- NOTE: Roll Rate is calculated at LOAN LEVEL (using loan_key)
-- Based on existing AUW.sql logic from Manual underwriting folder

-- =============================================================================
-- SINGLE TABLE: AUW DPD 1-7 Roll Rate with All Fields
-- Following the exact logic from AUW.sql (aw_list tagging approach)
-- =============================================================================

CREATE OR REPLACE TABLE analytics.credit.auw_dpd_1_7_roll_rate_analysis AS

WITH 
-- Step 1: OG AUW customers (credit limit increase after OG review)
og_auw_updated_tag AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      review_complete_time__c::DATE AS review_complete_time,
      t2.credit_limit AS post_cl,
      t1.credit_limit AS pre_cl,
      recordtypeid
    FROM (
      SELECT *
      FROM external_data_sources.salesforce_nova.loan__c
      WHERE recordtypeid IN ('012Rd000001B2txIAC')
        AND review_complete_time__c IS NOT NULL
        AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
    ) a
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) AS t1
      ON a.fundbox_id__c = t1.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(DAY, 1, t1.edate)
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) AS t2
      ON a.fundbox_id__c = t2.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(DAY, -1, t2.edate)
  )
  SELECT fbbid, MIN(review_complete_time) AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
  GROUP BY 1
),

-- Step 2: HVC customers
HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    MIN(review_complete_time__c::DATE) AS loan_review_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000002EppVIAS'
    AND status__c IS NOT NULL
    AND status__c IN (
      'Complete - Increase',
      'Complete - Temp Increase',
      'Complete - Temp Increase + Terms',
      'Complete - Counteroffer',
      'Complete - Increase + Better Terms'
    )
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  GROUP BY 1
),
HVC_list_ns AS (
  SELECT hvc.fbbid, hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

-- Step 3: Pre-Approval customers
pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    MIN(review_complete_time__c::DATE) AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
    AND fbbid IN (
      SELECT DISTINCT fbbid
      FROM bi.public.customers_data
      WHERE first_approved_time IS NOT NULL
    )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  GROUP BY 1
),

-- Step 4: OB AUW Pre-Doc Increase customers
tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      CASE
        WHEN YEAR(auw_pre_doc_review_start_time__c::DATE) >= 2025
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        WHEN auw_pre_doc_review_start_time__c IS NULL
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        ELSE auw_pre_doc_approved_limit__c
      END AS final_auw_pre_doc_approved_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT fbbid, MIN(ob_increase_auw_date) AS ob_increase_auw_date_min
  FROM tab1
  WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit
  GROUP BY 1
),

-- Step 5: OB AUW Post-Doc Increase customers
tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT fbbid, MIN(ob_increase_auw_date) AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
  GROUP BY 1
),

-- Step 6: Second Look customers
tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date,
    b.first_approved_credit_limit,
    approved_uw_credit_limit__c,
    first_suggested_credit_limit__c,
    (approved_uw_credit_limit__c - COALESCE(b.first_approved_credit_limit, first_suggested_credit_limit__c)) AS delta_n
  FROM external_data_sources.salesforce_nova.loan__c a
  LEFT JOIN bi.public.customers_data b
    ON a.fundbox_id__c = b.fbbid
  WHERE recordtypeid = '0124T000000DSMTQA4'
    AND review_complete_time__c::DATE IS NOT NULL
    AND FUNDBOX_ID__C NOT IN (999999999999999910, 9987800100888)  
    AND FUNDBOX_ID__C IN (
      SELECT DISTINCT fbbid FROM bi.public.customers_data WHERE first_approved_time IS NOT NULL
    )
),
tab2_SL AS (
  SELECT
    a.*,
    b.automated_cl,
    b.cl_delta,
    CASE
      WHEN b.cl_delta > 0 THEN 'Increase'
      WHEN b.cl_delta < 0 THEN 'Decrease'
      WHEN b.cl_delta = 0 THEN 'No Change'
      ELSE 'NA'
    END AS tag1_chk
  FROM tab1_SL a
  LEFT JOIN analytics.credit.second_look_accounts b
    ON a.fbbid = b.fbbid
),

-- Step 7: Combine all AUW customer lists with their effective dates and tags
aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'Second Look' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

-- Step 8: Base loan data with DPD metrics
auw_base AS (
  SELECT
    loan_key, fbbid, week_end_date, week_start_date, loan_created_date, first_planned_transmission_date,
    is_charged_off, charge_off_date, outstanding_principal_due, originated_amount, dpd_days_corrected,
    new_cust_filter, bucket_group, risk_bucket, termunits, partner, intuit_flow, nav_flow,
    national_funding_flow, lendio_flow, payment_plan, industry_type, fico, vantage4,
    total_paid, fees_paid, principal_paid, is_after_co, customer_annual_revenue_group,
    os_p_1_90, os_1_90, os_p_0, os_1_7
  FROM analytics.credit.loan_level_data_pb
  WHERE sub_product <> 'mca'
    AND fbbid IN (SELECT DISTINCT fbbid FROM aw_list)
    AND week_end_date >= '2025-11-01'
),

-- Step 9: Join with customer level data for additional attributes
auw_base_join AS (
  SELECT 
    a.*,
    b.fbbid AS fbbid_aw_list,
    b.eff_dt,
    b.tag1,
    c.account_status,
    c.IS_CHARGED_OFF_FMD,
    c.termunits AS cust_termunits,
    c.partner AS pa_partner,
    c.channel,
    c.tier,
    c.sub_product,
    c.ob_bucket_group,
    c.credit_limit,
    c.first_approved_credit_limit
  FROM auw_base a
  LEFT JOIN aw_list b
    ON a.fbbid = b.fbbid
    AND a.week_end_date >= b.eff_dt
  LEFT JOIN analytics.credit.customer_level_data_td c
    ON a.fbbid = c.fbbid AND a.week_end_date = c.week_end_date
),

-- Step 10: Deduplicate (keep most recent AUW tag per loan per week)
auw_base_join_deduped AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbbid, loan_key, week_end_date ORDER BY eff_dt DESC) AS rnk
    FROM auw_base_join
  ) q
  WHERE rnk = 1
)

-- Final Output: Single table with all fields for roll rate analysis
SELECT 
    -- Time Dimensions
    week_end_date,
    week_start_date,
    
    -- Identifiers
    loan_key,
    fbbid,
    loan_created_date,
    
    -- AUW Program Tag (CRITICAL for roll rate calculation)
    tag1 AS auw_program_type,
    eff_dt AS auw_effective_date,
    
    -- DPD Metrics for Roll Rate Calculation
    os_p_0,             -- Previous week DPD 0 outstanding (DENOMINATOR)
    os_1_7,             -- Current DPD 1-7 outstanding
    os_1_90,            -- Current DPD 1-90 outstanding
    os_p_1_90,          -- Previous week DPD 1-90 outstanding
    dpd_days_corrected,
    
    -- Loan Attributes
    is_charged_off,
    outstanding_principal_due,
    originated_amount,
    bucket_group,
    risk_bucket,
    fico,
    
    -- Channel/Partner Dimensions
    channel,
    COALESCE(cust_termunits, termunits) AS termunits,
    COALESCE(pa_partner, partner) AS partner,
    
    -- Risk Dimensions
    tier AS risk_grade,
    ob_bucket_group,
    
    -- Product Dimension
    sub_product,
    
    -- Industry Dimension
    industry_type,
    
    -- Revenue Dimension
    customer_annual_revenue_group AS revenue_segment,
    
    -- Credit Limit Dimension
    credit_limit,
    first_approved_credit_limit,
    CASE 
        WHEN credit_limit < 10000 THEN '< $10K'
        WHEN credit_limit < 25000 THEN '$10K - $25K'
        WHEN credit_limit < 50000 THEN '$25K - $50K'
        WHEN credit_limit < 100000 THEN '$50K - $100K'
        WHEN credit_limit < 250000 THEN '$100K - $250K'
        ELSE '>= $250K'
    END AS credit_limit_bucket

FROM auw_base_join_deduped
WHERE tag1 IS NOT NULL
;


-- =============================================================================
-- WEEKLY ROLL RATE QUERY (Run this to get weekly roll rates)
-- =============================================================================
/*
SELECT 
    week_end_date,
    
    -- Total DPD 1-7 Roll Rate (#)
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) AS dpd_1_7_all,
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END) AS dpd_0_all,
    ROUND(COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END), 0), 4) AS roll_rate_num_pct,
    
    -- Total DPD 1-7 Roll Rate ($)
    SUM(CASE WHEN os_p_0 > 0 THEN os_1_7 ELSE 0 END) AS dpd_1_7_sum_all,
    SUM(os_p_0) AS dpd_0_sum_all,
    ROUND(SUM(CASE WHEN os_p_0 > 0 THEN os_1_7 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(os_p_0), 0), 4) AS roll_rate_os_pct,
    
    -- 8-Week Moving Average
    AVG(ROUND(SUM(CASE WHEN os_p_0 > 0 THEN os_1_7 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(os_p_0), 0), 4)) OVER (ORDER BY week_end_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS roll_rate_8wk_ma

FROM analytics.credit.auw_dpd_1_7_roll_rate_analysis
GROUP BY week_end_date
ORDER BY week_end_date;
*/


-- =============================================================================
-- ROLL RATE BY AUW PROGRAM TYPE (OB AUW, OG AUW, HVC, Pre-Approval, Second Look)
-- =============================================================================
/*
SELECT 
    week_end_date,
    auw_program_type,
    
    -- DPD 1-7 Roll Rate (#)
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) AS dpd_1_7,
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END) AS dpd_0,
    ROUND(COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END), 0), 4) AS roll_rate_num_pct,
    
    -- DPD 1-7 Roll Rate ($)
    SUM(CASE WHEN os_p_0 > 0 THEN os_1_7 ELSE 0 END) AS dpd_1_7_sum,
    SUM(os_p_0) AS dpd_0_sum,
    ROUND(SUM(CASE WHEN os_p_0 > 0 THEN os_1_7 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(os_p_0), 0), 4) AS roll_rate_os_pct

FROM analytics.credit.auw_dpd_1_7_roll_rate_analysis
GROUP BY week_end_date, auw_program_type
ORDER BY week_end_date, auw_program_type;
*/


-- =============================================================================
-- CONCENTRATION ANALYSIS BY ALL DIMENSIONS
-- =============================================================================
/*
SELECT 
    week_end_date,
    auw_program_type,
    channel,
    termunits,
    industry_type,
    credit_limit_bucket,
    revenue_segment,
    risk_grade,
    ob_bucket_group,
    
    -- DPD 1-7 Roll Rate (#)
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) AS dpd_1_7,
    COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END) AS dpd_0,
    ROUND(COUNT(DISTINCT CASE WHEN os_p_0 > 0 AND os_1_7 > 0 THEN loan_key END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN os_p_0 > 0 THEN loan_key END), 0), 4) AS roll_rate_pct

FROM analytics.credit.auw_dpd_1_7_roll_rate_analysis
WHERE week_end_date >= '2025-11-01'
GROUP BY ALL
HAVING dpd_1_7 > 0
ORDER BY dpd_1_7 DESC;
*/
