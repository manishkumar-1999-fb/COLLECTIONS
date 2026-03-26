-- running all auw scripts from the new tab here


-- New AUW Tab (as of 20th Jan 2026 KM Run)
-- PERFORMANCE METRICS 
CREATE OR REPLACE TABLE indus.public.final_auw_metrics AS

WITH

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
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),


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


tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
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
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT fbbid, MIN(ob_increase_auw_date) AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
  GROUP BY 1
),


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


aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC'         AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date,         'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min,    'OB AUW'       AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min,    'OB AUW'       AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date,               'Second Look'  AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time,           'OG AUW'       AS tag1 FROM og_auw_updated_tag
),


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
),


auw_base_cl as (
select *
from analytics.credit.customer_level_data_td
where fbbid in (select DISTINCT fbbid FROM aw_list)
),

auw_base_join AS (
  SELECT 
    a.*,
    b.fbbid AS fbbid_aw_list,
    b.eff_dt,
    b.tag1,
    c.account_status,
    -- c.credit_limit,
    C.IS_CHARGED_OFF_FMD,
    c.termunits as cust_termunits,
    c.partner as pa_partner
  FROM auw_base a
  LEFT JOIN aw_list b
    ON a.fbbid = b.fbbid
    AND a.week_end_date >= b.eff_dt
  LEFT JOIN analytics.credit.customer_level_data_td c
    ON a.fbbid = c.fbbid AND a.week_end_date = c.week_end_date
),

auw_join_cl as (
select a.*,
 -- a.account_status,
 --    a.credit_limit,
 --    a.IS_CHARGED_OFF_FMD
    a.termunits as cust_termunits,
    a.partner as pa_partner,
    c.tag1,
    c.eff_dt
from auw_base_cl a
LEFT JOIN aw_list c
    ON a.fbbid = c.fbbid
    AND a.week_end_date >= c.eff_dt
),

auw_base_join_deduped AS (
  SELECT *
  FROM (
    SELECT
      *,
      CASE WHEN is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL THEN 0 ELSE is_charged_off_fmd END AS is_chargeoff_fmd, 
    CASE WHEN account_status = 'active' AND (is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL) AND dpd_days_corrected < 98 THEN 1 ELSE 0 END AS open_accounts,
    CASE WHEN (DPD_DAYS_CORRECTED < 98) AND (is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL) AND OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END AS active_accounts,
      ROW_NUMBER() OVER (PARTITION BY fbbid, loan_key, week_end_date ORDER BY eff_dt DESC) AS rnk
    FROM auw_base_join
  ) q
  WHERE rnk = 1)
  
,auw_join_cl_deduped AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbbid, week_end_date ORDER BY eff_dt DESC) AS rnk
    FROM auw_join_cl
  ) q
  WHERE rnk = 1  ),


auw_base_new_cl AS (
  SELECT 
    *,
    CASE WHEN is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL THEN 0 ELSE is_charged_off_fmd END AS is_chargeoff_fmd, 
    CASE WHEN account_status = 'active' AND (is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL) AND dpd_days_corrected < 98 THEN 1 ELSE 0 END AS open_accounts,
    CASE WHEN (DPD_DAYS_CORRECTED < 98) AND (is_charged_off_fmd = 0 OR is_charged_off_fmd IS NULL) AND OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END AS active_accounts,
MAX(credit_limit) OVER (PARTITION BY fbbid, week_end_date ORDER BY eff_dt DESC) AS max_exposure
  FROM --auw_base_join_deduped
  auw_join_cl_deduped
),

fbbid_cl_unique as (
select WEEK_START_DATE,
week_end_date,
termunits,
pa_partner,
fbbid,
tag1,
max(max_exposure) as open_exposure
from auw_base_new_cl
where open_accounts = 1 and tag1 is not null
group by 1,2,3,4,5,6
),


auw_open_exposure_summary as (
select 
WEEK_START_DATE,
week_end_date,
termunits,
pa_partner
,SUM(open_exposure) as open_exposure
,SUM(CASE WHEN tag1 IN ('OB AUW', 'Second Look') THEN open_exposure END) ob_exposure
  ,SUM(CASE WHEN tag1 = 'OG AUW' THEN open_exposure END) og_exposure
  ,SUM(CASE WHEN tag1 = 'HVC' THEN open_exposure END) hvc_exposure
  ,SUM(CASE WHEN tag1 = 'Pre-Approval' THEN open_exposure END) pre_app_exposure
from fbbid_cl_unique
group by 1,2,3,4
),

auw_perf_metrics_cte AS (
  SELECT
    week_start_date,
    week_end_date,
termunits,
pa_partner,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS ob_auw_total_os,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS ob_auw_os_1_90_dpd,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS ob_auw_gross_co,

    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) AS ob_auw_gross_co_week,
    
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN fees_paid * 52 ELSE 0 END) AS ob_auw_revenue,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN fees_paid ELSE 0 END) AS ob_auw_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS ob_auw_gross_yield,
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      + SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS ob_auw_net_yield,

SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN fees_paid ELSE 0 END)
      - SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END)
      + SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid ELSE 0 END)
      AS ob_auw_net_yield_week,
      
    SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS ob_auw_originations,

    SUM(CASE WHEN tag1 = 'OG AUW' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS og_auw_total_os,
    SUM(CASE WHEN tag1 = 'OG AUW' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS og_auw_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS og_auw_gross_co,

   SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) AS og_auw_gross_co_week,
    
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END) AS og_auw_revenue,
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid ELSE 0 END) AS og_auw_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS og_auw_gross_yield,
     SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS og_auw_net_yield,

 SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid ELSE 0 END)
      AS og_auw_net_yield_week,     
      
    SUM(CASE WHEN tag1 = 'OG AUW' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS og_auw_originations,

    SUM(CASE WHEN tag1 = 'HVC' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS hvc_total_os,
    SUM(CASE WHEN tag1 = 'HVC' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS hvc_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS hvc_gross_co,

 SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) AS hvc_gross_co_week,
    
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END) AS hvc_revenue,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid ELSE 0 END) AS hvc_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS hvc_gross_yield,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS hvc_net_yield,

 SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid ELSE 0 END)
      AS hvc_net_yield_week,
      
    SUM(CASE WHEN tag1 = 'HVC' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS hvc_originations,

    SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS pre_approvals_total_os,
    SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS pre_approvals_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS pre_approvals_gross_co,

    SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) AS pre_approvals_gross_co_week,
    
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END) AS pre_approvals_revenue,
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid ELSE 0 END) AS pre_approvals_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS pre_approvals_gross_yield,
     SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS pre_approvals_net_yield,

SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END)
      + SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid ELSE 0 END)
      AS pre_approvals_net_yield_week,
 
    SUM(CASE WHEN tag1 = 'Pre-Approval' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS pre_approvals_originations,

    SUM(CASE WHEN is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS total_os_all,
    SUM(CASE WHEN is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS os_1_90_dpd_all,
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_co_all,

 SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) AS gross_co_all_week,
 
    SUM(fees_paid * 52) AS revenue_all,
    SUM(fees_paid) AS revenue_all_NOT_ANNUAL,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_yield_all,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
    + SUM(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS net_yield_all,

 SUM(fees_paid) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END)
    + SUM(CASE WHEN is_after_co = 1 THEN principal_paid ELSE 0 END)
      AS net_yield_all_week,
      
    SUM(case when loan_created_date between week_start_date and week_end_date then originated_amount else 0 end) AS originations_all

  -- Draws Cured       
    ,COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND tag1 = 'OG AUW' THEN loan_key ELSE NULL END) og_cured
    ,COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND tag1 = 'HVC' THEN loan_key ELSE NULL END) hvc_cured
    ,COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND tag1 = 'Pre-Approval' THEN loan_key ELSE NULL END) pre_app_cured
    ,COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND tag1 IN ('OB AUW','Second Look') THEN loan_key ELSE NULL END) ob_cured
    ,COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 THEN loan_key ELSE NULL END) draws_cured

  -- Net CO (Corrected Logic)
  ,(sum(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) - sum(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ) as net_co_all

  ,(sum(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) - sum(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ) as ob_net_co
  ,(sum(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) - sum(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ) as pre_app_net_co
  ,(sum(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) - sum(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ) as og_net_co
  ,(sum(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) - sum(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ) as hvc_net_co


  ,(sum(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) - sum(CASE WHEN is_after_co = 1 THEN principal_paid ELSE 0 END) ) as net_co_all_week

  ,(sum(CASE WHEN tag1 IN ('OB AUW','Second Look') AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) - sum(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid ELSE 0 END) ) as ob_net_co_week
  ,(sum(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) - sum(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid ELSE 0 END) ) as pre_app_net_co_week
  ,(sum(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) - sum(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid ELSE 0 END) ) as og_net_co_week
  ,(sum(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due ELSE 0 END) - sum(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid ELSE 0 END) ) as hvc_net_co_week


    
    
  -- Total Open Exposure
  -- ,CASE WHEN is_charged_off_fmd=0 or is_charged_off_fmd is null then 0 else is_charged_off_fmd end as is_chargeoff_fmd 
  -- ,CASE WHEN account_status='active' and is_chargeoff_fmd = 0 and dpd_days_corrected < 98 THEN 1 ELSE 0 END AS open_accounts 

  
  
  -- Number of Active Customers
  -- ,CASE WHEN (DPD_DAYS_CORRECTED < 98) AND is_chargeoff_fmd=0 AND OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END active_accounts
  ,COUNT(DISTINCT CASE WHEN tag1 IN ('OB AUW','Second Look') AND active_accounts=1 THEN fbbid END) ob_active
  ,COUNT(DISTINCT CASE WHEN tag1= 'OG AUW' AND active_accounts=1 THEN fbbid END) og_active
  ,COUNT(DISTINCT CASE WHEN tag1= 'HVC' AND active_accounts=1 THEN fbbid END) hvc_active
  ,COUNT(DISTINCT CASE WHEN tag1= 'Pre-Approval' AND active_accounts=1 THEN fbbid END) pre_app_active
  ,COUNT(DISTINCT CASE WHEN active_accounts=1 THEN fbbid END) active_all


  -- $ Recoveries
  , SUM(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) recoveries_all
  , SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) ob_recoveries
  , SUM(CASE WHEN tag1= 'OG AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) og_recoveries
  , SUM(CASE WHEN tag1= 'HVC' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) hvc_recoveries
  , SUM(CASE WHEN tag1= 'Pre-Approval' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) pre_app_recoveries

 , SUM(CASE WHEN is_after_co = 1 THEN principal_paid ELSE 0 END) recoveries_all_week
  , SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND is_after_co = 1 THEN principal_paid ELSE 0 END) ob_recoveries_week
  , SUM(CASE WHEN tag1= 'OG AUW' AND is_after_co = 1 THEN principal_paid ELSE 0 END) og_recoveries_week
  , SUM(CASE WHEN tag1= 'HVC' AND is_after_co = 1 THEN principal_paid ELSE 0 END) hvc_recoveries_week
  , SUM(CASE WHEN tag1= 'Pre-Approval' AND is_after_co = 1 THEN principal_paid ELSE 0 END) pre_app_recoveries_week


  -- Total DPD 1-7 Roll Rate (#)
  , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) dpd_1_7_all
  , COUNT(DISTINCT CASE WHEN tag1 IN ('OB AUW','Second Look') AND os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) ob_dpd_1_7
  , COUNT(DISTINCT CASE WHEN tag1 = ('OG AUW') AND os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) og_dpd_1_7
  , COUNT(DISTINCT CASE WHEN tag1 = ('HVC') AND os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) hvc_dpd_1_7
  , COUNT(DISTINCT CASE WHEN tag1 = ('Pre-Approval') AND os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) pre_app_dpd_1_7

  , COUNT(DISTINCT CASE WHEN os_p_0>0 THEN loan_key ELSE NULL END) dpd_0_all
  , COUNT(DISTINCT CASE WHEN tag1 IN ('OB AUW','Second Look') AND os_p_0>0 THEN loan_key ELSE NULL END) ob_dpd_0
  , COUNT(DISTINCT CASE WHEN tag1 = ('OG AUW') AND os_p_0>0 THEN loan_key ELSE NULL END) og_dpd_0
  , COUNT(DISTINCT CASE WHEN tag1 = ('HVC') AND os_p_0>0 THEN loan_key ELSE NULL END) hvc_dpd_0
  , COUNT(DISTINCT CASE WHEN tag1 = ('Pre-Approval') AND os_p_0>0 THEN loan_key ELSE NULL END) pre_app_dpd_0


  -- Total DPD 1-7 Roll Rate ($)
  , SUM(CASE WHEN os_p_0>0 THEN os_1_7 ELSE 0 END) dpd_1_7_sum_all
  , SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') AND os_p_0>0 THEN os_1_7 ELSE 0 END) ob_dpd_1_7_sum
  , SUM(CASE WHEN tag1 = ('OG AUW') AND os_p_0>0 THEN os_1_7 ELSE 0 END) og_dpd_1_7_sum
  , SUM(CASE WHEN tag1 = ('HVC') AND os_p_0>0 THEN os_1_7 ELSE 0 END) hvc_dpd_1_7_sum
  , SUM(CASE WHEN tag1 = ('Pre-Approval') AND os_p_0>0 THEN os_1_7 ELSE 0 END) pre_app_dpd_1_7_sum

  , SUM(os_p_0) dpd_0_sum_all
  , SUM(CASE WHEN tag1 IN ('OB AUW','Second Look') THEN os_p_0 ELSE 0 END) ob_dpd_0_sum
  , SUM(CASE WHEN tag1 = ('OG AUW') THEN os_p_0 ELSE 0 END) og_dpd_0_sum
  , SUM(CASE WHEN tag1 = ('HVC') THEN os_p_0 ELSE 0 END) hvc_dpd_0_sum
  , SUM(CASE WHEN tag1 = ('Pre-Approval') THEN os_p_0 ELSE 0 END) pre_app_dpd_0_sum


  -- # of Delinquent Draws
  , COUNT(DISTINCT CASE WHEN os_1_90>0 THEN loan_key ELSE NULL END) num_delq_all
  , COUNT(DISTINCT CASE WHEN tag1 IN ('OB AUW', 'Second Look') AND os_1_90>0 THEN loan_key ELSE NULL END) ob_num_delq
  , COUNT(DISTINCT CASE WHEN tag1 = ('OG AUW') AND os_1_90>0 THEN loan_key ELSE NULL END) og_num_delq
  , COUNT(DISTINCT CASE WHEN tag1 = ('HVC') AND os_1_90>0 THEN loan_key ELSE NULL END) hvc_num_delq
  , COUNT(DISTINCT CASE WHEN tag1 = ('Pre-Approval') AND os_1_90>0 THEN loan_key ELSE NULL END) pre_app_num_delq


  FROM auw_base_join_deduped
  WHERE tag1 IS NOT NULL
  GROUP BY 1, 2,3,4
)

select a.*,
c.open_exposure,
c.ob_exposure,
c.og_exposure,
c.hvc_exposure,
c.pre_app_exposure

from auw_open_exposure_summary c
left join  auw_perf_metrics_cte a 
on a.week_start_date= c.week_start_date and a.week_end_date = c.week_end_date
and a.termunits = c.termunits
and a.pa_partner = c.pa_partner
;






















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------

-- OPERATIONAL METRICS 
CREATE OR REPLACE TABLE indus.public.op_metrics AS

-- US National Holidays 2026 (update this list annually)
WITH us_holidays AS (
  SELECT holiday_date FROM (VALUES 
    ('2026-01-19'::DATE),  -- MLK Day
    ('2026-02-16'::DATE),  -- Presidents Day
    ('2026-05-25'::DATE),  -- Memorial Day
    ('2026-06-19'::DATE),  -- Juneteenth
    ('2026-07-03'::DATE),  -- Independence Day (observed)
    ('2026-09-07'::DATE),  -- Labor Day
    ('2026-11-26'::DATE),  -- Thanksgiving
    ('2026-11-27'::DATE),  -- Day after Thanksgiving
    ('2026-12-24'::DATE),  -- Christmas Eve
    ('2026-12-25'::DATE),  -- Christmas Day
    ('2026-12-31'::DATE)   -- New Year's Eve
  ) AS t(holiday_date)
)

  SELECT
    CASE
      WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0 AND DATEDIFF('day', review_complete_date_coalesce, current_date()) <= 0 THEN NULL
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0 THEN current_date() - 1
      ELSE DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
    END AS week_end_date,
    
    -- Original Total Metrics (unsplit)
    SUM(sla_hrs) AS sum_sla_hrs,
    COUNT(*) AS files_reviewed,
    SUM(CASE WHEN program_type = 'OG AUW' THEN sla_hrs END) AS og_sla_hrs,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_sla_hrs,
    SUM(CASE WHEN program_type = 'OG AUW' THEN 1 ELSE 0 END) AS og_count,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN 1 ELSE 0 END) AS auw_monitoring_count,
    MEDIAN(sla_hrs) AS median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'OG AUW' THEN sla_hrs END) AS og_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_median_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Reactive' THEN sla_hrs END) AS reactive_median_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Proactive' THEN sla_hrs END) AS proactive_median_sla_hrs,
    COUNT(DISTINCT CASE WHEN sla_hrs <= 24 THEN fundbox_id__c END) AS perc_files,
    COUNT(DISTINCT CASE WHEN program_type = 'OG AUW' AND sla_hrs <= 24 THEN fundbox_id__c END) AS og_perc_files,
    COUNT(DISTINCT CASE WHEN program_type = 'AUW Monitoring' AND sla_hrs <= 24 THEN fundbox_id__c END) AS auw_monitoring_perc_files,

    -- OB Metrics
    SUM(CASE WHEN program_type IN ('OB AUW','SL') THEN sla_hrs END) AS ob_sla_hrs,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') THEN 1 ELSE 0 END) AS ob_count,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') THEN sla_hrs END) AS ob_median_sla_hrs,
    COUNT(DISTINCT CASE WHEN program_type IN ('OB AUW','SL') AND sla_hrs <= 24 THEN fundbox_id__c END) AS ob_perc_files,

    -- Pre-Approval Metrics
    SUM(CASE WHEN program_type = 'Pre-Approvals' THEN sla_hrs END) AS pre_app_sla_hrs,
    SUM(CASE WHEN program_type = 'Pre-Approvals' THEN 1 ELSE 0 END) AS pre_app_count,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' THEN sla_hrs END) AS pre_app_median_sla_hrs,
    COUNT(DISTINCT CASE WHEN program_type = 'Pre-Approvals' AND sla_hrs <= 24 THEN fundbox_id__c END) AS pre_app_perc_files,

    -- OB Metrics (Split)
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND reason_for_muw = 'lemur high risk' THEN sla_hrs END) AS ob_sla_hrs_lemur,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN sla_hrs END) AS ob_sla_hrs_core,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND reason_for_muw = 'lemur high risk' THEN 1 ELSE 0 END) AS ob_lemur_count,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN 1 ELSE 0 END) AS ob_core_count,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') AND reason_for_muw = 'lemur high risk' THEN sla_hrs END) AS ob_median_sla_hrs_lemur,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN sla_hrs END) AS ob_median_sla_hrs_core,
    COUNT(DISTINCT CASE WHEN program_type IN ('OB AUW','SL') AND sla_hrs <= 24 AND reason_for_muw = 'lemur high risk' THEN fundbox_id__c END) AS ob_perc_files_lemur,
    COUNT(DISTINCT CASE WHEN program_type IN ('OB AUW','SL') AND sla_hrs <= 24 AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN fundbox_id__c END) AS ob_perc_files_core,

    -- Pre-Approval Metrics (Split) 
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND reason_for_muw = 'lemur high risk' THEN sla_hrs END) AS pre_app_sla_hrs_lemur,
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN sla_hrs END) AS pre_app_sla_hrs_core,
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND reason_for_muw = 'lemur high risk' THEN 1 ELSE 0 END) AS pre_app_lemur_count,
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN 1 ELSE 0 END) AS pre_app_core_count,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' AND reason_for_muw = 'lemur high risk' THEN sla_hrs END) AS pre_app_median_sla_hrs_lemur,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN sla_hrs END) AS pre_app_median_sla_hrs_core,
    COUNT(DISTINCT CASE WHEN program_type = 'Pre-Approvals' AND sla_hrs <= 24 AND reason_for_muw = 'lemur high risk' THEN fundbox_id__c END) AS pre_app_perc_files_lemur,
    COUNT(DISTINCT CASE WHEN program_type = 'Pre-Approvals' AND sla_hrs <= 24 AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN fundbox_id__c END) AS pre_app_perc_files_core,

    SUM(CASE WHEN program_group = 'Reactive' THEN sla_hrs END) AS reactive_sla_hrs,
    SUM(CASE WHEN program_group = 'Proactive' THEN sla_hrs END) AS proactive_sla_hrs,
    SUM(CASE WHEN program_group = 'Reactive' THEN 1 ELSE 0 END) AS reactive_count,
    SUM(CASE WHEN program_group = 'Proactive' THEN 1 ELSE 0 END) AS proactive_count,

    -- Marketplace SLA Metrics (TOTAL SUM)
    SUM(CASE WHEN LOWER(marketplace) = 'lendio' THEN sla_hrs END) AS lendio_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'nav' THEN sla_hrs END) AS nav_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bluevine' THEN sla_hrs END) AS bluevine_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'sofi' THEN sla_hrs END) AS sofi_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'fundwell' THEN sla_hrs END) AS fundwell_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'snapcap' THEN sla_hrs END) AS snapcap_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = '1west' THEN sla_hrs END) AS onewest_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bigthink' THEN sla_hrs END) AS bigthink_sla_hrs,

    -- Marketplace SLA Metrics for Pre-Approvals (SUM)
    SUM(CASE WHEN LOWER(marketplace) = 'lendio' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_lendio_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'nav' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_nav_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bluevine' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_bluevine_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'sofi' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_sofi_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'fundwell' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_fundwell_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'snapcap' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_snapcap_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = '1west' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_onewest_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bigthink' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_bigthink_sla_hrs,
    
    -- Marketplace SLA Metrics (MEDIAN)
    MEDIAN(CASE WHEN LOWER(marketplace) = 'lendio' THEN sla_hrs END) AS lendio_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'nav' THEN sla_hrs END) AS nav_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bluevine' THEN sla_hrs END) AS bluevine_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'sofi' THEN sla_hrs END) AS sofi_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'fundwell' THEN sla_hrs END) AS fundwell_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'snapcap' THEN sla_hrs END) AS snapcap_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = '1west' THEN sla_hrs END) AS onewest_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bigthink' THEN sla_hrs END) AS bigthink_median_sla_hrs,

    -- Marketplace Pre-App SLA Metrics (MEDIAN)
    MEDIAN(CASE WHEN LOWER(marketplace) = 'lendio' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_lendio_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'nav' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_nav_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bluevine' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_bluevine_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'sofi' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_sofi_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'fundwell' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_fundwell_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'snapcap' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_snapcap_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = '1west' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_onewest_median_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bigthink' AND program_type = 'Pre-Approvals' THEN sla_hrs END) AS PreAPP_bigthink_median_sla_hrs,

    -- Marketplace Counts (TOTAL)
    COUNT(CASE WHEN LOWER(marketplace) = 'lendio' THEN 1 END) AS lendio_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'nav' THEN 1 END) AS nav_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'bluevine' THEN 1 END) AS bluevine_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'sofi' THEN 1 END) AS sofi_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'fundwell' THEN 1 END) AS fundwell_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'snapcap' THEN 1 END) AS snapcap_count,
    COUNT(CASE WHEN LOWER(marketplace) = '1west' THEN 1 END) AS onewest_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'bigthink' THEN 1 END) AS bigthink_count,

    -- Marketplace Pre-App Counts
    COUNT(CASE WHEN LOWER(marketplace) = 'lendio' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_lendio_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'nav' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_nav_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'bluevine' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_bluevine_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'sofi' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_sofi_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'fundwell' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_fundwell_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'snapcap' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_snapcap_count,
    COUNT(CASE WHEN LOWER(marketplace) = '1west' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_onewest_count,
    COUNT(CASE WHEN LOWER(marketplace) = 'bigthink' AND program_type = 'Pre-Approvals' THEN 1 END) AS PreAPP_bigthink_count,

    ----- Business SLA HRS (Using Logic from bigthink) -----
    SUM(business_sla_hrs) AS sum_business_sla_hrs,
    SUM(CASE WHEN program_type = 'OG AUW' THEN business_sla_hrs END) AS og_business_sla_hrs,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN business_sla_hrs END) AS auw_monitoring_business_sla_hrs,
    MEDIAN(business_sla_hrs) AS median_business_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'OG AUW' THEN business_sla_hrs END) AS og_median_business_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'AUW Monitoring' THEN business_sla_hrs END) AS auw_monitoring_median_business_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Reactive' THEN business_sla_hrs END) AS reactive_median_business_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Proactive' THEN business_sla_hrs END) AS proactive_median_business_sla_hrs,
    
    -- Keeping these commented out as requested per bigthink script
    -- COUNT(DISTINCT CASE WHEN business_sla_hrs <= 24 THEN fundbox_id__c END) AS perc_files,
    -- COUNT(DISTINCT CASE WHEN program_type = 'OG AUW' AND business_sla_hrs <= 24 THEN fundbox_id__c END) AS og_perc_files,
    -- COUNT(DISTINCT CASE WHEN program_type = 'AUW Monitoring' AND business_sla_hrs <= 24 THEN fundbox_id__c END) AS auw_monitoring_perc_files,

    SUM(CASE WHEN program_type IN ('OB AUW','SL') THEN business_sla_hrs END) AS ob_business_sla_hrs,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') THEN business_sla_hrs END) AS ob_median_business_sla_hrs,

    SUM(CASE WHEN program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS pre_app_business_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS pre_app_median_business_sla_hrs,

    -- OB Metrics Split (Business)
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND reason_for_muw = 'lemur high risk' THEN business_sla_hrs END) AS ob_business_sla_hrs_lemur,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN business_sla_hrs END) AS ob_business_sla_hrs_core,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') AND reason_for_muw = 'lemur high risk' THEN business_sla_hrs END) AS ob_median_business_sla_hrs_lemur,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN business_sla_hrs END) AS ob_median_business_sla_hrs_core,

    -- Pre-Approval Metrics Split (Business)
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND reason_for_muw = 'lemur high risk' THEN business_sla_hrs END) AS pre_app_business_sla_hrs_lemur,
    SUM(CASE WHEN program_type = 'Pre-Approvals' AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN business_sla_hrs END) AS pre_app_business_sla_hrs_core,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' AND reason_for_muw = 'lemur high risk' THEN business_sla_hrs END) AS pre_app_median_business_sla_hrs_lemur,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' AND (reason_for_muw <> 'lemur high risk' OR reason_for_muw IS NULL) THEN business_sla_hrs END) AS pre_app_median_business_sla_hrs_core,

    SUM(CASE WHEN program_group = 'Reactive' THEN business_sla_hrs END) AS reactive_business_sla_hrs,
    SUM(CASE WHEN program_group = 'Proactive' THEN business_sla_hrs END) AS proactive_business_sla_hrs,
  
    -- Marketplace Business SLA Metrics (TOTAL SUM)
    SUM(CASE WHEN LOWER(marketplace) = 'lendio' THEN business_sla_hrs END) AS lendio_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'nav' THEN business_sla_hrs END) AS nav_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bluevine' THEN business_sla_hrs END) AS bluevine_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'sofi' THEN business_sla_hrs END) AS sofi_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'fundwell' THEN business_sla_hrs END) AS fundwell_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'snapcap' THEN business_sla_hrs END) AS snapcap_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = '1west' THEN business_sla_hrs END) AS onewest_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bigthink' THEN business_sla_hrs END) AS bigthink_business_sla_hrs,

    -- Marketplace Business SLA for Pre-Approval (SUM)
    SUM(CASE WHEN LOWER(marketplace) = 'lendio' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreApp_lendio_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'nav' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreApp_nav_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bluevine' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS preAPP_bluevine_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'sofi' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_sofi_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'fundwell' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_fundwell_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'snapcap' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_snapcap_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = '1west' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_onewest_business_sla_hrs,
    SUM(CASE WHEN LOWER(marketplace) = 'bigthink' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_bigthink_business_sla_hrs,

    -- Marketplace Business SLA Metrics (TOTAL MEDIAN)
    MEDIAN(CASE WHEN LOWER(marketplace) = 'lendio' THEN business_sla_hrs END) AS lendio_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'nav' THEN business_sla_hrs END) AS nav_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bluevine' THEN business_sla_hrs END) AS bluevine_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'sofi' THEN business_sla_hrs END) AS sofi_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'fundwell' THEN business_sla_hrs END) AS fundwell_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'snapcap' THEN business_sla_hrs END) AS snapcap_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = '1west' THEN business_sla_hrs END) AS onewest_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bigthink' THEN business_sla_hrs END) AS bigthink_median_business_sla_hrs,

    -- Marketplace Pre-App Business SLA Metrics (MEDIAN)
    MEDIAN(CASE WHEN LOWER(marketplace) = 'lendio' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_lendio_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'nav' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_nav_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bluevine' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_bluevine_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'sofi' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_sofi_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'fundwell' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_fundwell_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = 'snapcap' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_snapcap_median_business_sla_hrs,
    MEDIAN(CASE WHEN LOWER(marketplace) = '1west' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_onewest_median_business_sla_hrs,  
    MEDIAN(CASE WHEN LOWER(marketplace) = 'bigthink' AND program_type = 'Pre-Approvals' THEN business_sla_hrs END) AS PreAPP_bigthink_median_business_sla_hrs

  FROM (
    -- INNER SUBQUERY
    SELECT
      fundbox_id__c,
      recordtypeid,
      status__c,
      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC' THEN 'OB AUW'
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN 'SL'
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN 'Pre-Approvals'
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS') THEN 'OG AUW'
        WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
        ELSE 'Other'
      END AS program_type,
      CASE
        WHEN recordtypeid IN ('012Rd000000AcjxIAC', '0124T000000DSMTQA4', '012Rd000000jbbJIAQ') THEN 'Reactive'
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN 'Proactive'
        ELSE 'Other'
      END AS program_group,
      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC' THEN b.first_approved_credit_limit
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.automated_cl
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.automated_cl_pa
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t1.credit_limit
        ELSE c.credit_limit
      END AS credit_limit_filled,
      CASE
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa
        WHEN (recordtypeid = '012Rd000002Dp5CIAS' AND status__c IN ('Close Account', 'RMR/Disable')) THEN 0
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t2.credit_limit
        ELSE COALESCE(A.approved_uw_credit_limit__c, 0)
      END AS approved_uw_credit_limit,
      CASE
        WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
          CASE
            WHEN status__c = 'Complete - Current CL' THEN 'No change'
            WHEN status__c = 'Reduce CL' THEN 'Decrease'
            WHEN status__c IN ('Close Account', 'RMR/Disable') THEN 'Close/RMR/Disable'
            ELSE status__c
          END
        ELSE
          CASE
            WHEN approved_uw_credit_limit > credit_limit_filled THEN 'Increase'
            WHEN approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit <> 0 THEN 'Decrease'
            WHEN approved_uw_credit_limit = credit_limit_filled THEN 'No change'
            WHEN approved_uw_credit_limit = 0 THEN 'Rejected'
            ELSE 'Other'
          END
      END AS decision_type,
      review_start_time__c::DATE AS review_start_date,
      review_complete_time__c::DATE AS review_complete_date,
      auw_pre_doc_review_complete_time__c::DATE AS auw_pre_doc_review_complete_date,
      COALESCE(review_complete_time__c, auw_pre_doc_review_complete_time__c) AS review_complete_time_coalesce,
      COALESCE (ready_for_uw_start_time__c , createddate) AS review_start_time_coalesce,
      COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce,
      TIMESTAMPDIFF(MINUTE, review_start_time_coalesce, review_complete_time_coalesce) AS sla_minutes,
      sla_minutes / 60 AS sla_hrs,

      -- Business Hours SLA Calculation (9am-6pm EST, Mon-Fri, excluding US holidays from CTE)
      CONVERT_TIMEZONE('America/New_York', review_start_time_coalesce) AS start_est,
      CONVERT_TIMEZONE('America/New_York', review_complete_time_coalesce) AS end_est,

      CASE 
          WHEN start_est IS NULL OR end_est IS NULL THEN NULL
          -- RULE 1: Start after hours/weekend/holiday AND end before 9AM/weekend/holiday AND no business days in between → 10 min
          WHEN (DAYOFWEEK(start_est::DATE) IN (0,6) OR start_est::DATE IN (SELECT holiday_date FROM us_holidays) OR EXTRACT(HOUR FROM start_est) >= 18 OR EXTRACT(HOUR FROM start_est) < 9) 
               AND (DAYOFWEEK(end_est::DATE) IN (0,6) OR end_est::DATE IN (SELECT holiday_date FROM us_holidays) OR EXTRACT(HOUR FROM end_est) < 9) 
               AND (GREATEST(0, (DATEDIFF('day', start_est::DATE, end_est::DATE) - 1 - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2) 
               + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END 
               + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
               - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END)))) = 0 
          THEN 10.0 / 60.0
          -- RULE 1b: Start after hours/weekend/holiday AND end before 9AM/weekend/holiday BUT has business days in between
          WHEN (DAYOFWEEK(start_est::DATE) IN (0,6) OR start_est::DATE IN (SELECT holiday_date FROM us_holidays) OR EXTRACT(HOUR FROM start_est) >= 18 OR EXTRACT(HOUR FROM start_est) < 9) 
               AND (DAYOFWEEK(end_est::DATE) IN (0,6) OR end_est::DATE IN (SELECT holiday_date FROM us_holidays) OR EXTRACT(HOUR FROM end_est) < 9) 
          THEN (GREATEST(0, (DATEDIFF('day', start_est::DATE, end_est::DATE) - 1 - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2) 
               + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END 
               + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
               - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END)))) * 9.0
          -- RULE 2: Same day, not weekend/holiday, both within 9AM-6PM → simple subtraction
          WHEN start_est::DATE = end_est::DATE AND DAYOFWEEK(start_est::DATE) NOT IN (0,6) AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays) AND EXTRACT(HOUR FROM start_est) >= 9 AND EXTRACT(HOUR FROM start_est) < 18 AND EXTRACT(HOUR FROM end_est) >= 9 AND EXTRACT(HOUR FROM end_est) <= 18
          THEN (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0) - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0)
          -- RULE 3: Same day, not weekend/holiday, start within hours, end after 6PM → cap end at 6PM
          WHEN start_est::DATE = end_est::DATE AND DAYOFWEEK(start_est::DATE) NOT IN (0,6) AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays) AND EXTRACT(HOUR FROM start_est) >= 9 AND EXTRACT(HOUR FROM start_est) < 18 AND EXTRACT(HOUR FROM end_est) >= 18
          THEN 18 - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0)
          -- RULE 4: Same day, not weekend/holiday, start before 9AM, end within hours → start at 9AM
          WHEN start_est::DATE = end_est::DATE AND DAYOFWEEK(start_est::DATE) NOT IN (0,6) AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays) AND EXTRACT(HOUR FROM start_est) < 9 AND EXTRACT(HOUR FROM end_est) >= 9 AND EXTRACT(HOUR FROM end_est) <= 18
          THEN (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0) - 9
          -- RULE 5: Same day, not weekend/holiday, spans entire business day (before 9AM to after 6PM) → 9 hours
          WHEN start_est::DATE = end_est::DATE AND DAYOFWEEK(start_est::DATE) NOT IN (0,6) AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays) AND EXTRACT(HOUR FROM start_est) < 9 AND EXTRACT(HOUR FROM end_est) >= 18
          THEN 9.0
          -- MULTI-DAY CASES
          ELSE (
              -- Hours from START day (0 if weekend or holiday)
              CASE WHEN DAYOFWEEK(start_est::DATE) IN (0,6) OR start_est::DATE IN (SELECT holiday_date FROM us_holidays) THEN 0 WHEN EXTRACT(HOUR FROM start_est) >= 18 THEN 0 WHEN EXTRACT(HOUR FROM start_est) < 9 THEN 9.0 ELSE 18.0 - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0) END
              -- Full business days in between × 9 hours (minus weekends and holidays)
              + GREATEST(0, (DATEDIFF('day', start_est::DATE, end_est::DATE) - 1 - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2) 
               + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END 
               + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
               - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END))) * 9.0
              -- Hours from END day (0 if weekend or holiday)
              + CASE WHEN DAYOFWEEK(end_est::DATE) IN (0,6) OR end_est::DATE IN (SELECT holiday_date FROM us_holidays) THEN 0 WHEN EXTRACT(HOUR FROM end_est) < 9 THEN 0 WHEN EXTRACT(HOUR FROM end_est) >= 18 THEN 9.0 ELSE (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0) - 9.0 END
          )
      END AS business_sla_hrs,

      CASE 
        WHEN SL.automated_cl >= 70000 THEN 'high line'
        WHEN SL.automated_cl >= 15000 AND LEFT(B.zip_code, 2) IN ('91', '92', '93') THEN 'zip'
        WHEN SL.automated_cl >= 15000 AND LEFT(B.industry_naics_code, 2) = '48' THEN 'naics'
        WHEN lrl.risk_level IN (5) AND SL.automated_cl BETWEEN 30000 AND 49999.99 THEN 'lemur high risk'
        WHEN lrl.risk_level IN (4, 5) AND SL.automated_cl BETWEEN 50000 AND 69999.99 THEN 'lemur high risk'
      END AS reason_for_muw,

      CASE WHEN cld.termunits IS NOT NULL THEN cld.termunits ELSE A.partner_name__c END AS marketplace
      
    FROM external_data_sources.salesforce_nova.loan__c A
    LEFT JOIN bi.public.customers_data b ON b.fbbid = a.fundbox_id__c
    LEFT JOIN bi.public.daily_approved_customers_data c ON c.fbbid = a.fundbox_id__c AND a.createddate::DATE = c.edate
    LEFT JOIN analytics.credit.second_look_accounts sl ON a.fundbox_id__c = sl.fbbid
    LEFT JOIN (SELECT fbbid, partner_name, calculated_annual_revenue AS calculated_annual_revenue_pa,
                      pre_approval_amount AS automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
              FROM bi.customers.leads_data
    ) l ON l.fbbid = a.fundbox_id__c AND a.recordtypeid = '012Rd000000jbbJIAQ'
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t1
      ON a.fundbox_id__c = t1.fbbid AND a.review_complete_time__c::DATE = DATEADD(day, 1, t1.edate) AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t2
      ON a.fundbox_id__c = t2.fbbid AND a.review_complete_time__c::DATE = DATEADD(day, -1, t2.edate) AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')
    LEFT JOIN (SELECT DISTINCT fbbid, termunits FROM analytics.credit.customer_level_data_td) cld
      ON a.fundbox_id__c = cld.fbbid
    LEFT JOIN (
      SELECT * FROM cdc_v2.risk.llm_risk_level
      QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
    ) lrl
      ON A.FUNDBOX_ID__C = lrl.fbbid
      
    WHERE (review_complete_time__c IS NOT NULL OR auw_pre_doc_review_complete_time__c IS NOT NULL)
      AND recordtypeid IN ('012Rd000000AcjxIAC','0124T000000DSMTQA4','012Rd000000jbbJIAQ',
                           '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  ) c
  GROUP BY 1;
















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


--- AUW Termunits script
CREATE OR REPLACE TABLE INDUS.PUBLIC.AUW_OP_METRICS_RG AS

-- US National Holidays 2026 (update this list annually)
WITH us_holidays AS (
  SELECT holiday_date FROM (VALUES 
    ('2026-01-19'::DATE),  -- MLK Day
    ('2026-02-16'::DATE),  -- Presidents Day
    ('2026-05-25'::DATE),  -- Memorial Day
    ('2026-06-19'::DATE),  -- Juneteenth
    ('2026-07-03'::DATE),  -- Independence Day (observed)
    ('2026-09-07'::DATE),  -- Labor Day
    ('2026-11-26'::DATE),  -- Thanksgiving
    ('2026-11-27'::DATE),  -- Day after Thanksgiving
    ('2026-12-24'::DATE),  -- Christmas Eve
    ('2026-12-25'::DATE),  -- Christmas Day
    ('2026-12-31'::DATE)   -- New Year's Eve
  ) AS t(holiday_date)
),

auw_op_metrics_cte AS (
  SELECT
 
    CASE
      WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0 AND DATEDIFF('day', review_complete_date_coalesce, current_date()) <= 0 THEN NULL
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0 THEN current_date() - 1
      ELSE DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
    END AS week_end_date,
     -- ADDED
  op_termunits,
    SUM(sla_hrs) AS sum_sla_hrs,
    sum(business_sla_hrs) as sum_business_sla_hrs,
    COUNT(*) AS files_reviewed,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') THEN sla_hrs END) AS ob_sla_hrs,
    SUM(CASE WHEN program_type = 'OG AUW' THEN sla_hrs END) AS og_sla_hrs,
    SUM(CASE WHEN program_type = 'Pre-Approvals' THEN sla_hrs END) AS pre_app_sla_hrs,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_sla_hrs,
    SUM(CASE WHEN program_type IN ('OB AUW','SL') THEN 1 ELSE 0 END) AS ob_count,
    SUM(CASE WHEN program_type = 'OG AUW' THEN 1 ELSE 0 END) AS og_count,
    SUM(CASE WHEN program_type = 'Pre-Approvals' THEN 1 ELSE 0 END) AS pre_app_count,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN 1 ELSE 0 END) AS auw_monitoring_count,
    MEDIAN(sla_hrs) AS median_sla_hrs,
    median(business_sla_hrs) as median_business_sla_hrs,
    MEDIAN(CASE WHEN program_type IN ('OB AUW','SL') THEN sla_hrs END) AS ob_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'OG AUW' THEN sla_hrs END) AS og_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals' THEN sla_hrs END) AS pre_app_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_median_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Reactive' THEN sla_hrs END) AS reactive_median_sla_hrs,
    MEDIAN(CASE WHEN program_group = 'Proactive' THEN sla_hrs END) AS proactive_median_sla_hrs,

    COUNT(DISTINCT CASE WHEN sla_hrs <= 24 THEN fundbox_id__c END) AS perc_files,
    COUNT(DISTINCT CASE WHEN program_type IN ('OB AUW','SL') AND sla_hrs <= 24 THEN fundbox_id__c END) AS ob_perc_files,
    COUNT(DISTINCT CASE WHEN program_type = 'OG AUW' AND sla_hrs <= 24 THEN fundbox_id__c END) AS og_perc_files,
    COUNT(DISTINCT CASE WHEN program_type = 'Pre-Approvals' AND sla_hrs <= 24 THEN fundbox_id__c END) AS pre_app_perc_files,
    COUNT(DISTINCT CASE WHEN program_type = 'AUW Monitoring' AND sla_hrs <= 24 THEN fundbox_id__c END) AS auw_monitoring_perc_files
  FROM (
    SELECT
      fundbox_id__c,
      recordtypeid,
      status__c,
      CASE WHEN cld.termunits IS NOT NULL THEN cld.termunits ELSE a.partner_name__c END as op_termunits,
      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC' THEN 'OB AUW'
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN 'SL'
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN 'Pre-Approvals'
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS') THEN 'OG AUW'
        WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
        ELSE 'Other'
      END AS program_type,
      CASE
        WHEN recordtypeid IN ('012Rd000000AcjxIAC', '0124T000000DSMTQA4', '012Rd000000jbbJIAQ') THEN 'Reactive'
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN 'Proactive'
        ELSE 'Other'
      END AS program_group,
      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC' THEN b.first_approved_credit_limit
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.automated_cl
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.automated_cl_pa
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t1.credit_limit
        ELSE c.credit_limit
      END AS credit_limit_filled,
      CASE
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa
        WHEN (recordtypeid = '012Rd000002Dp5CIAS' AND status__c IN ('Close Account', 'RMR/Disable')) THEN 0
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t2.credit_limit
        ELSE COALESCE(A.approved_uw_credit_limit__c, 0)
      END AS approved_uw_credit_limit,
      CASE
        WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
          CASE
            WHEN status__c = 'Complete - Current CL' THEN 'No change'
            WHEN status__c = 'Reduce CL' THEN 'Decrease'
            WHEN status__c IN ('Close Account', 'RMR/Disable') THEN 'Close/RMR/Disable'
            ELSE status__c
          END
        ELSE
          CASE
            WHEN approved_uw_credit_limit > credit_limit_filled THEN 'Increase'
            WHEN approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit <> 0 THEN 'Decrease'
            WHEN approved_uw_credit_limit = credit_limit_filled THEN 'No change'
            WHEN approved_uw_credit_limit = 0 THEN 'Rejected'
            ELSE 'Other'
          END
      END AS decision_type,
      review_start_time__c::DATE AS review_start_date,
      review_complete_time__c::DATE AS review_complete_date,
      auw_pre_doc_review_complete_time__c::DATE AS auw_pre_doc_review_complete_date,
      COALESCE(review_complete_time__c, auw_pre_doc_review_complete_time__c) AS review_complete_time_coalesce,
      COALESCE(review_start_time__c, auw_pre_doc_review_start_time__c, full_underwriting_start_time__c) AS review_start_time_coalesce,
      COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce,
      TIMESTAMPDIFF(MINUTE, review_start_time_coalesce, review_complete_time_coalesce) AS sla_minutes,
      sla_minutes / 60 AS sla_hrs,
      
     -- Business Hours SLA Calculation (9am-6pm EST, Mon-Fri only, excluding US holidays from CTE)
      CONVERT_TIMEZONE('America/New_York', review_start_time_coalesce) AS start_est,
      CONVERT_TIMEZONE('America/New_York', review_complete_time_coalesce) AS end_est,

      CASE 
          WHEN start_est IS NULL OR end_est IS NULL THEN NULL

          -- RULE 1: Start after hours/weekend/holiday AND end before 9AM/weekend/holiday AND no business days in between → 10 min
          WHEN (
              DAYOFWEEK(start_est::DATE) IN (0,6) 
              OR start_est::DATE IN (SELECT holiday_date FROM us_holidays)
              OR EXTRACT(HOUR FROM start_est) >= 18 
              OR EXTRACT(HOUR FROM start_est) < 9
          ) AND (
              DAYOFWEEK(end_est::DATE) IN (0,6) 
              OR end_est::DATE IN (SELECT holiday_date FROM us_holidays)
              OR EXTRACT(HOUR FROM end_est) < 9
          ) AND (
              -- Check no full business days in between (excluding weekends and holidays)
              GREATEST(0, (
                  DATEDIFF('day', start_est::DATE, end_est::DATE) - 1
                  - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2)
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
                  - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END)
              )) = 0
          ) THEN 10.0 / 60.0

          -- RULE 1b: Start after hours/weekend/holiday AND end before 9AM/weekend/holiday BUT has business days in between
          WHEN (
              DAYOFWEEK(start_est::DATE) IN (0,6) 
              OR start_est::DATE IN (SELECT holiday_date FROM us_holidays)
              OR EXTRACT(HOUR FROM start_est) >= 18 
              OR EXTRACT(HOUR FROM start_est) < 9
          ) AND (
              DAYOFWEEK(end_est::DATE) IN (0,6) 
              OR end_est::DATE IN (SELECT holiday_date FROM us_holidays)
              OR EXTRACT(HOUR FROM end_est) < 9
          ) THEN (
              -- Only count the full business days in between × 9 hours (excluding holidays)
              GREATEST(0, (
                  DATEDIFF('day', start_est::DATE, end_est::DATE) - 1
                  - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2)
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
                  - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END)
              )) * 9.0
          )

          -- RULE 2: Same day, not weekend/holiday, both within 9AM-6PM → simple subtraction
          WHEN start_est::DATE = end_est::DATE 
               AND DAYOFWEEK(start_est::DATE) NOT IN (0,6)
               AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays)
               AND EXTRACT(HOUR FROM start_est) >= 9 AND EXTRACT(HOUR FROM start_est) < 18
               AND EXTRACT(HOUR FROM end_est) >= 9 AND EXTRACT(HOUR FROM end_est) <= 18
          THEN (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0)
               - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0)

          -- RULE 3: Same day, not weekend/holiday, start within hours, end after 6PM → cap end at 6PM
          WHEN start_est::DATE = end_est::DATE 
               AND DAYOFWEEK(start_est::DATE) NOT IN (0,6)
               AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays)
               AND EXTRACT(HOUR FROM start_est) >= 9 AND EXTRACT(HOUR FROM start_est) < 18
               AND EXTRACT(HOUR FROM end_est) >= 18
          THEN 18 - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0)

          -- RULE 4: Same day, not weekend/holiday, start before 9AM, end within hours → start at 9AM
          WHEN start_est::DATE = end_est::DATE 
               AND DAYOFWEEK(start_est::DATE) NOT IN (0,6)
               AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays)
               AND EXTRACT(HOUR FROM start_est) < 9
               AND EXTRACT(HOUR FROM end_est) >= 9 AND EXTRACT(HOUR FROM end_est) <= 18
          THEN (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0) - 9

          -- RULE 5: Same day, not weekend/holiday, spans entire business day (before 9AM to after 6PM) → 9 hours
          WHEN start_est::DATE = end_est::DATE 
               AND DAYOFWEEK(start_est::DATE) NOT IN (0,6)
               AND start_est::DATE NOT IN (SELECT holiday_date FROM us_holidays)
               AND EXTRACT(HOUR FROM start_est) < 9
               AND EXTRACT(HOUR FROM end_est) >= 18
          THEN 9.0

          -- MULTI-DAY CASES
          ELSE (
              -- Hours from START day
              CASE 
                  -- Start is weekend or holiday → no hours on start day
                  WHEN DAYOFWEEK(start_est::DATE) IN (0,6) OR start_est::DATE IN (SELECT holiday_date FROM us_holidays) THEN 0
                  -- Start after 6PM → no hours on start day (next day starts at 9AM)
                  WHEN EXTRACT(HOUR FROM start_est) >= 18 THEN 0
                  -- Start before 9AM → full day from 9AM-6PM
                  WHEN EXTRACT(HOUR FROM start_est) < 9 THEN 9.0
                  -- Start within business hours → hours from start to 6PM
                  ELSE 18.0 - (EXTRACT(HOUR FROM start_est) + EXTRACT(MINUTE FROM start_est)/60.0)
              END
              +
              -- Full business days in between × 9 hours (excluding weekends and holidays)
              GREATEST(0, (
                  DATEDIFF('day', start_est::DATE, end_est::DATE) - 1
                  - (DATEDIFF('week', start_est::DATE, end_est::DATE) * 2)
                  -- Adjust: if start is weekend, don't double-subtract
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 0 THEN 1 ELSE 0 END  -- Sunday
                  + CASE WHEN DAYOFWEEK(start_est::DATE) = 6 THEN 1 ELSE 0 END  -- Saturday
                  -- Adjust: if end is weekend, don't double-subtract
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 0 THEN 1 ELSE 0 END
                  + CASE WHEN DAYOFWEEK(end_est::DATE) = 6 THEN 1 ELSE 0 END
                  -- Subtract holidays that fall between start and end dates
                  - (CASE WHEN '2026-01-19' > start_est::DATE AND '2026-01-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-02-16' > start_est::DATE AND '2026-02-16' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-05-25' > start_est::DATE AND '2026-05-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-06-19' > start_est::DATE AND '2026-06-19' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-07-03' > start_est::DATE AND '2026-07-03' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-09-07' > start_est::DATE AND '2026-09-07' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-26' > start_est::DATE AND '2026-11-26' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-11-27' > start_est::DATE AND '2026-11-27' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-24' > start_est::DATE AND '2026-12-24' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-25' > start_est::DATE AND '2026-12-25' < end_est::DATE THEN 1 ELSE 0 END + CASE WHEN '2026-12-31' > start_est::DATE AND '2026-12-31' < end_est::DATE THEN 1 ELSE 0 END)
              )) * 9.0
              +
              -- Hours from END day
              CASE 
                  -- End is weekend or holiday → no hours on end day
                  WHEN DAYOFWEEK(end_est::DATE) IN (0,6) OR end_est::DATE IN (SELECT holiday_date FROM us_holidays) THEN 0
                  -- End before 9AM → no hours on end day
                  WHEN EXTRACT(HOUR FROM end_est) < 9 THEN 0
                  -- End after 6PM → full day 9AM-6PM
                  WHEN EXTRACT(HOUR FROM end_est) >= 18 THEN 9.0
                  -- End within business hours → hours from 9AM to end
                  ELSE (EXTRACT(HOUR FROM end_est) + EXTRACT(MINUTE FROM end_est)/60.0) - 9.0
              END
          )
      END AS business_sla_hrs


      
    FROM external_data_sources.salesforce_nova.loan__c A
    LEFT JOIN bi.public.customers_data b ON b.fbbid = a.fundbox_id__c
    LEFT JOIN bi.public.daily_approved_customers_data c ON c.fbbid = a.fundbox_id__c AND a.createddate::DATE = c.edate
    LEFT JOIN analytics.credit.second_look_accounts sl ON a.fundbox_id__c = sl.fbbid
    LEFT JOIN (SELECT fbbid, partner_name, calculated_annual_revenue AS calculated_annual_revenue_pa,
                      pre_approval_amount AS automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
               FROM bi.customers.leads_data
    ) l ON l.fbbid = a.fundbox_id__c AND a.recordtypeid = '012Rd000000jbbJIAQ'
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t1
      ON a.fundbox_id__c = t1.fbbid AND a.review_complete_time__c::DATE = DATEADD(day, 1, t1.edate) AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')
    LEFT JOIN (SELECT fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t2
      ON a.fundbox_id__c = t2.fbbid AND a.review_complete_time__c::DATE = DATEADD(day, -1, t2.edate) AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')

     LEFT JOIN (select distinct fbbid, termunits from analytics.credit.customer_level_data_td) cld
    ON a.fundbox_id__c = cld.fbbid 
      
    WHERE (review_complete_time__c IS NOT NULL OR auw_pre_doc_review_complete_time__c IS NOT NULL)
      AND recordtypeid IN ('012Rd000000AcjxIAC','0124T000000DSMTQA4','012Rd000000jbbJIAQ',
                           '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
      AND fundbox_id__c <> 9987800100888 
  ) c
  GROUP BY 1,2
)

SELECT
b.week_end_date,
  CASE
    WHEN b.op_termunits = 'Direct' then 'Direct'
    WHEN b.op_termunits = 'Intuit' then 'Intuit'
    WHEN lower(b.op_termunits) IN ('lendio','nav','bluevine','1west','sofi','marketplaces') then 'Marketplaces'
    WHEN b.op_termunits = 'Platforms' then 'Platforms'
    WHEN b.op_termunits = 'Terminated Brokers and Partners' then 'Terminated Brokers and Partners'
    -- WHEN b.op_termunits = 'Other Partners' then 'Other Partners'
    ELSE 'Other Partners'
    END AS sla_termunits,
  b.sum_sla_hrs, 
  b.sum_business_sla_hrs,
  b.files_reviewed,
  -- b.ob_sla_hrs, b.og_sla_hrs, 
  -- b.pre_app_sla_hrs, b.auw_monitoring_sla_hrs,
  -- b.ob_count, b.og_count, 
  -- b.pre_app_count, b.auw_monitoring_count,

  b.median_sla_hrs,
  b.median_business_sla_hrs,
  -- b.ob_median_sla_hrs, b.og_median_sla_hrs, b.pre_app_median_sla_hrs, b.auw_monitoring_median_sla_hrs,
  b.perc_files, 
  -- b.ob_perc_files, b.og_perc_files, b.pre_app_perc_files, b.auw_monitoring_perc_files,
 

FROM auw_op_metrics_cte b
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY B.week_end_date;





















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


-------  UNDERWRITTEN SCRIPT

-- SELECT * FROM indus.public.auw_metrics_uw where tag1 = 'HVC' order by week_end_date desc;



CREATE OR REPLACE TABLE indus.public.auw_metrics_uw AS

WITH
og_auw_updated_tag AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      review_complete_time__c::DATE AS review_complete_time,
      t2.credit_limit AS post_cl, -- exp added, og cl
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
  SELECT fbbid, 
    review_complete_time AS auw_og_inc_time
  FROM tab1
  -- WHERE post_cl > pre_cl -- removed so we don't only take increase cases
),


HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000002EppVIAS'
    AND status__c IS NOT NULL
    -- AND status__c IN (...) -- removed
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),
HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),


pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   -- WHERE first_approved_time IS NOT NULL       -- removed
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),


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
      -- AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%' -- removed
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT fbbid, 
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  -- WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit  -- removed
),


tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
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
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      -- AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%' -- removed
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT fbbid, 
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  -- WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit -- removed
),


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
      SELECT DISTINCT fbbid FROM bi.public.customers_data 
      -- WHERE first_approved_time IS NOT NULL  --removed
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


aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),


risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*,
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN '0'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN '1'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN '2'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN '3'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN '4'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN '5'
            ELSE NULL 
        END AS RISK_LEVEL,
        
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 0 
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 1
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 2
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 3
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 4
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 5
            ELSE NULL 
        END AS RISK_LEVEL_NUM,
        
        b.review_complete_date_coalesce,
        b.review_start_date_coalesce
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),


all_fbbids AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
),

lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1
),

core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS core_lemur_tag
  FROM all_fbbids a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
),


approval_count as (
select 
    DATE_TRUNC('week', eff_dt::date + 4)::date + 2 AS week_end_date,
    a.*, 
    b.core_lemur_tag
from auw_with_ranked_risk a
LEFT JOIN core_lemur_tagging b
    ON a.fbbid = b.fbbid
)

select 
    a.week_end_date, 
    a.tag1,
    a.risk_level,
    -- b.termunits,
    case when tag1 = 'Pre-Approval' then c.partner_name__c else b.termunits end as termunits,  -- check whether all from marketplaces

    a.risk_level_num,
    COALESCE(a.core_lemur_tag, 'Core') AS lemur_tag, 
    count(distinct a.fbbid) as underwritten,
    count(CASE WHEN a.risk_level IS NOT NULL THEN a.fbbid END) as uw_with_risk,
    SUM(a.risk_level_num) AS risk_level_sum,
    
    count(distinct case when tag1 = 'OB AUW' then a.fbbid end) as ob_underwritten,
    count(CASE WHEN tag1 = 'OB AUW' and a.risk_level IS NOT NULL THEN a.fbbid END) as ob_uw_with_risk,
    SUM(CASE WHEN tag1 = 'OB AUW' then a.risk_level_num end) AS ob_risk_level_sum
    
from approval_count a
left join analytics.credit.customer_level_data_td b
    on a.fbbid = b.fbbid
    and a.week_end_date = b.week_end_date

left join (select distinct fundbox_id__c, partner_name__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ') c
on a.fbbid = c.fundbox_id__c


group by 1, 2, 3, 4, 5, 6
order by 1 desc;













------------ ----------------- ------------------- ------------------ ------------- -------------- ------------

--------- APPROVAL SCRIPT

CREATE OR REPLACE TABLE indus.public.auw_metrics_approved AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time,
    (post_cl - pre_cl) AS exposure_added,
    post_cl -- Ensure this is available if needed later
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date,
    APPROVED_UW_CHANGE_AMOUNT__C AS exposure_added
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date,
    hvc.exposure_added
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  AND status__c <> 'Rejected'
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    final_auw_pre_doc_approved_limit -- ADDED HERE
  FROM tab1
  WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit
),

tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
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
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    auw_post_doc_approved_limit__c -- ADDED HERE
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

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

 aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1, exposure_added, NULL AS ob_post_cl FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1, NULL AS exposure_added, NULL AS ob_post_cl FROM pre_approval_ns
  
  -- MAPPING PRE-DOC
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added, final_auw_pre_doc_approved_limit AS ob_post_cl FROM tab1_pre_doc_inc
  
  -- MAPPING POST-DOC
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added, auw_post_doc_approved_limit__c AS ob_post_cl FROM tab1_post_doc_inc
  
  -- MAPPING SL (SECOND LOOK) - Here is the part for SL
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1, NULL AS exposure_added, approved_uw_credit_limit__c AS ob_post_cl FROM tab2_SL WHERE tag1_chk <> 'NA'
  
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1, exposure_added, NULL AS ob_post_cl FROM og_auw_updated_tag
 ),

 risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
 ),

 auw_with_ranked_risk AS (
    SELECT 
        a.*, 
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN '0'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN '1'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN '2'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN '3'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN '4'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN '5'
            ELSE NULL 
        END AS RISK_LEVEL,
        
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 0 
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 1
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 2
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 3
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 4
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 5
            ELSE NULL 
        END AS RISK_LEVEL_NUM,
        b.review_complete_date_coalesce,
        b.review_start_date_coalesce
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
 ),


 all_fbbids_in_play AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
 ),

 lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1 
),

 core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS lemur_tag
  FROM all_fbbids_in_play a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
 ),

  approval_count as (
 select 
    DATE_TRUNC('week', a.eff_dt::date + 4)::date + 2 AS week_end_date,
    a.fbbid,
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    b.lemur_tag,
    SUM(a.exposure_added) AS sum_exposure_added,
    SUM(a.ob_post_cl) AS sum_ob_post_cl  -- ADDED AGGREGATION
 from auw_with_ranked_risk a
 LEFT JOIN core_lemur_tagging b
    ON a.fbbid = b.fbbid
 WHERE a.eff_dt IS NOT NULL
 GROUP BY 1, 2, 3, 4, 5,6
 )

 SELECT 
    a.week_end_date, 
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    a.lemur_tag,
    case when tag1 = 'Pre-Approval' then c.partner_name__c else b.termunits end as termunits,
    
    count(a.fbbid) as approvals,
    sum(a.sum_exposure_added) as exp_added,
    sum(b.fico_onboarding) as sum_fico,
    sum(b.VANTAGE) as sum_vantage,
    sum(case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end) as sum_cl,
    sum(b.customer_annual_revenue) as annual_rev,

    count(case when b.is_ftu = 1 AND b.is_ftd28 = 1 then a.fbbid end) as ftd28,
    count(CASE WHEN b.fico_onboarding IS NOT NULL THEN a.fbbid END) as approvals_with_fico,
    count(CASE WHEN b.vantage IS NOT NULL THEN a.fbbid END) as approvals_with_vantage,

    count(CASE WHEN (case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end) IS NOT NULL THEN a.fbbid END) as approvals_with_cl,

    count(case when tag1 = 'OB AUW' then a.fbbid end) as ob_approvals,
    
    -- UPDATED FINAL SELECTION
    sum(case when tag1 = 'OB AUW' then a.sum_ob_post_cl end) as ob_total_exposure,
    
    sum(case when tag1 = 'OB AUW' then b.fico_onboarding end) as ob_sum_fico,
    sum(case when tag1 = 'OB AUW' then b.credit_limit end) as ob_sum_cl,
    sum(case when tag1 = 'OB AUW' then b.customer_annual_revenue end) as ob_annual_rev,
    count(case when tag1 = 'OB AUW' and b.is_ftu = 1 AND b.is_ftd28 = 1 then a.fbbid end) as ob_ftd28,
    count(CASE WHEN tag1 = 'OB AUW' and b.fico_onboarding IS NOT NULL THEN a.fbbid END) as ob_approvals_with_fico,
    count(CASE WHEN tag1 = 'OB AUW' and b.credit_limit IS NOT NULL THEN a.fbbid END) as ob_approvals_with_cl,

    sum(case when tag1 = 'OB AUW' then b.vantage end) as ob_sum_vantage,
    count(CASE WHEN tag1 = 'OB AUW' and b.vantage IS NOT NULL THEN a.fbbid END) as ob_approvals_with_vantage
    
 from approval_count a
 left join analytics.credit.customer_level_data_td b
    on a.fbbid = b.fbbid
    and a.week_end_date = b.week_end_date

 left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
 where recordtypeid = '012Rd000000jbbJIAQ' and status__c <> 'Rejected' and pre_approved_credit_limit__c is not null) c
 on a.fbbid = c.fundbox_id__c

 group by 1, 2, 3, 4, 5, 6
 order by 1 desc;


















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


-------- APPROVAL RATE SCRIPT

CREATE OR REPLACE TABLE indus.public.auw_metrics_app_rate AS

SELECT
    uw.week_end_date,
    uw.tag1,
    uw.risk_level,
    uw.risk_level_num,
    uw.termunits,
    uw.lemur_tag AS lemur_tag, 

    app.approvals, 
    case when uw.tag1 = 'OB AUW' then app.approvals end as ob_approvals,
    -- app.risk_level_num,
    uw.underwritten,
        case when uw.tag1 = 'OB AUW' then uw.underwritten end as ob_underwritten

FROM
    indus.public.auw_metrics_uw AS uw 
LEFT JOIN
    indus.public.auw_metrics_approved AS app 
    

    ON uw.week_end_date = app.week_end_date
    AND uw.tag1 = app.tag1
    AND uw.lemur_tag = app.lemur_tag 
    
    AND uw.risk_level IS NOT DISTINCT FROM app.risk_level
    AND uw.termunits IS NOT DISTINCT FROM app.termunits;



















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


    -------- 28 DAY FTD METRIC SCRIPT

    CREATE OR REPLACE TABLE indus.public.auw_metrics_app_reg AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time,
    (post_cl - pre_cl) AS exposure_added
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date,
    APPROVED_UW_CHANGE_AMOUNT__C AS exposure_added
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date,
    hvc.exposure_added
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  -- AND status__c <> 'Rejected'
    AND fbbid IN (
      SELECT DISTINCT fbbid
      FROM bi.public.customers_data
      WHERE first_approved_time IS NOT NULL
    )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit
),

tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
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
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1, exposure_added FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1, NULL AS exposure_added FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1, NULL AS exposure_added FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1, exposure_added FROM og_auw_updated_tag
),

risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*, 
        -- CASE 
        --     WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 'Zero'
        --     WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 'One'
        --     WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 'Two'
        --     WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 'Three'
        --     WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 'Four'
        --     WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 'Five'
        --     ELSE NULL 
        -- END AS RISK_LEVEL,
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN '0'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN '1'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN '2'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN '3'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN '4'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN '5'
            ELSE NULL 
        END AS RISK_LEVEL,
        
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 0 
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 1
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 2
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 3
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 4
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 5
            ELSE NULL 
        END AS RISK_LEVEL_NUM,
        b.review_complete_date_coalesce,
        b.review_start_date_coalesce
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),


all_fbbids_in_play AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
),

lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  -- LEFT JOIN bi.public.customers_data b ON a.fbbid = b.fbbid  -- removed because don't need the zips/naics flags which are pulled
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1 
),

core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS lemur_tag
  FROM all_fbbids_in_play a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
),

approval_count as (
select 
    DATE_TRUNC('week', a.eff_dt::date + 4)::date + 2 AS week_end_date,
    a.fbbid,
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    b.lemur_tag,
    SUM(a.exposure_added) AS sum_exposure_added 
from auw_with_ranked_risk a
LEFT JOIN core_lemur_tagging b
    ON a.fbbid = b.fbbid
WHERE a.eff_dt IS NOT NULL
GROUP BY 1, 2, 3, 4, 5,6
)

select 
    a.week_end_date, 
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    a.lemur_tag,
    case when tag1 = 'Pre-Approval' then c.partner_name__c else b.termunits end as termunits, 
    
    count(a.fbbid) as approvals,

    count(case when b.is_ftu = 1 AND b.is_ftd28 = 1 then a.fbbid end) as ftd28

    
from approval_count a
left join analytics.credit.customer_level_data_td b
    on a.fbbid = b.fbbid
    and a.week_end_date = b.week_end_date

left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c

-- where tag1 = 'Pre-Approval'
group by 1, 2, 3, 4, 5, 6
order by 1 desc;



















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


-------------   14 DAY REGISTRATION RATE METRIC SCRIPT


CREATE OR REPLACE TABLE indus.public.auw_metrics_reg_rate AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time,
    (post_cl - pre_cl) AS exposure_added
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date,
    APPROVED_UW_CHANGE_AMOUNT__C AS exposure_added
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date,
    hvc.exposure_added
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    -- Kept as DATE for Joins
    review_complete_time__c::DATE AS pre_approval_date,
    -- Added as TIMESTAMP for Calculation
    review_complete_time__c AS pre_approval_timestamp 
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  -- AND status__c <> 'Rejected'
   AND fbbid IN (
      SELECT DISTINCT fbbid
      FROM bi.public.customers_data
      WHERE first_approved_time IS NOT NULL
    )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit
),

tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
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
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1, exposure_added, NULL::TIMESTAMP AS approval_time FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1, NULL AS exposure_added, pre_approval_timestamp AS approval_time FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added, NULL::TIMESTAMP AS approval_time FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1, NULL AS exposure_added, NULL::TIMESTAMP AS approval_time FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1, NULL AS exposure_added, NULL::TIMESTAMP AS approval_time FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1, exposure_added, NULL::TIMESTAMP AS approval_time FROM og_auw_updated_tag
),

risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*, 
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN '0'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN '1'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN '2'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN '3'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN '4'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN '5'
            ELSE NULL 
        END AS RISK_LEVEL,
        
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 0 
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 1
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 2
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 3
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 4
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 5
            ELSE NULL 
        END AS RISK_LEVEL_NUM,
        b.review_complete_date_coalesce,
        b.review_start_date_coalesce
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),

all_fbbids_in_play AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
),

lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1 
),

core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS lemur_tag
  FROM all_fbbids_in_play a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
),

approval_count as (
select 
    DATE_TRUNC('week', a.eff_dt::date + 4)::date + 2 AS week_end_date,
    a.approval_time, 
    a.fbbid,
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    b.lemur_tag,
    SUM(a.exposure_added) AS sum_exposure_added 
from auw_with_ranked_risk a
LEFT JOIN core_lemur_tagging b
    ON a.fbbid = b.fbbid
WHERE a.eff_dt IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7
)

select 
    a.week_end_date, 
    a.tag1,
    a.risk_level,
    a.risk_level_num,
    a.lemur_tag,
    case when tag1 = 'Pre-Approval' then c.partner_name__c else b.termunits end as termunits,
    
    count(a.fbbid) as approvals,
    
    count(CASE 
        WHEN a.tag1 = 'Pre-Approval' 
        AND b.registration_time IS NOT NULL 
        AND DATEDIFF(day, a.approval_time, b.registration_time) < 15 
        THEN a.fbbid 
    END) as reg_14_pa,

    sum(a.sum_exposure_added) as exp_added,
    sum(b.fico_onboarding) as sum_fico,
    sum(case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end) as sum_cl,
    sum(b.customer_annual_revenue) as annual_rev,
    count(case when b.is_ftu = 1 AND b.is_ftd28 = 1 then a.fbbid end) as ftd28,
    count(CASE WHEN b.fico_onboarding IS NOT NULL THEN a.fbbid END) as approvals_with_fico,
    count(CASE WHEN (case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end) IS NOT NULL THEN a.fbbid END) as approvals_with_cl,

    count(case when tag1 = 'OB AUW' then a.fbbid end) as ob_approvals,
    sum(case when tag1 = 'OB AUW' then a.sum_exposure_added end) as ob_exp_added,
    sum(case when tag1 = 'OB AUW' then b.fico_onboarding end) as ob_sum_fico,
    sum(case when tag1 = 'OB AUW' then b.credit_limit end) as ob_sum_cl,
    sum(case when tag1 = 'OB AUW' then b.customer_annual_revenue end) as ob_annual_rev,
    count(case when tag1 = 'OB AUW' and b.is_ftu = 1 AND b.is_ftd28 = 1 then a.fbbid end) as ob_ftd28,
    count(CASE WHEN tag1 = 'OB AUW' and b.fico_onboarding IS NOT NULL THEN a.fbbid END) as ob_approvals_with_fico,
    count(CASE WHEN tag1 = 'OB AUW' and b.credit_limit IS NOT NULL THEN a.fbbid END) as ob_approvals_with_cl,
    -- Added Jan 15th
    sum(b.vantage) as sum_vantage,
    count(CASE WHEN b.vantage IS NOT NULL THEN a.fbbid END) as approvals_with_vantage,
    sum(case when tag1 = 'OB AUW' then b.vantage end) as ob_sum_vantage,
    count(CASE WHEN tag1 = 'OB AUW' and b.vantage IS NOT NULL THEN a.fbbid END) as ob_approvals_with_vantage,


from approval_count a
left join analytics.credit.customer_level_data_td b
    on a.fbbid = b.fbbid
    and a.week_end_date = b.week_end_date 

left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c

group by 1, 2, 3, 4, 5, 6
order by 1 desc;







CREATE OR REPLACE TABLE indus.public.auw_metrics_combined_14d AS

SELECT 
    a.week_end_date, 
    a.termunits,
    a.risk_level, 
    a.tag1, 
    a.approvals, 
    ZEROIFNULL(b.reg_14_pa) AS reg_14_pa
FROM 
    (
        SELECT 
            week_end_date, 
            risk_level, 
            tag1, 
            termunits, 
            lemur_tag,
            SUM(approvals) as approvals
        FROM indus.public.auw_metrics_approved
        WHERE lower(termunits) <> 'sofi' 
        GROUP BY 1, 2, 3, 4, 5
    ) a

LEFT JOIN 
    (
        SELECT 
            week_end_date, 
            risk_level, 
            tag1, 
            termunits,
            lemur_tag,
            SUM(reg_14_pa) as reg_14_pa
        FROM indus.public.auw_metrics_reg_rate
        WHERE lower(termunits) <> 'sofi'
        GROUP BY 1, 2, 3, 4, 5
    ) b

    ON a.week_end_date = b.week_end_date
    AND a.tag1 = b.tag1
    AND a.risk_level IS NOT DISTINCT FROM b.risk_level
    AND a.termunits IS NOT DISTINCT FROM b.termunits
    AND a.lemur_tag IS NOT DISTINCT FROM b.lemur_tag
    
    ;





















------------ ----------------- ------------------- ------------------ ------------- -------------- ------------


------------------------ MEDIAN SCRIPTS

 CREATE OR REPLACE TABLE indus.public.ob_median_all AS
 WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  AND status__c <> 'Rejected'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   WHERE first_approved_time IS NOT NULL
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    first_approved_credit_limit
  FROM tab1
  WHERE ob_increase_auw_date_min IS NOT NULL
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date
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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

distinct_weekly_events AS (
  SELECT DISTINCT
    DATE_TRUNC('week', eff_dt::date + 4)::date + 2 AS week_end_date,
    fbbid,
    tag1
  FROM aw_list
  WHERE eff_dt IS NOT NULL
    AND tag1 IN ('HVC', 'Pre-Approval', 'OB AUW', 'OG AUW')
),

      
weekly_data AS (
  SELECT
    a.week_end_date,
    a.tag1,
    -- b.credit_limit,
    case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end as credit_limit,
     a.fbbid,
    -- case when tag1 = 'Pre-Approval' then coalesce(annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue

    case when tag1 = 'Pre-Approval' then coalesce(c.OB_ANNUALIZED_REVENUE__c / 12 , c.current_annualized_revenue__c / 12, c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue    -- added nov 25
    
  FROM distinct_weekly_events a
  LEFT JOIN analytics.credit.customer_level_data_td b
      ON a.fbbid = b.fbbid
      AND a.week_end_date = b.week_end_date
left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c, OB_ANNUALIZED_REVENUE__c, current_annualized_revenue__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c

)

SELECT
    week_end_date,
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN credit_limit END) AS ob_median_cl,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN credit_limit END) AS pre_app_median_cl,

    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN monthly_revenue END) AS ob_median_rev,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN monthly_revenue END) AS pre_app_median_rev
    
FROM weekly_data
GROUP BY 1;




















-- Lemur Median
CREATE OR REPLACE TABLE indus.public.ob_median_lemur AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  and status__c <>'Rejected'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   WHERE first_approved_time IS NOT NULL
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    first_approved_credit_limit
  FROM tab1
  WHERE ob_increase_auw_date_min IS NOT NULL
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date
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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),

all_fbbids_in_play AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
),

lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1 
),

core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS lemur_tag
  FROM all_fbbids_in_play a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
),

distinct_events_with_tags AS (
  SELECT DISTINCT
    DATE_TRUNC('week', a.eff_dt::date + 4)::date + 2 AS week_end_date,
    a.fbbid,
    a.tag1,
    COALESCE(t.lemur_tag, 'Core') AS lemur_tag
  FROM auw_with_ranked_risk a
  LEFT JOIN core_lemur_tagging t
    ON a.fbbid = t.fbbid
  WHERE a.eff_dt IS NOT NULL
    AND a.tag1 IN ('HVC', 'Pre-Approval', 'OB AUW', 'OG AUW')
),

weekly_data AS (
  SELECT
    a.week_end_date,
    a.tag1,
    a.lemur_tag,
    -- b.credit_limit,
    case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end as credit_limit,
    a.fbbid,
    -- (b.customer_annual_revenue / 12) AS monthly_revenue
        -- case when tag1 = 'Pre-Approval' then coalesce(c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue

    case when tag1 = 'Pre-Approval' then coalesce(c.OB_ANNUALIZED_REVENUE__c / 12 , c.current_annualized_revenue__c / 12, c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue    -- added nov 25
        
  FROM distinct_events_with_tags a
  LEFT JOIN analytics.credit.customer_level_data_td b
      ON a.fbbid = b.fbbid
      AND a.week_end_date = b.week_end_date
left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c,OB_ANNUALIZED_REVENUE__c, current_annualized_revenue__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c
)

SELECT
    week_end_date,
    lemur_tag,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN credit_limit END) AS ob_median_cl,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN credit_limit END) AS pre_app_median_cl,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN monthly_revenue END) AS ob_median_rev,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN monthly_revenue END) AS pre_app_median_rev,
    
FROM weekly_data
GROUP BY 1, 2
-- ORDER BY 1 DESC 2
;

















-- Termunits Median
CREATE OR REPLACE TABLE indus.public.ob_median_termunits AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  and status__c <>'Rejected'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   WHERE first_approved_time IS NOT NULL
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    first_approved_credit_limit
  FROM tab1
  WHERE ob_increase_auw_date_min IS NOT NULL
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date
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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

distinct_weekly_events AS (
  SELECT DISTINCT
    DATE_TRUNC('week', eff_dt::date + 4)::date + 2 AS week_end_date,
    fbbid,
    tag1
  FROM aw_list
  WHERE eff_dt IS NOT NULL
    AND tag1 IN ('Pre-Approval', 'OB AUW') 
),

weekly_data AS (
  SELECT
    a.week_end_date,
    a.tag1,
    -- b.termunits,
      case when tag1 = 'Pre-Approval' then c.partner_name__c else b.termunits end as termunits,
    -- b.credit_limit,
    case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end as credit_limit,
    a.fbbid,
    -- (b.customer_annual_revenue / 12) AS monthly_revenue
        -- case when tag1 = 'Pre-Approval' then coalesce(c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue

    case when tag1 = 'Pre-Approval' then coalesce(c.OB_ANNUALIZED_REVENUE__c / 12 , c.current_annualized_revenue__c / 12, c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue    -- added nov 25
        
  FROM distinct_weekly_events a
  LEFT JOIN analytics.credit.customer_level_data_td b
      ON a.fbbid = b.fbbid
      AND a.week_end_date = b.week_end_date

left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c,OB_ANNUALIZED_REVENUE__c, current_annualized_revenue__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c
)

SELECT
    week_end_date,
    termunits,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN credit_limit END) AS ob_median_cl,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN credit_limit END) AS pre_app_median_cl,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN monthly_revenue END) AS ob_median_rev,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN monthly_revenue END) AS pre_app_median_rev
    
FROM weekly_data
GROUP BY 1, 2
ORDER BY 1 DESC, 2;




















-- Risk Level Median


CREATE OR REPLACE TABLE indus.public.ob_median_rl AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  and status__c <>'Rejected'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   WHERE first_approved_time IS NOT NULL
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    first_approved_credit_limit
  FROM tab1
  WHERE ob_increase_auw_date_min IS NOT NULL
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date
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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*, 
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 'Zero'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 'One'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 'Two'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 'Three'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 'Four'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 'Five'
            ELSE NULL 
        END AS RISK_LEVEL
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),

distinct_weekly_events AS (
  SELECT DISTINCT
    DATE_TRUNC('week', eff_dt::date + 4)::date + 2 AS week_end_date,
    fbbid,
    tag1,
    risk_level
  FROM auw_with_ranked_risk
  WHERE eff_dt IS NOT NULL
    AND tag1 IN ('Pre-Approval', 'OB AUW') 
),

weekly_data AS (
  SELECT
    a.week_end_date,
    a.tag1,
    a.risk_level,
    -- b.credit_limit,
    case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end as credit_limit,
    a.fbbid,
        -- case when tag1 = 'Pre-Approval' then coalesce(c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue

    case when tag1 = 'Pre-Approval' then coalesce(c.OB_ANNUALIZED_REVENUE__c / 12 , c.current_annualized_revenue__c / 12, c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue    -- added nov 25
        
    -- (b.customer_annual_revenue / 12) AS monthly_revenue
  FROM distinct_weekly_events a
  LEFT JOIN analytics.credit.customer_level_data_td b
      ON a.fbbid = b.fbbid
      AND a.week_end_date = b.week_end_date

left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c,OB_ANNUALIZED_REVENUE__c, current_annualized_revenue__c, annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c

)

SELECT
    week_end_date,
    risk_level,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN credit_limit END) AS ob_median_cl,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN credit_limit END) AS pre_app_median_cl,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN monthly_revenue END) AS ob_median_rev,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN monthly_revenue END) AS pre_app_median_rev
    
FROM weekly_data
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


























------------ ----------------- ------------------- ------------------ ------------- -------------- ------------
CREATE OR REPLACE TABLE indus.public.ob_median_lemur_rl AS
WITH
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
  SELECT
    fbbid,  
    review_complete_time AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
),

HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS loan_review_date
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
),

HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
),

pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
  and status__c <>'Rejected'
    -- AND fbbid IN (
    --   SELECT DISTINCT fbbid
    --   FROM bi.public.customers_data
    --   WHERE first_approved_time IS NOT NULL
    -- )
    AND review_complete_time__c IS NOT NULL
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
),

tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
      AND fundbox_id__c NOT IN (999999999999999910, 9987800100888)  
  )
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min,
    first_approved_credit_limit
  FROM tab1
  WHERE ob_increase_auw_date_min IS NOT NULL
),

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
  SELECT
    fbbid,  
    ob_increase_auw_date AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
),

tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date
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

aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC' AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date, 'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min, 'OB AUW' AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date, 'OB AUW' AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time, 'OG AUW' AS tag1 FROM og_auw_updated_tag
),

risk_reviews AS (
    SELECT 
        fundbox_id__c, 
        risk_level__c, 
        COALESCE(ready_for_uw_start_time__c::DATE, createddate::DATE) AS review_start_date_coalesce,
        COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce
    FROM external_data_sources.salesforce_nova.loan__c
    WHERE risk_level__c IS NOT NULL
      AND COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) IS NOT NULL
),

auw_with_ranked_risk AS (
    SELECT 
        a.*, 
        CASE 
            WHEN b.RISK_LEVEL__C = 'Risk Level 0' THEN 'Zero'
            WHEN b.RISK_LEVEL__C = 'Risk Level I' THEN 'One'
            WHEN b.RISK_LEVEL__C = 'Risk Level II' THEN 'Two'
            WHEN b.RISK_LEVEL__C = 'Risk Level III' THEN 'Three'
            WHEN b.RISK_LEVEL__C = 'Risk Level IV' THEN 'Four'
            WHEN b.RISK_LEVEL__C = 'Risk Level V' THEN 'Five'
            ELSE NULL 
        END AS RISK_LEVEL
    from aw_list a
    LEFT JOIN risk_reviews b
      ON a.fbbid = b.fundbox_id__c
     AND b.review_complete_date_coalesce = a.eff_dt 
),

all_fbbids_in_play AS (
  SELECT DISTINCT fbbid
  FROM auw_with_ranked_risk
  WHERE fbbid IS NOT NULL
),

lemur_logic_base AS (
  SELECT
    a.fbbid,
    'Lemur' AS core_lemur_tag
  FROM analytics.credit.second_look_accounts a
  LEFT JOIN (
    SELECT fbbid, risk_level
    FROM cdc_v2.risk.llm_risk_level
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY decision_time DESC) = 1
  ) lrl
    ON a.fbbid = lrl.fbbid
  WHERE
    (lrl.risk_level IN (5) AND a.automated_cl BETWEEN 30000 AND 49999.99)
    OR
    (lrl.risk_level IN (4, 5) AND a.automated_cl BETWEEN 50000 AND 69999.99)
  GROUP BY 1 
),

core_lemur_tagging AS (
  SELECT
    a.fbbid,
    COALESCE(l.core_lemur_tag, 'Core') AS lemur_tag
  FROM all_fbbids_in_play a
  LEFT JOIN lemur_logic_base l
    ON a.fbbid = l.fbbid
),

distinct_weekly_events_with_all_tags AS (
  SELECT DISTINCT
    DATE_TRUNC('week', a.eff_dt::date + 4)::date + 2 AS week_end_date,
    a.fbbid,
    a.tag1,
    a.risk_level, 
    COALESCE(t.lemur_tag, 'Core') AS lemur_tag 
  FROM auw_with_ranked_risk a
  LEFT JOIN core_lemur_tagging t
    ON a.fbbid = t.fbbid
  WHERE a.eff_dt IS NOT NULL
    AND a.tag1 IN ('Pre-Approval', 'OB AUW') 
),

weekly_data AS (
  SELECT
    a.week_end_date,
    a.tag1,
    a.risk_level,
    a.lemur_tag,
    -- b.credit_limit,
    case when tag1 = 'Pre-Approval' then c.pre_approved_credit_limit__c else b.credit_limit end as credit_limit,
    a.fbbid,
    -- case when tag1 = 'Pre-Approval' then coalesce(c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue

    case when tag1 = 'Pre-Approval' then coalesce(c.OB_ANNUALIZED_REVENUE__c / 12 , c.current_annualized_revenue__c / 12, c.annualized_revenue__c / 12, b.customer_annual_revenue / 12) else b.customer_annual_revenue / 12 end AS monthly_revenue    -- added nov 25
    
    -- (b.customer_annual_revenue / 12) AS monthly_revenue
  FROM distinct_weekly_events_with_all_tags a
  LEFT JOIN analytics.credit.customer_level_data_td b
      ON a.fbbid = b.fbbid
      AND a.week_end_date = b.week_end_date

left join (select distinct fundbox_id__c,pre_approved_credit_limit__c, partner_name__c,OB_ANNUALIZED_REVENUE__c, current_annualized_revenue__c,  annualized_revenue__c from external_data_sources.salesforce_nova.loan__c 
where recordtypeid = '012Rd000000jbbJIAQ' and status__c <>'Rejected' and pre_approved_credit_limit__c is not null) c
on a.fbbid = c.fundbox_id__c

)

SELECT
    week_end_date,
    lemur_tag,
    risk_level,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN credit_limit END) AS ob_median_cl,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN credit_limit END) AS pre_app_median_cl,
    
    MEDIAN(CASE WHEN tag1 = 'OB AUW' THEN monthly_revenue END) AS ob_median_rev,
    MEDIAN(CASE WHEN tag1 = 'Pre-Approval' THEN monthly_revenue END) AS pre_app_median_rev
    
FROM weekly_data
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;