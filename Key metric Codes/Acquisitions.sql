-------------------------------------------------------------------------------- FUNNEL STRUCTURE -----------------------------------------------------------------------
  CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_td1_table AS

WITH RECURSIVE a AS (
    SELECT DATEFROMPARTS(2020, 12, 30) AS week_end_date
    UNION ALL
    SELECT DATEADD(DAY, 7, week_end_date)
    FROM a
    WHERE week_end_date <= current_date() - 7
)

, b AS (
    SELECT CASE WHEN dayofweek(current_date()) = 3 THEN current_date() ELSE current_date() - 1 END week_end_date
)

, source_table AS (
    SELECT * FROM analytics.credit.customer_level_data_td
)

, channel_table AS (
    SELECT DISTINCT channel FROM source_table WHERE channel IS NOT NULL
)

, tier_table AS (
    SELECT DISTINCT tier FROM source_table WHERE tier IS NOT NULL
)

, sub_product_table AS (
    SELECT DISTINCT sub_product FROM source_table WHERE sub_product IS NOT NULL
)

, risk_bucket_table AS (
    SELECT DISTINCT ob_bucket_group AS risk_bucket FROM source_table WHERE ob_bucket_group IS NOT NULL
)

, reg_client_table AS (
    SELECT DISTINCT registration_medium AS reg_client FROM source_table WHERE registration_medium IS NOT NULL
)

, industry_table AS (
    SELECT DISTINCT industry_type FROM source_table WHERE industry_type IS NOT NULL
)

, customer_revenue_table AS (
    SELECT DISTINCT customer_annual_revenue_group_ob AS customer_revenue_group FROM source_table WHERE customer_annual_revenue_group_ob IS NOT NULL
)

, partner_table AS (
    SELECT DISTINCT partner FROM source_table WHERE partner IS NOT NULL
)

, termunits_table AS (
    SELECT DISTINCT termunits FROM source_table WHERE termunits IS NOT NULL
)

, intuit_flow AS (
    SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION
    SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION
    SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION
    SELECT 'Intuit' partner, 'Direct Mail' intuit_flow UNION
    SELECT 'Intuit' partner, 'Marketplace' intuit_flow
    UNION
    SELECT partner, 'not intuit' intuit_flow FROM partner_table WHERE partner <> 'Intuit'
)

, nav_flow AS (
    SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION
    SELECT 'Nav' partner, 'Nav Pre-approval' nav_flow UNION
    SELECT 'Nav' partner, 'Logged In' nav_flow UNION
    SELECT 'Nav' partner, 'Logged Out' nav_flow UNION
    SELECT 'Nav' partner, 'CTA' nav_flow UNION
    SELECT 'Nav' partner, 'Mobile' nav_flow UNION
    SELECT 'Nav' partner, 'Other' nav_flow
    UNION
    SELECT partner, 'not nav' nav_flow FROM partner_table WHERE partner <> 'Nav'
)

, lendio_flow AS (
    SELECT 'Lendio' partner, 'Non-Sales' lendio_flow UNION
    SELECT 'Lendio' partner, 'Direct Sales' lendio_flow UNION
    SELECT 'Lendio' partner, 'Lendio Embedded' lendio_flow
    UNION
    SELECT partner, 'not lendio' lendio_flow FROM partner_table WHERE partner <> 'Lendio'
)

, national_funding_flow AS (
    SELECT 'National Funding' partner, 'National Funding (Small Business Loans API)' national_funding_flow UNION
    SELECT 'National Funding' partner, 'National Funding (Non-API)' national_funding_flow UNION
    SELECT 'Moneyjet' partner, 'Moneyjet' national_funding_flow UNION
    SELECT 'QuickBridge' partner, 'QuickBridge' national_funding_flow
    UNION
    SELECT partner, 'Not National Funding' national_funding_flow FROM partner_table WHERE partner NOT IN ('National Funding', 'Moneyjet', 'QuickBridge')
)

, evercommerce_flow AS (
    SELECT 'EverCommerce' partner, 'InvoiceSimple' evercommerce_flow UNION
    SELECT 'EverCommerce' partner, 'Joist' evercommerce_flow
    UNION
    SELECT partner, 'Not Evercommerce' evercommerce_flow FROM partner_table WHERE partner <> 'EverCommerce'
)

SELECT
    a.week_end_date,
    ct.channel,
    tu.termunits,
    tt.tier,
    spt.sub_product,
    it.industry_type,
    crt.customer_revenue_group,
    rbt.risk_bucket AS bucket_group,
    rct.reg_client,
    pt.partner,
    if_.intuit_flow,
    nf.nav_flow,
    lf.lendio_flow,
    nff.national_funding_flow,
    ecf.evercommerce_flow
FROM a
CROSS JOIN channel_table ct
CROSS JOIN termunits_table tu
CROSS JOIN tier_table tt
CROSS JOIN sub_product_table spt
CROSS JOIN industry_table it
CROSS JOIN customer_revenue_table crt
CROSS JOIN risk_bucket_table rbt
CROSS JOIN reg_client_table rct
CROSS JOIN partner_table pt
LEFT JOIN intuit_flow if_ ON pt.partner = if_.partner
LEFT JOIN nav_flow nf ON pt.partner = nf.partner
LEFT JOIN lendio_flow lf ON pt.partner = lf.partner
LEFT JOIN national_funding_flow nff ON pt.partner = nff.partner
LEFT JOIN evercommerce_flow ecf ON pt.partner = ecf.partner

UNION

SELECT
    b.week_end_date,
    ct.channel,
    tu.termunits,
    tt.tier,
    spt.sub_product,
    it.industry_type,
    crt.customer_revenue_group,
    rbt.risk_bucket AS bucket_group,
    rct.reg_client,
    pt.partner,
    if_.intuit_flow,
    nf.nav_flow,
    lf.lendio_flow,
    nff.national_funding_flow,
    ecf.evercommerce_flow
FROM b
CROSS JOIN channel_table ct
CROSS JOIN termunits_table tu
CROSS JOIN tier_table tt
CROSS JOIN sub_product_table spt
CROSS JOIN industry_table it
CROSS JOIN customer_revenue_table crt
CROSS JOIN risk_bucket_table rbt
CROSS JOIN reg_client_table rct
CROSS JOIN partner_table pt
LEFT JOIN intuit_flow if_ ON pt.partner = if_.partner
LEFT JOIN nav_flow nf ON pt.partner = nf.partner
LEFT JOIN lendio_flow lf ON pt.partner = lf.partner
LEFT JOIN national_funding_flow nff ON pt.partner = nff.partner
LEFT JOIN evercommerce_flow ecf ON pt.partner = ecf.partner

ORDER BY 1 DESC,2,3,4,5,6,7;


  CREATE OR REPLACE table INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_td_table AS (
    SELECT *, 1 AS ONE
    FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_td1_table);



-------------------------------------------------------------------------------- ACQUISITIONS CODE -----------------------------------------------------------------------

CREATE OR REPLACE TABLE INDUS.PUBLIC.ACQUISITIONS_AGG_TD AS

  (WITH FUNNEL AS
      (SELECT DISTINCT A.fbbid
          , channel
          , termunits
          , industry_type
          , customer_annual_revenue_group_ob as customer_annual_revenue_group
          , partner
          , intuit_flow
          , nav_flow
          , lendio_flow
          , national_funding_flow
          , evercommerce_flow
          , tier
          , sub_product
          , ob_bucket_group 
          , registration_medium 
          
          ,  CASE 
                    WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                    WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', registration_time::date,   
                      current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                  END reg_start_week_end_date
      
          ,  CASE 
                    WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
                    WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', cip_connected_time::date,   
                      current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
                  END cip_connected_week_end_date
      
          ,  CASE 
                    WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_connected_time::date+4)::date+2
                    WHEN datediff('day', first_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_connected_time::date,   
                      current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', first_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', first_connected_time::date+4)::date+2
                  END connected_week_end_date
      
          ,  CASE 
                    WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                    WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                      registration_flow_completed_time::date,   
                      current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                  END reg_flow_completed_week_end_date
      
          ,  CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
                        WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                        first_risk_review_time::date,   
                        current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
                      END risk_review_week_end_date
      
          , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                              WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                              registration_flow_completed_time::date, current_date()) <= 0 THEN NULL 
                              WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                              ELSE DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                          END underwritten_week_end_date
      
          , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
                            WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_approved_time::date,   
                              current_date()) <= 0 THEN NULL 
                            WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                            ELSE DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
                          END app_week_end_date 
      
          , MONTH(registration_time::date) - in_business_since_month + (YEAR(registration_time::date) - in_business_since_year)*12 pq_months_in_business
          , CASE WHEN IN_BUSINESS_SINCE_MONTH IS NOT NULL THEN 1 ELSE 0 END has_pq_months_in_business
          
          , fico_onboarding
          
          , calc_revenue
          , has_calc_revenue
          
          , ob_risk_bucket
          , ob_bucket_retro
          , CASE WHEN ob_risk_bucket IS NOT NULL AND ob_risk_bucket != 0 THEN 1 END AS underwritten_not_null
          , CASE WHEN ob_bucket_retro IS NOT NULL AND ob_bucket_retro != 0 THEN 1 END AS approvals_not_null
      
          , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_rejected_time::date+4)::date+2
                WHEN datediff('day', first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_rejected_time::date,  
                current_date()) <= 0 THEN NULL 
                WHEN datediff('day', first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                ELSE DATE_TRUNC('WEEK', first_rejected_time::date+4)::date+2
                END dec_weekend_date 
          
          , CASE WHEN REJECTION_REASON IS NOT NULL THEN REJECTION_REASON
                WHEN REJECTION_REASON IS NULL and CURRENT_CREDIT_STATUS ='rejected' THEN 'Other Rejections'
                WHEN REJECTION_REASON IS NULL and CURRENT_CREDIT_STATUS !='rejected' THEN 'Not Rejected'
                else 'Check cases'
                END AS REJECTION_REASON_2
                
          , CASE WHEN REJECTION_REASON_2='Not Rejected' THEN 0 ELSE 1 END AS is_rejected
      
          , CASE WHEN rejection_reason='Data Rule Reject' THEN A.FBBID ELSE NULL END as data_rejections1
          , CASE WHEN rejection_reason='Fraud Rejection' THEN A.FBBID ELSE NULL END as fraud_rejections1
          , CASE WHEN rejection_reason='Credit Rule Reject' THEN A.FBBID ELSE NULL END as credit_rejections1
          , CASE WHEN rejection_reason='FICO Sub 600' THEN A.FBBID ELSE NULL END as fico_599_rejections1
          , CASE WHEN rejection_reason='Policy Rejection/Model Rejection' THEN A.FBBID ELSE NULL END as policy_model_rejections1
          , CASE WHEN rejection_reason is null AND current_credit_status='rejected' THEN A.FBBID ELSE NULL END as other_rejections1
      
          , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_draw_time::date,   
                current_date()) <= 0 THEN NULL 
                WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                ELSE DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                END ftd_weekend_date 
      
          , CASE WHEN is_ftd7=1 THEN A.FBBID ELSE null END as ftd_7 
          , CASE WHEN is_ftd28=1 THEN A.FBBID ELSE null END as ftd_28
          , CASE WHEN is_ftd_0_7=1 THEN 1 ELSE 0 END as ftd_0_7
          , CASE WHEN is_ftd_8_28=1 THEN 1 ELSE 0 END as ftd_8_28
          , CASE WHEN is_ftd_29_60=1 THEN 1 ELSE 0 END as ftd_29_60
          , CASE WHEN is_ftd_61_=1 THEN 1 ELSE 0 END as ftd_61_
          
          , is_test
          , is_ftu
          , is_underwriting_new
          , is_approved
          , is_registration_flow_completed
      
          , first_draw_amount
          , first_approved_credit_limit
          , B.credit_limit
      
      from analytics.credit.customer_level_data_td A
      LEFT JOIN 
      (SELECT DISTINCT FBBID
            , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                  WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_draw_time::date,   
                  current_date()) <= 0 THEN NULL 
                  WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                  ELSE DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                  END ftd_weekend_date2
            , credit_limit
          FROM analytics.credit.customer_level_data_td
          where week_end_date=ftd_weekend_date2) B
      ON A.fbbid=B.fbbid
      WHERE TRUE
      --AND A.SUB_PRODUCT <> 'Credit Builder'
      AND is_test=0)


  SELECT A.*
  ------------ vertical metrics ------------
      , registrations
      , cip_connected
      , connected
      , flow_completed
      , risk_review
      , underwritten
      , approvals
      , rejections
      , data_rejections
      , fraud_rejections
      , credit_rejections
      , fico_599_rejections
      , policy_model_rejections
      , other_rejections
      , ftds
      , ftds_7
      , ftds_28
      , ftds_0_7
      , ftds_8_28
      , ftds_29_60
      , ftds_61_
  ------------ calculated & reported metrics ------------
      , sum_fico_und
      , underwritten_calc_revenue
      , underwritten_has_calc_revenue
      , sum_risk_und
      , underwritten_not_null     
      , underwritten_pq_months_in_business
      , underwritten_has_pq_months_in_business
      , sum_risk_app
      , approvals_not_null
      , sum_fico_app
  ------------ limits & utilization metrics ------------
      , sum_icl
      , ftd_credit_limit
      , first_draw_amt

  FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_td_table A

  LEFT JOIN --------REGISTRATIONS DATA
  (SELECT reg_start_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT FBBID) REGISTRATIONS
          FROM funnel
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) REG
      ON A.week_end_date=REG.reg_start_week_end_date
          AND A.channel=REG.channel
          AND A.termunits=REG.termunits
          AND A.partner=REG.partner
          AND A.intuit_flow=REG.intuit_flow
          AND A.nav_flow=REG.nav_flow
          AND A.lendio_flow=REG.lendio_flow
          AND A.national_funding_flow=REG.national_funding_flow
          AND A.evercommerce_flow=REG.evercommerce_flow
          AND A.industry_type=REG.industry_type
          AND A.customer_revenue_group=REG.customer_annual_revenue_group
          AND A.tier=REG.tier
          AND A.sub_product=REG.sub_product
          AND A.bucket_group=REG.ob_bucket_group
          AND A.reg_client=REG.registration_medium

  LEFT JOIN ------------ CIP CONNECTION
          (SELECT cip_connected_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT FBBID) CIP_CONNECTED
          FROM FUNNEL
          GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) CIP
          ON A.week_end_date=CIP.cip_connected_week_end_date
          AND A.channel=CIP.channel
          AND A.termunits=CIP.termunits
          AND A.industry_type=CIP.industry_type
          AND A.customer_revenue_group=CIP.customer_annual_revenue_group
          AND A.partner=CIP.partner
          AND A.intuit_flow=CIP.intuit_flow
          AND A.nav_flow=CIP.nav_flow
          AND A.lendio_flow=CIP.lendio_flow
          AND A.national_funding_flow=CIP.national_funding_flow
          AND A.evercommerce_flow=CIP.evercommerce_flow
          AND A.tier=CIP.tier
          AND A.sub_product=CIP.sub_product
          AND A.bucket_group=CIP.ob_bucket_group
          AND A.reg_client=CIP.registration_medium

    LEFT JOIN ------------ CONNECTION
          (SELECT connected_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT FBBID) connected
          FROM funnel
          GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) CON
          ON A.week_end_date=CON.connected_week_end_date
          AND A.channel=CON.channel
          AND A.termunits=CON.termunits
          AND A.industry_type=CON.industry_type
          AND A.customer_revenue_group=CON.customer_annual_revenue_group
          AND A.partner=CON.partner
          AND A.intuit_flow=CON.intuit_flow
          AND A.nav_flow=CON.nav_flow
          AND A.lendio_flow=CON.lendio_flow
          AND A.national_funding_flow=CON.national_funding_flow
          AND A.evercommerce_flow=CON.evercommerce_flow
          AND A.tier=CON.tier
          AND A.sub_product=CON.sub_product
          AND A.bucket_group=CON.ob_bucket_group
          AND A.reg_client=CON.registration_medium

  LEFT JOIN ------------ FLOW COMPLETED
          (SELECT reg_flow_completed_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT FBBID) FLOW_COMPLETED
          FROM funnel
          GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) F
          ON A.week_end_date=F.reg_flow_completed_week_end_date
          AND A.channel=F.channel
          AND A.termunits=F.termunits
          AND A.industry_type=F.industry_type
          AND A.customer_revenue_group=F.customer_annual_revenue_group
          AND A.partner=F.partner
          AND A.intuit_flow=F.intuit_flow
          AND A.nav_flow=F.nav_flow
          AND A.lendio_flow=F.lendio_flow
          AND A.national_funding_flow=F.national_funding_flow
          AND A.evercommerce_flow=F.evercommerce_flow
          AND A.tier=F.tier
          AND A.sub_product=F.sub_product
          AND A.bucket_group=F.ob_bucket_group
          AND A.reg_client=F.registration_medium

      LEFT JOIN ------------ RISK REVIEW
          (SELECT risk_review_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT fbbid) risk_review
          FROM funnel
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) R
          ON A.week_end_date=R.risk_review_week_end_date
          AND A.channel=R.channel
          AND A.termunits=R.termunits
          AND A.industry_type=R.industry_type
          AND A.customer_revenue_group=R.customer_annual_revenue_group
          AND A.partner=R.partner
          AND A.intuit_flow=R.intuit_flow
          AND A.nav_flow=R.nav_flow
          AND A.lendio_flow=R.lendio_flow
          AND A.national_funding_flow=R.national_funding_flow
          AND A.evercommerce_flow=R.evercommerce_flow
          AND A.tier=R.tier
          AND A.sub_product=R.sub_product
          AND A.bucket_group=R.ob_bucket_group
          AND A.reg_client=R.registration_medium

  LEFT JOIN ------------ UNDERWRITTEN
          (SELECT underwritten_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 

            , count(distinct fbbid) underwritten

            , COUNT(DISTINCT CASE WHEN ob_risk_bucket IS NOT NULL AND ob_risk_bucket != 0 THEN fbbid END) AS underwritten_not_null
            , sum(pq_months_in_business) underwritten_pq_months_in_business
            , sum(has_pq_months_in_business) underwritten_has_pq_months_in_business
            , sum(calc_revenue) underwritten_calc_revenue
            , sum(has_calc_revenue) underwritten_has_calc_revenue
            , sum(fico_onboarding) sum_fico_und
            , sum(ob_risk_bucket) sum_risk_und
          FROM funnel 
            WHERE is_underwriting_new=1
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) UW
          ON A.week_end_date=UW.underwritten_week_end_date
          AND A.channel=UW.channel
          AND A.termunits=UW.termunits
          AND A.industry_type=UW.industry_type
          AND A.customer_revenue_group=UW.customer_annual_revenue_group
          AND A.partner=UW.partner
          AND A.intuit_flow=UW.intuit_flow
          AND A.nav_flow=UW.nav_flow
          AND A.lendio_flow=UW.lendio_flow
          AND A.national_funding_flow=UW.national_funding_flow
          AND A.evercommerce_flow=UW.evercommerce_flow
          AND A.tier=UW.tier
          AND A.sub_product=UW.sub_product
          AND A.bucket_group=UW.ob_bucket_group
          AND A.reg_client=UW.registration_medium

      LEFT JOIN ------------  APPROVALS
          (SELECT app_week_end_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium
            , COUNT(DISTINCT fbbid) approvals
            , COUNT(DISTINCT CASE WHEN IS_FTU=1 THEN ftd_7 ELSE NULL END) ftds_7
            , COUNT(DISTINCT CASE WHEN IS_FTU=1 THEN ftd_28 ELSE NULL END) ftds_28
            , SUM(ob_bucket_retro) sum_risk_app
            , COUNT(DISTINCT CASE WHEN ob_bucket_retro IS NOT NULL AND ob_bucket_retro != 0 THEN fbbid END) AS approvals_not_null
            , SUM(fico_onboarding) sum_fico_app
            , SUM(first_approved_credit_limit) sum_icl

          FROM funnel
            WHERE is_approved=1
            AND sub_product<>'Credit Builder'
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) APP
          ON A.week_end_date=APP.app_week_end_date
          AND A.channel=APP.channel
          AND A.termunits=APP.termunits
          AND A.industry_type=APP.industry_type
          AND A.customer_revenue_group=APP.customer_annual_revenue_group
          AND A.partner=APP.partner
          AND A.intuit_flow=APP.intuit_flow
          AND A.nav_flow=APP.nav_flow
          AND A.lendio_flow=APP.lendio_flow
          AND A.national_funding_flow=APP.national_funding_flow
          AND A.evercommerce_flow=APP.evercommerce_flow
          AND A.tier=APP.tier
          AND A.sub_product=APP.sub_product
          AND A.bucket_group=APP.ob_bucket_group
          AND A.reg_client=APP.registration_medium

      LEFT JOIN ------------  REJECTIONS
      (SELECT dec_weekend_date
            , channel
            , termunits
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT data_rejections1) as data_rejections
            , COUNT(DISTINCT fraud_rejections1) as fraud_rejections
            , COUNT(DISTINCT credit_rejections1) as credit_rejections
            , COUNT(DISTINCT fico_599_rejections1) as fico_599_rejections
            , COUNT(DISTINCT policy_model_rejections1) as policy_model_rejections
            , COUNT(DISTINCT other_rejections1) as other_rejections
            , SUM(is_rejected) rejections
      FROM funnel
            WHERE is_registration_flow_completed=1
          GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) D
          ON A.week_end_date=D.dec_weekend_date
          AND A.channel=D.channel
          AND A.termunits=D.termunits
          AND A.industry_type=D.industry_type
          AND A.customer_revenue_group=D.customer_annual_revenue_group
          AND A.partner=D.partner
          AND A.intuit_flow=D.intuit_flow
          AND A.nav_flow=D.nav_flow
          AND A.lendio_flow=D.lendio_flow
          AND A.national_funding_flow=D.national_funding_flow
          AND A.evercommerce_flow=D.evercommerce_flow
          AND A.tier=D.tier
          AND A.sub_product=D.sub_product
          AND A.bucket_group=D.ob_bucket_group
          AND A.reg_client=D.registration_medium

  LEFT JOIN ------------  FTDS
          ( SELECT ftd_weekend_date
                , channel
                , termunits
                , industry_type
                , customer_annual_revenue_group
                , partner
                , intuit_flow
                , nav_flow
                , lendio_flow
                , national_funding_flow
                , evercommerce_flow
                , tier
                , sub_product
                , ob_bucket_group 
                , registration_medium 
                , COUNT(DISTINCT fbbid) ftds
                , SUM(CASE WHEN ftd_0_7=1 THEN 1 ELSE 0 END) ftds_0_7
                , SUM(CASE WHEN ftd_8_28=1 THEN 1 ELSE 0 END) ftds_8_28
                , SUM(CASE WHEN ftd_29_60=1 THEN 1 ELSE 0 END) ftds_29_60
                , SUM(CASE WHEN ftd_61_=1 THEN 1 ELSE 0 END) ftds_61_
                , SUM(credit_limit) ftd_credit_limit
                , SUM(first_draw_amount) first_draw_amt
          FROM funnel
          WHERE true
            AND is_test=0
            AND is_ftu=1
            AND sub_product <> 'Credit Builder'
          GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            order by 1,2,3,4,5,6,7,8,9,10) B
          ON A.week_end_date=B.ftd_weekend_date
          AND A.channel=B.channel
          AND A.termunits=B.termunits
          AND A.industry_type=B.industry_type
          AND A.customer_revenue_group=B.customer_annual_revenue_group
          AND A.partner=B.partner
          AND A.intuit_flow=B.intuit_flow
          AND A.lendio_flow=B.lendio_flow
          AND A.national_funding_flow=B.national_funding_flow
          AND A.evercommerce_flow=B.evercommerce_flow
          AND A.nav_flow=B.nav_flow
          AND A.tier=B.tier
          AND A.sub_product=B.sub_product
          AND A.bucket_group=B.ob_bucket_group
          AND A.reg_client=B.registration_medium

  WHERE (registrations is not null
      OR cip_connected is not null
      OR connected is not null
      OR flow_completed is not null
      OR risk_review is not null
      OR underwritten is not null
      OR approvals is not null
      OR rejections is not null
      OR data_rejections is not null
      OR fraud_rejections is not null
      OR credit_rejections is not null
      OR fico_599_rejections is not null
      OR policy_model_rejections is not null
      OR other_rejections is not null
      OR ftds is not null
      OR ftds_7 is not null
      OR ftds_28 is not null
      OR ftds_0_7 is not null
      OR ftds_8_28 is not null
      OR ftds_29_60 is not null
      OR ftds_61_ is not null
      OR sum_fico_und is not null
      OR underwritten_calc_revenue is not null
      OR underwritten_has_calc_revenue is not null
      OR sum_risk_und is not null
      OR underwritten_not_null is not null     
      OR underwritten_pq_months_in_business is not null
      OR underwritten_has_pq_months_in_business is not null
      OR sum_risk_app is not null
      OR approvals_not_null is not null
      OR sum_fico_app is not null
      OR sum_icl is not null
      OR ftd_credit_limit is not null
      OR first_draw_amt is not null)
  ORDER BY 1,2,3,4,5,6,7,8,9,10)
  ;
