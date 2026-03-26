CREATE OR REPLACE TABLE INDUS.PUBLIC.ACQUISITIONS_partner_raman AS

  (WITH FUNNEL AS
      (SELECT DISTINCT A.fbbid
          , channel
          , termunits
          , industry_type
          , customer_annual_revenue_group
          , icl_bucket
          , partner
          , intuit_flow
          , nav_flow
          , lendio_flow
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
          FROM analytics.credit.customer_level_data_td_pp
          where week_end_date=ftd_weekend_date2) B
      ON A.fbbid=B.fbbid
      WHERE TRUE
      AND SUB_PRODUCT <> 'Credit Builder'
      AND is_test=0)


  SELECT A.*
  ------------ vertical metrics ------------
      , registrations
      , approvals
      , ftds_7

  FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_pp_raman A

  LEFT JOIN --------REGISTRATIONS DATA
  (SELECT app_week_end_date
            , channel
            , termunits
            , industry_type
            , icl_bucket
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , tier
            , sub_product
            , ob_bucket_group 
            , registration_medium 
            , COUNT(DISTINCT FBBID) REGISTRATIONS
          FROM funnel
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            order by 1,2,3,4,5,6,7,8,9,10) REG
      ON A.week_end_date=REG.app_week_end_date
          AND A.channel=REG.channel
          AND A.termunits=REG.termunits
          AND A.icl_bucket=REG.icl_bucket
          AND A.partner=REG.partner
          AND A.intuit_flow=REG.intuit_flow
          AND A.nav_flow=REG.nav_flow
          AND A.lendio_flow=REG.lendio_flow
          AND A.industry_type=REG.industry_type
          AND A.customer_revenue_group=REG.customer_annual_revenue_group
          AND A.tier=REG.tier
          AND A.sub_product=REG.sub_product
          AND A.bucket_group=REG.ob_bucket_group
          AND A.reg_client=REG.registration_medium

      LEFT JOIN ------------  APPROVALS
          (SELECT app_week_end_date
            , channel
            , termunits
            , icl_bucket
            , industry_type
            , customer_annual_revenue_group
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
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
            GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            order by 1,2,3,4,5,6,7,8,9,10) APP
          ON A.week_end_date=APP.app_week_end_date
          AND A.channel=APP.channel
          AND A.termunits=APP.termunits
          AND A.icl_bucket=APP.icl_bucket
          AND A.industry_type=APP.industry_type
          AND A.customer_revenue_group=APP.customer_annual_revenue_group
          AND A.partner=APP.partner
          AND A.intuit_flow=APP.intuit_flow
          AND A.nav_flow=APP.nav_flow
          AND A.lendio_flow=APP.lendio_flow
          AND A.tier=APP.tier
          AND A.sub_product=APP.sub_product
          AND A.bucket_group=APP.ob_bucket_group
          AND A.reg_client=APP.registration_medium

  ORDER BY 1,2,3,4,5,6,7,8,9,10)
  ;