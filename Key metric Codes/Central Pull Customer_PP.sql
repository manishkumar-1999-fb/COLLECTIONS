CREATE OR REPLACE TABLE ANALYTICS.CREDIT.customer_level_data_td AS (
SELECT 
    CD.FBBID
   ,  CASE WHEN DACD.week_end_date IS NULL THEN CD.reg_week_end ELSE DACD.week_end_date END week_end_date
   ,  CASE WHEN DACD.week_start_date IS NULL THEN CD.reg_week_start ELSE DACD.week_start_date END week_start_date
   , underwritten_time
   , CD.first_approved_time
   , CD.first_rejected_time
   , CD.cip_connected_time
   , CD.first_cip_attempt_time
   , CD.is_cip_connected
   , CD.is_connected
   , CD.first_connected_time
   , CD.registration_time
   , CD.is_registration_flow_completed
   , CD.registration_flow_completed_time
   , CD.is_risk_review
   , CD.first_risk_review_time
   , CD.last_rejected_time
   , CD.first_approved_credit_limit
   , CD.first_draw_amount
   , CD.fico_onboarding
   , CD.first_approved_credit_status_comment
   , CD.first_rejected_credit_status_comment
   , CD.is_approved
   , CD.first_rejected_reason
   , CD.underwriting_flow
   , CD.is_fraud
   , CD.current_credit_status
   , CD.first_decision_time
   , CD.current_credit_status_start_time
   , CD.first_fraud_tag_time
   , CD.first_draw_time
   , CD.is_ftu
   , CD.fi_account_connected_time
   , CD.first_dashboard_login_device_type
   , CD.first_dashboard_login_integration
   , CD.first_dashboard_login_time
   , CD.lt_acquisition_channel
   , CD.lt_acquisition_subchannel
   , CD.lt_source
   , CD.registration_client
   , CD.registration_source
   , CD.year_started
   , CD.is_underwriting_new
   , CD.is_underwriting_old
   , CD.is_ftd7
   , CD.is_ftd28
   , CD.is_ftd_0_7
   , CD.is_ftd_8_28
   , CD.is_ftd_29_60
   , CD.is_ftd_61_
   , CD.is_test
   , DACD.credit_limit
   , FMD.OUTSTANDING_PRINCIPAL_DUE
   , FMD.IS_CHARGED_OFF as is_charged_off_fmd
   , FMD.originated_amount
   , FMD.charge_off_date_fmd
   , DACD.credit_status
   , DACD.credit_status_reason
   , DACD.credit_status_comment
   , DACD.credit_limit_reason
   , DACD.credit_limit_decision_mode
   , DACD.credit_limit_comment
   , DACD.account_status
   , DACD.account_status_reason
   , DACD.account_status_comment
   , DACD.dpd_days_corrected
   , DACD.is_chargeoff
   , DACD.chargeoff_time
   , DACD.is_locked_dashboard
   , DACD.dashboard_status_change_time
   , DACD.dashboard_status_change_reason
   , DACD.is_disabled
   , DACD.is_active_account   
   , DACD.recovery_suggested_state 
   , DACD.recovery_suggested_substate
   , DACD.recovery_max_suggested_state
   , DACD.delinquency_bucket
   , DACD.delinquency_level
   , DACD.last_increase_time
   , DACD.last_draw_time
   , DACD.fee_rate_52
   , DACD.fundbox_plus_status
   , dacd.fees_due
   , dacd.outstanding_principal
   , dacd.discount_pending
   , dacd.AVAILABLE_BALANCE_FI
   , dacd.is_urp_eligible
   , dacd.credit_utilization
   , dacd.fi_data_update_to_time
   , DACD.PLATFORM_DATA_UPDATE_TO_TIME
   , DACD.CREDIT_LIMIT_CHANGE
   , DACD.sub_product AS sub_product_daily
   , FD.tenure_status
   , CASE WHEN CD.calc_revenue>0 and CD.calc_revenue<=150000 THEN '$0 - $150K'
          WHEN CD.calc_revenue>150000 and CD.calc_revenue<=500000 THEN '$150K - $500K'
          WHEN CD.calc_revenue>500000 and CD.calc_revenue<=1000000 THEN '$500K - $1M'
          WHEN CD.calc_revenue>1000000 THEN '> $1M'
          ELSE 'Other/No Data' 
        END AS customer_annual_revenue_group
   , CD.calc_revenue -- only initial i think
   , CD.has_calc_revenue
   , CD.industry_type
   , F.registration_medium
   , F.channel
   , F.partner
   , F.intuit_flow
   , F.national_funding_flow
   , F.nav_flow
   , F.lendio_flow
   , F.tier
   , F.termunits
   , F.sub_product
   , F.icl_bucket
   , CD.product
   , F.ob_bucket_group
   , F.ob_bucket_retro
   , fd.new_cust_filter
   , FD.TENURE_BUCKET
   ---- These 2 are largely in the case that a customer is not approved, then the first model output is taken ; mostly relevant for acquisitions
   , CASE WHEN cd.is_approved = 1 THEN f.ob_bucket_retro
    	  ELSE f.ob_risk_bucket_first
        END ob_risk_bucket
   , CASE WHEN ob_risk_bucket BETWEEN 1 AND 4 THEN 'OB: 1-4'
          WHEN ob_risk_bucket BETWEEN 5 AND 7 THEN 'OB: 5-7'
          WHEN ob_risk_bucket BETWEEN 8 AND 10 THEN 'OB: 8-10'
          WHEN ob_risk_bucket BETWEEN 11 AND 12 THEN 'OB: 11-12'
          WHEN ob_risk_bucket >= 13 THEN 'OB: 13+'
          ELSE 'No Bucket'
        END ob_risk_bucket_group 
   --
   , FD.og_bucket_group
   , FD.og_bucket_retro
   --
   , CASE WHEN FD.tenure_status = 'Tenure <60' THEN F.ob_bucket_retro
          WHEN FD.tenure_status = 'Tenure >=60' THEN FD.og_bucket_retro
        END risk_bucket ---- this takes OB or OG bucket depending on whether the customer is New or Existing
   , AUW.fbbid as fbbid_auw
   , CASE WHEN DACD.week_end_date>=augmented_uw_start_week_end THEN 1 ELSE 0 END AS augmented_uw_start_flag
   , CASE WHEN DACD.week_end_date>=augmented_uw_cl_post_doc_week_end THEN 1 ELSE 0 END AS augmented_uw_cl_post_doc_flag
   , CASE WHEN DACD.week_end_date>=augmented_uw_cl_pre_doc_week_end THEN 1 ELSE 0 END AS augmented_uw_cl_pre_doc_flag
   , CASE WHEN DACD.week_end_date>=AUW_PRE_DOC_REVIEW_START_TIME_WEEKEND THEN 1 ELSE 0 END AS AUW_PRE_DOC_REVIEW_flag
   , CASE WHEN DACD.week_end_date>=AUW_POST_DOC_REVIEW_START_TIME_WEEKEND THEN 1 ELSE 0 END AS AUW_POST_DOC_REVIEW_flag
   , CASE WHEN DACD.week_end_date>=AUW_POST_DOC_REVIEW_COMPLETE_TIME_WEEKEND THEN 1 ELSE 0 END AS AUW_POST_DOC_REVIEW_COMPLETE_flag
   , CASE WHEN DACD.week_end_date>=AUW_PRE_DOC_REVIEW_COMPLETE_TIME_WEEKEND THEN 1 ELSE 0 END AS AUW_PRE_DOC_REVIEW_COMPLETE_flag
   , CASE WHEN DACD.week_end_date>=AUW_PRE_DOC_REVIEW_COMPLETE_TIME_WEEKEND AND AUW_PRE_DOC_REVIEW_STATUS__C ILIKE '%Increase%' THEN
        AUW_PRE_DOC_APPROVED_LIMIT__C ELSE NULL END AUW_CL_PRE --
   , CASE WHEN DACD.week_end_date>=AUW_POST_DOC_REVIEW_COMPLETE_TIME_WEEKEND AND AUW_POST_DOC_REVIEW_STATUS__C ILIKE '%Increase%' THEN
        AUW_POST_DOC_APPROVED_LIMIT__C ELSE NULL END AUW_CL_POST -- 
   , augmented_uw_start_week_end 
   , augmented_uw_cl_post_doc_week_end
   , augmented_uw_cl_pre_doc_week_end
   , AUW_POST_DOC_REVIEW_COMPLETE_TIME_WEEKEND
   , AUW_PRE_DOC_REVIEW_COMPLETE_TIME_WEEKEND
   , AUW_FLOW_TYPE__C
  -- , AUW_STATUS__C
   , AUW_PRE_DOC_REVIEW_START_TIME_WEEKEND
   , AUW_POST_DOC_REVIEW_START_TIME_WEEKEND
   -- , AUW_START_DATE_TIME_WEEKEND
   , AUW_PHASE__C
   , AUW_PRE_DOC_REVIEW_STATUS__C
   , AUW_PRE_DOC_APPROVED_LIMIT__C
   , AUW_POST_DOC_APPROVED_LIMIT__C
   , AUW_POST_DOC_REVIEW_STATUS__C
   , APPROVED_UW_CREDIT_LIMIT__C 
   , SL.manual_review_start_week_end_date
   , SL.review_complete_week_end_date
   , SL_review_complete_FLAG
   , SL_TOTAL_EXPOSURE_SL
   , SL_INC_FLAG
   , SL_DEC_FLAG
   , SL_NO_CHANGE_FLAG
   , SL_INC
   , SL_DEC
   , SL_AUTOMATED_CL
   , SLA_rejected_time
   , SLA_first_approved_credit_limit
   , cl_delta as SLA_cl_delta
   , first_suggested_credit_limit__c as first_suggested_credit_limit_sl
   , og_auw_week_end
   , CASE WHEN OG_AUW.og_auw_week_end=DACD.week_end_date then 1 else 0 end as og_auw_this_week_flag
   , LAST_VALUE(OG_AUW_INCREASE_DATE IGNORE NULLS) OVER (PARTITION BY DACD.FBBID ORDER BY DACD.WEEK_END_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
         ROW) AS IS_OG_AUW
   , og_auw_limit
   , incr_amnt AS og_auw_increase
   , CASE WHEN PQ.fbbid IS NOT NULL then 1 ELSE 0 END pre_qual_flag
   , FEB.rejection_reason
   , D.offer_dt
   , D.offer
   , D.goto_cl
   , D.offer_status
   , D.incr_amt
   , D.accept_tm
   , D.new_12w_pct
   , D.new_24w_pct
   , RMRE.flow_type
   , RMRE.rmr_segment
   , enter_rmr_this_week
   , unrmr_this_week
   , CASE WHEN R.fbbid is not null then 1 else 0 END AS is_rmr
   , rmre.rmr_group
   , R.lock_start AS rmr_lock_start
   , R.lock_end as rmr_lock_end
   , G.lock_start -- disabled payoff
   , G.lock_end -- disabled payoff
   , C.TOTAL_PAID
   , C.FEES_PAID
   , rev_run.fees_paid as CUMULATIVE_FEES_PAID
   , C.PRINCIPAL_PAID
   , C.IS_AFTER_CHARGEOFF
   , PQ.in_business_since_month
   , PQ.in_business_since_year
   , CD.reg_week_end
   , CD.reg_week_start
   , current_credit_status_reason
  
FROM

------------------------------------------------------- CUSTOMERS DATA -------------------------------------------------------

    (SELECT fbbid
         , first_rejected_time
         , first_approved_time
         , nvl(first_approved_time, first_rejected_time) underwritten_time
         , cip_connected_time
         , CASE 
            	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
            	WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                cip_connected_time::date, current_date()) <= 0 THEN NULL 
            	WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            	ELSE DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
                END cip_connected_week_end_date
         , first_cip_attempt_time
         , is_cip_connected
         , is_connected
         , first_connected_time
         , CASE 
            	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_connected_time::date +4)::date+2
            	WHEN datediff('day', first_connected_time::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                first_connected_time::date , current_date()) <= 0 THEN NULL 
            	WHEN datediff('day', first_connected_time::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            	ELSE DATE_TRUNC('WEEK', first_connected_time::date +4)::date+2
                END first_connected_week_end_date
         , is_registration_flow_completed
         , registration_flow_completed_time
         , CASE 
            	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
            	WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                registration_flow_completed_time::date, current_date()) <= 0 THEN NULL 
            	WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            	ELSE DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                END reg_complete_week_end_date
         , CASE WHEN FIRST_RISK_REVIEW_TIME IS NULL THEN 0 ELSE 1 END AS is_risk_review
         , first_risk_review_time
         , CASE 
            	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
            	WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                first_risk_review_time::date, current_date()) <= 0 THEN NULL 
            	WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            	ELSE DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
                END risk_review_week_end_date
         , last_rejected_time
         , first_approved_credit_limit
         , first_approved_credit_limit_reason
         , first_draw_amount
         , fico_onboarding
         , current_credit_status
         , current_credit_status_reason
         , first_approved_credit_status_comment
         , first_rejected_credit_status_comment
         , is_approved
         , first_rejected_reason
         , underwriting_flow
         , is_test
         , is_fraud
         , current_credit_status_start_time
         , registration_time
         , CASE
               WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_time::date+4)::date+2
               WHEN datediff('day', registration_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', registration_time, current_date()) <= 0 THEN
               NULL 
               WHEN datediff('day', registration_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', registration_time::date+4)::date+2
           END REG_WEEK_END
         , (DATEADD('day', -6, REG_WEEK_END))  AS REG_WEEK_START
         , first_fraud_tag_time
         , first_draw_time
         , fi_account_connected_time
         , first_dashboard_login_device_type
         , first_dashboard_login_integration
         , first_dashboard_login_time
         , lt_acquisition_channel
         , lt_acquisition_subchannel
         , lt_source
         , registration_client
         , registration_source
         , product
         , year_started
         , is_underwriting
         , CASE 
                WHEN (left(industry_naics_code,3) BETWEEN 441 AND 453 OR left(industry_naics_code,3) = 722) THEN 'Retail / Restaurants'
                WHEN left(industry_naics_code,3) = 454 THEN 'E-Commerce'
                WHEN left(industry_naics_code,3) BETWEEN 236 AND 238 THEN 'Construction'
                WHEN left(industry_naics_code,3) BETWEEN 481 AND 492 THEN 'Transportation'
                WHEN left(industry_naics_code,3) = 541 THEN 'Professional Services'
                ELSE 'Others'
            END industry_type
         , CASE 
             WHEN is_underwriting = 1 THEN 1 
             WHEN first_rejected_reason in ('Onboarding dynamic decision reject') then 1
             ELSE 0
           END AS is_underwriting_new
         , is_underwriting is_underwriting_old
         , first_decision_time
         , CASE 
        	 WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_decision_time::date+4)::date+2
        	 WHEN datediff('day', first_decision_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
             first_decision_time::date, current_date())
             <= 0 THEN NULL 
        	 WHEN datediff('day', first_decision_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
        	 ELSE DATE_TRUNC('WEEK', first_decision_time::date+4)::date+2
             END first_dec_week_end_date
         , COALESCE(first_account_size_accounting_software,first_account_size_fi) * 12 calc_revenue ---- This only gives initial revenue; also, if both are null it will  be null, and nulls will be categorised under 'Other/No Data'
         , CASE WHEN first_account_size_accounting_software IS NOT NULL THEN 1 WHEN first_account_size_fi IS NOT NULL THEN 1 ELSE 0 END    
            has_calc_revenue
         , CASE 
             WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
             WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
             first_approved_time::date,
             current_date()) <= 0 THEN NULL 
             WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
             ELSE DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
             END app_week_end_date
        , is_ftu
        , CASE WHEN datediff('day', first_approved_time::date, first_draw_time::date) <= 7 THEN 1 ELSE 0 END AS is_ftd_0_7
        , CASE WHEN datediff('day', first_approved_time::date, first_draw_time::date) BETWEEN 8 AND 28 THEN 1 ELSE 0 END AS is_ftd_8_28
        , CASE WHEN datediff('day', first_approved_time::date, first_draw_time::date) BETWEEN 29 AND 60 THEN 1 ELSE 0 END AS is_ftd_29_60
        , CASE WHEN datediff('day', first_approved_time::date, first_draw_time::date) >= 61 THEN 1 ELSE 0 END AS is_ftd_61_
        , CASE 
        	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_time::date +4)::date+2
        	WHEN datediff('day', first_draw_time::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_draw_time::date ,
            current_date()) <= 0 THEN NULL 
        	WHEN datediff('day', first_draw_time::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
        	ELSE DATE_TRUNC('WEEK', first_draw_time::date +4)::date+2
            END ftd_week_end_date
        , CASE WHEN datediff('day',first_approved_time::date,first_draw_time::date)<8 THEN 1 ELSE 0 END AS is_ftd7
        , CASE WHEN datediff('day',first_approved_time::date,first_draw_time::date)<29 THEN 1 ELSE 0 END AS is_ftd28
    FROM BI.PUBLIC.customers_data
    ) CD
    
    ------------------------------------------------------- DACD -------------------------------------------------------
    
    LEFT JOIN 
    (SELECT fbbid
         , CASE
               WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', edate::date+4)::date+2
               WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', edate, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
           END week_end_date
         , DATEADD('day', -6, week_end_date)  AS week_start_date
         , sub_product 
         --, icl_bucket
         , credit_limit
         , credit_status
         , credit_status_reason
         , credit_status_comment
         , credit_sub_status
         , credit_limit_reason
         , credit_limit_decision_mode
         , credit_limit_comment
         , account_status
         , account_status_reason
         , account_status_comment
         , account_status_decision_mode
         -- , date_trunc('week', registration_date + 4)::date - 4 registration_week
         -- , registration_week + 6 AS registration_week_end_date
         -- , left(registration_date,7) AS registration_month
         -- , CONCAT(year(registration_date)::varchar(), CASE WHEN month(registration_date) IN (1,2,3) THEN '_Q1' WHEN month(registration_date) IN (4,5,6)
         --    THEN '_Q2' WHEN month(registration_date) IN (7,8,9) THEN '_Q3' WHEN month(registration_date) IN (10,11,12) THEN '_Q4' END) 
         --    AS registration_quarter
         , CASE WHEN DPD_DAYS IS NULL  AND is_chargeoff = 0 THEN 0
	            WHEN DPD_DAYS IS NULL  AND is_chargeoff = 1 THEN 98
	            ELSE DPD_DAYS
                END AS dpd_days_corrected 
         , is_chargeoff
         , chargeoff_time
         , is_locked_dashboard
         , dashboard_status_change_time
         , dashboard_status_change_reason
         , is_disabled
         , is_active_account
         , is_xl
         , xl_status    
         , cip_connected_time
         , cip_type
         , recovery_suggested_state 
         , recovery_suggested_substate
         , recovery_max_suggested_state
         , delinquency_bucket
         , delinquency_level
         , last_increase_time
         , first_draw_amount
         , first_draw_time
         , first_draw_time
         , last_draw_time
         , loans_created
         , credited_amount
         , default_principal
         , default_principal_paid
         , default_fees_paid
         , last_good_debit_date
         , fee_rate_52
         , fundbox_plus_status
         , credit_utilization
         , fees_due
         , outstanding_principal
  		 , discount_pending
 	     , AVAILABLE_BALANCE_FI
         , is_urp_eligible
         , fi_data_update_to_time
         , PLATFORM_DATA_UPDATE_TO_TIME
         , CREDIT_LIMIT_CHANGE
    FROM BI.PUBLIC.daily_approved_customers_data
    WHERE edate=week_end_date
    AND edate>='2020-12-24'
    ORDER BY 1,2) DACD
    ON CD.fbbid = DACD. fbbid

    
------------------------------------------------------- FILTERS TABLE - Static -------------------------------------------------------

LEFT JOIN 
    (SELECT DISTINCT fbbid
         -- , CASE
         --       WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', edate::date+4)::date+2
         --       WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', edate, current_date()) <= 0 THEN NULL 
         --       WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
         --       ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
         --   END week_end_date
         -- , CASE WHEN new_cust_filter='Existing Customer' THEN 'Tenure >=60' 
         --        WHEN new_cust_filter='New Customer' THEN 'Tenure <60' 
         --        ELSE new_cust_filter
         --    END tenure_status
        , reg_client as registration_medium
        , channel
        , partner
        , intuit_flow
        , national_funding_flow
        , nav_flow
        , lendio_flow
        , tier
        , termunits
        , sub_product
        , icl_bucket
       -- , customer_annual_revenue -- i forgot how to calc annual rev
        , ob_bucket_group_retro as ob_bucket_group
        , ob_bucket_retro
        -- , og_bucket_retro 
        -- , og_bucket_group_retro as og_bucket_group
        , ob_risk_bucket_first
    from indus.public.indus_key_metrics_filters_v2
) F
     ON CD.fbbid=F.fbbid

------------------------------------------------------- FILTERS TABLE - Dynamic -------------------------------------------------------

LEFT JOIN 
    (SELECT DISTINCT fbbid
         , new_cust_filter
         , CASE
               WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', edate::date+4)::date+2
               WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', edate, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
           END week_end_date
         , CASE WHEN new_cust_filter='Existing Customer' THEN 'Tenure >=60' 
                WHEN new_cust_filter='New Customer' THEN 'Tenure <60' 
                ELSE new_cust_filter
            END tenure_status
       -- , customer_annual_revenue -- i forgot how to calc annual rev
        , og_bucket_retro 
        , og_bucket_group_retro as og_bucket_group
        ,TENURE_BUCKET
    from indus.public.indus_key_metrics_filters_v2
    where edate=week_end_date
) FD
    ON CD.fbbid=FD.fbbid
    AND DACD.week_end_date=FD.week_end_date

------------------------------------------------------- OB AUW -------------------------------------------------------

LEFT JOIN 
    (SELECT fundbox_id__c as fbbid
         , recordtypeid
         , AUW_POST_DOC_REVIEW_COMPLETE_TIME__C
         , AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C
         , AUW_FLOW_TYPE__C
         , AUW_STATUS__C
         , AUW_PRE_DOC_REVIEW_START_TIME__C
         , AUW_PHASE__C
         , APPROVED_UW_CREDIT_LIMIT__C
         , AUW_PRE_DOC_REVIEW_STATUS__C
         , AUW_POST_DOC_REVIEW_STATUS__C
         , AUW_PRE_DOC_APPROVED_LIMIT__C
         , AUW_POST_DOC_APPROVED_LIMIT__C
         , AUW_START_DATE_TIME__C
         , AUW_POST_DOC_REVIEW_START_TIME__C
         , CASE WHEN AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C IS NOT NULL THEN 1 ELSE 0 
                END AS AUW_PRE_DOC_REVIEW_COMPLETE_FLAG
         ,CASE WHEN AUW_POST_DOC_REVIEW_COMPLETE_TIME__C IS NOT NULL THEN 1 ELSE 0 
                END AS AUW_POST_DOC_REVIEW_COMPLETE_FLAG
         , augmented_uw_start_time
         , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', augmented_uw_start_time::date+4)::date+2
               WHEN datediff('day', augmented_uw_start_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               augmented_uw_start_time, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', augmented_uw_start_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', augmented_uw_start_time::date+4)::date+2
               END augmented_uw_start_week_end
        , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', augmented_uw_cl_post_doc_time::date+4)::date+2
               WHEN datediff('day', augmented_uw_cl_post_doc_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               augmented_uw_cl_post_doc_time, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', augmented_uw_cl_post_doc_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', augmented_uw_cl_post_doc_time::date+4)::date+2
               END augmented_uw_cl_post_doc_week_end
         , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', augmented_uw_cl_pre_doc_time::date+4)::date+2
               WHEN datediff('day', augmented_uw_cl_pre_doc_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               augmented_uw_cl_pre_doc_time, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', augmented_uw_cl_pre_doc_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', augmented_uw_cl_pre_doc_time::date+4)::date+2
               END augmented_uw_cl_pre_doc_week_end
         , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', AUW_POST_DOC_REVIEW_COMPLETE_TIME__C::date+4)::date+2
               WHEN datediff('day', AUW_POST_DOC_REVIEW_COMPLETE_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               AUW_POST_DOC_REVIEW_COMPLETE_TIME__C, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', AUW_POST_DOC_REVIEW_COMPLETE_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', AUW_POST_DOC_REVIEW_COMPLETE_TIME__C::date+4)::date+2
               END AUW_POST_DOC_REVIEW_COMPLETE_TIME_WEEKEND
      , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C::date+4)::date+2
               WHEN datediff('day', AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', AUW_PRE_DOC_REVIEW_COMPLETE_TIME__C::date+4)::date+2
               END AUW_PRE_DOC_REVIEW_COMPLETE_TIME_WEEKEND
       , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', AUW_PRE_DOC_REVIEW_START_TIME__C::date+4)::date+2
               WHEN datediff('day', AUW_PRE_DOC_REVIEW_START_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               AUW_PRE_DOC_REVIEW_START_TIME__C, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', AUW_PRE_DOC_REVIEW_START_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', AUW_PRE_DOC_REVIEW_START_TIME__C::date+4)::date+2
               END AUW_PRE_DOC_REVIEW_START_TIME_WEEKEND
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', AUW_POST_DOC_REVIEW_START_TIME__C::date+4)::date+2
               WHEN datediff('day', AUW_POST_DOC_REVIEW_START_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',      
               AUW_POST_DOC_REVIEW_START_TIME__C, current_date()) <= 0 THEN NULL 
               WHEN datediff('day', AUW_POST_DOC_REVIEW_START_TIME__C, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
               ELSE DATE_TRUNC('WEEK', AUW_POST_DOC_REVIEW_START_TIME__C::date+4)::date+2
               END AUW_POST_DOC_REVIEW_START_TIME_WEEKEND
     
        from
            (select *,
                rank()over(partition by fundbox_id__c order by createddate desc) as most_recent_record
                from indus.PUBLIC.SALESFORCE_NOVA__LOAN__C 
                    where recordtypeid= '012Rd000000AcjxIAC' 
                    AND fundbox_id__c IN 
                    	(select fbbid 
                        from cdc_v2.rules_engine.rules_set_results
                        where rules_set_name = 'manual_review_for_inc'
                        and final_status = 'passed'
                        and fbbid <> 1383113
                        order by fbbid)) a
             left join bi.public.customers_data b
             on a.fundbox_id__c=b.fbbid
             left join  indus.PUBLIC.CUSTOMERS_RT_DATA_SALESFORCE c
             on a.fundbox_id__c = c.fbbid
            where most_recent_record=1
            and first_approved_time>='2023-10-24'
            order by 1 desc) AUW
ON CD.fbbid = AUW.fbbid

------------------------------------------------------- SECOND LOOK INC/DEC-------------------------------------------------------

LEFT JOIN 
    (SELECT fundbox_id__c AS fbbid
     , CASE 
        	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', manual_review_start_time__c::date+4)::date+2
        	WHEN datediff('day', manual_review_start_time__c::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
            manual_review_start_time__c::date, current_date()) <= 0 THEN NULL 
        	WHEN datediff('day', manual_review_start_time__c::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
        	ELSE DATE_TRUNC('WEEK', manual_review_start_time__c::date+4)::date+2
        END manual_review_start_week_end_date
     , CASE 
        	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', review_complete_time__c::date+4)::date+2
        	WHEN datediff('day', review_complete_time__c::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
            review_complete_time__c::date, current_date()) <= 0 THEN NULL 
        	WHEN datediff('day', review_complete_time__c::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
        	ELSE DATE_TRUNC('WEEK', review_complete_time__c::date+4)::date+2
        END review_complete_week_end_date
     , CASE 
            WHEN review_complete_time__c IS NOT NULL THEN 1 
            ELSE 0 
        END AS SL_review_complete_FLAG
     , coalesce(approved_uw_credit_limit__c,first_suggested_credit_limit__c) AS SL_TOTAL_EXPOSURE_SL
     , first_suggested_credit_limit__c
     , CASE 
            WHEN (approved_uw_credit_limit__c - first_suggested_credit_limit__c) >0 THEN 1
            ELSE 0
        END AS SL_INC_FLAG  
     , CASE 
            WHEN (approved_uw_credit_limit__c - first_suggested_credit_limit__c) <0 THEN 1
            ELSE 0
        END AS SL_DEC_FLAG 
     , CASE 
            WHEN (approved_uw_credit_limit__c - first_suggested_credit_limit__c) =0 THEN 1
            ELSE 0
        END AS SL_NO_CHANGE_FLAG 
     , (approved_uw_credit_limit__c - first_suggested_credit_limit__c) AS SL_INC
     , (first_suggested_credit_limit__c - approved_uw_credit_limit__c ) AS SL_DEC
    FROM (
    SELECT *,
    RANK() OVER (PARTITION BY fundbox_id__c ORDER BY createddate DESC) AS most_recent_record_SL
    FROM  EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C
    WHERE recordtypeid= '0124T000000DSMTQA4'
    AND  manual_review_start_time__c > '2022-11-1')
    WHERE most_recent_record_SL=1) SL
ON CD.FBBID = SL.FBBID

        -- ------------------------------------------------------- SL Rejections - Automated CL for Exposure Lost -------------------------------------------------------
LEFT JOIN (select fbbid
                , automated_cl AS sl_automated_cl
                , first_rejected_time as SLA_rejected_time
                , first_approved_credit_limit as SLA_first_approved_credit_limit
                , cl_delta
                from analytics.credit.second_look_accounts) SLA
    ON CD.fbbid=SLA.fbbid

------------------------------------------------------- OG AUW -------------------------------------------------------

LEFT JOIN 
    -- (select fbbid 
    --       , date_trunc('week', last_modified_time::date+4)::date+2 AS og_auw_week_end 
    --       , total_limit as og_auw_limit 
    --       ,  reason
    --       , comment
    --       , system_user
    -- FROM cdc_v2.credit.CREDIT_LIMITS
    -- WHERE comment ilike '%AUW OG Review%') OG_AUW
    
    (select a.last_modified_time::date as og_auw_increase_date,
            date_trunc('week', og_auw_increase_date::date+4)::date+2 AS og_auw_week_end, 
           a.fbbid,
           a.total_limit as og_auw_limit,
           b.TOTAL_LIMIt as previous_limit,
           og_auw_limit - previous_limit as incr_amnt,
           a.reason,
           a.comment,
           a.system_user,
    from CDC_V2.credit.CREDIT_LIMITS as a
    left join CDC_V2.credit.CREDIT_LIMITS as b
    on true
    and a.fbbid  = b.fbbid
    and a.prev_credit_limit_id = b.id
    where true
    and a.comment ilike '%AUW OG Review%'
    ) OG_AUW
ON DACD.fbbid=OG_AUW.fbbid
AND DACD.week_end_date=OG_AUW.og_auw_week_end::date
    
------------------------------------------------------- PRE QUAL -------------------------------------------------------

LEFT JOIN 
    (select * from 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY last_modified_time DESC) AS row_num FROM indus."PUBLIC".pre_qual_users_indus a)
    where row_num = 1) pq
    ON CD.fbbid = PQ.fbbid

------------------------------------------------------- REJECT REASONS -------------------------------------------------------

LEFT JOIN 
(SELECT fbbid, rejection_reason
FROM
indus.public.feb_report) FEB
ON CD.fbbid = FEB.fbbid

------------------------------------------------------- FMD -------------------------------------------------------

LEFT JOIN 
    (SELECT 
    CASE
           WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', edate::date+4)::date+2
           WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', edate, current_date()) <= 0 THEN NULL 
           WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
           ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
       END week_end_date
     , fbbid 
     , max(CASE 
    	WHEN dpd_days IS NULL THEN 0 
    	ELSE dpd_days
        END) AS dpd_days_corrected_fmd
     , max(IS_CHARGED_OFF) AS IS_CHARGED_OFF
     , min(CHARGE_OFF_DATE) AS charge_off_date_fmd
     , sum(OUTSTANDING_PRINCIPAL_DUE) AS OUTSTANDING_PRINCIPAL_DUE
     , sum(case when loan_created_week_end_date = week_end_date then originated_amount else 0 end) originated_amount
    FROM 
    (SELECT *  
           , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', loan_created_date::date+4)::date+2
                   WHEN datediff('day', loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                        AND datediff('day', loan_created_date, current_date()) <= 0 THEN NULL 
                   WHEN datediff('day', loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                   ELSE DATE_TRUNC('WEEK', loan_created_date::date+4)::date+2
                   END loan_created_week_end_date
       FROM BI.FINANCE.FINANCE_METRICS_DAILY)
    WHERE PRODUCT_TYPE <> 'Flexpay'
        AND edate=week_end_date
    GROUP BY week_end_date,fbbid) FMD
ON DACD.fbbid=FMD.fbbid
AND DACD.week_end_date=FMD.week_end_date

------------------------------------------------------- INCREASED LINE OFFERS -------------------------------------------------------

LEFT JOIN 
    (select fundbox_id__c as fbbid
          , createddate::date as offer_dt
          , retention_offer__c as offer
          , new_cl_offer__c as goto_cl
          , cl_offer_delta__c as incr_amt
          , offer_status__c as offer_status
          , offer_accepted_date__c as accept_tm
          , OFFERED_NEW_FEE_RATE_12_WEEKS__C as new_12w_pct
          , OFFERED_NEW_FEE_RATE_24_WEEKS__C as new_24w_pct
    
    from indus."PUBLIC".salesforce_nova_offer__c as a
    
    inner join indus."PUBLIC".project_snow_rounds as c
    
    on  a.fundbox_id__c = c.fbbid
    and a.retention_enabled_date__c::date = c.retention_enabled_date
    where  retention_enabled_date__c::date >= '2023-05-01') D
ON DACD.fbbid = D.fbbid

------------------------------------------------------- RMR/UNRMR -------------------------------------------------------
LEFT JOIN 
    (SELECT DISTINCT rmr.fbbid
          , rmr.rmr_segment
          , rmr.flow_type
          , CASE 
            	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', rmr_time_ltz+4)::date+2
            	WHEN datediff('day', rmr_time_ltz, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', rmr_time_ltz, current_date()) <= 0 THEN
                NULL 
            	WHEN datediff('day', rmr_time_ltz, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            	ELSE DATE_TRUNC('WEEK', rmr_time_ltz+4)::date+2
            END rmr_week_end_date 
          , (CASE WHEN rmr.event_type LIKE '%Enter%' THEN dacd.credit_limit ELSE 0 END ) enter_rmr_this_week
          , (CASE WHEN rmr.event_type LIKE '%UnRMR%' AND flow_type NOT LIKE '%close%' THEN dacd.credit_limit ELSE 0 END) unrmr_this_week
          , CASE WHEN (rmr.rmr_segment = '1. Bucket 11+ or FICO < 550' OR
                rmr.rmr_segment = 'FICO < 550' or
                rmr.rmr_segment = 'OG bucket 11+'
                OR rmr.rmr_segment = '1b. OG 11-12' OR
                rmr.rmr_segment = 'OG bucket 13+') THEN '1. OG 11+/FICO < 550'
            WHEN rmr.rmr_segment = '2. MLE > 0.096' then  '2. MLE > 0.096'
            WHEN rmr.rmr_segment ILIKE '%og 9-10 with%'  THEN '3. OG 9-10 with 5+ inq/DQ in 30 days/FICO < 600'
            WHEN rmr.rmr_segment ILIKE '%og 9-10 partner%' THEN '4. OG 9-10 partner'
            WHEN rmr.rmr_segment = 'Vantage < 570' THEN '5. Vantage < 570'
            ELSE 'others' END AS rmr_group
    FROM indus.PUBLIC.RMR_VIEW rmr
    LEFT JOIN 
    BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA DACD
    ON rmr.fbbid = dacd.FBBID 
    AND rmr.rmr_time_ltz = dacd.edate - 1
    -- WHERE rmr.flow_type <> 'control'
    AND dacd.sub_product = 'Line Of Credit') RMRE
ON DACD.fbbid=RMRE.fbbid
AND DACD.week_end_date=RMRE.rmr_week_end_date

------------------------------------------------------- RMR IT IS -------------------------------------------------------

LEFT JOIN 
    (SELECT a.*
          , CASE WHEN (b.rmr_segment = '1. Bucket 11+ or FICO < 550' OR
                b.rmr_segment = 'FICO < 550' or
                b.rmr_segment = 'OG bucket 11+'
                OR b.rmr_segment = '1b. OG 11-12' OR
                b.rmr_segment = 'OG bucket 13+') THEN '1. OG 11+/FICO < 550'
            WHEN b.rmr_segment = '2. MLE > 0.096' then  '2. MLE > 0.096'
            WHEN b.rmr_segment ILIKE '%og 9-10 with%'  THEN '3. OG 9-10 with 5+ inq/DQ in 30 days/FICO < 600'
            WHEN b.rmr_segment ILIKE '%og 9-10 partner%' THEN '4. OG 9-10 partner'
            WHEN b.rmr_segment = 'Vantage < 570' THEN '5. Vantage < 570'
            ELSE 'others' END AS rmr_group, rmr_time_ltz
    from
    (SELECT fbbid,to_date(CREATED_TIME) lock_start,
    convert_timezone('UTC','America/Los_Angeles',lock_start::date)::date lock_start_ltz,
    IFNULL(to_date(LOCK_RELEASE_TIME),current_date()+10) lock_end 
    FROM indus."PUBLIC".credit_statuses_locks
    WHERE credit_lock_context ILIKE '%rmr%'
    )a
    LEFT join
    (SELECT DATA:tag_option::varchar flow_Type,
    DATA:segment::varchar rmr_segment,
    DATA:event_type::varchar event_type,
    DATA:fbbid AS fbbid,
    DATA:event_time::timestamp rmr_time,
    convert_timezone('UTC','America/Los_Angeles',DATA:event_time::date)::date rmr_time_ltz,
    DATA:comment::varchar AS comment
    FROM CDC_V2.RISK_HIST.EVENTS_OUTBOX eo
    WHERE DATA:entity_type = 'RMR_FLOW'
    AND DATA:event_type = 'RiskEnterRMREvent'
    qualify row_number() over(partition by fbbid order BY rmr_time  desc) = 1
    )b
    ON a.fbbid = b.fbbid) R
ON DACD.fbbid=R.fbbid
AND DACD.week_end_date >= R.lock_start AND DACD.week_end_date <= (R.lock_end)

------------------------------------------------------- DISABLED PAYOFF -------------------------------------------------------

LEFT JOIN
    (SELECT fbbid
          , lock_end
          , min(lock_start) lock_start
    FROM
        (SELECT fbbid,date_trunc('week', CREATED_TIME::date+4)::date-4 AS week_start
              , min(to_date(CREATED_TIME)) lock_start
              , max(IFNULL(to_date(LOCK_RELEASE_TIME),current_date()+10)) lock_end
    FROM indus."PUBLIC".credit_statuses_locks
    WHERE LOCK_REASON ILIKE '%payoff%' AND product_type = 'DDR'
    GROUP BY 1,2
    )
    GROUP BY 1,2
    ) G
ON DACD.fbbid = G.fbbid AND DACD.week_end_date >= G.lock_start AND DACD.week_end_date <= G.lock_end

------------------------------------------------------- PAYMENTS DATA -------------------------------------------------------

LEFT JOIN 
    (SELECT FBBID, 
		CASE 
            WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', EDATE::date +4)::date+2
            WHEN datediff('day', EDATE::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', EDATE::date , current_date()) <= 0 THEN NULL 
            WHEN datediff('day', EDATE::date , DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
            ELSE DATE_TRUNC('WEEK', EDATE::date +4)::date+2
            END week_end_date,
        MAX(IS_AFTER_CO) AS IS_AFTER_CHARGEOFF,
		SUM(TOTAL_PAID) AS TOTAL_PAID,
        SUM(FEES_PAID) AS FEES_PAID,
		SUM(PRINCIPAL_PAID) AS PRINCIPAL_PAID
FROM
(SELECT FBBID
      , EDATE
      , LOAN_KEY
      , STATUS_VALUE:PAYMENT_AMOUNT::FLOAT AS TOTAL_PAID
      , STATUS_VALUE:FEES_AMOUNT::FLOAT AS FEES_PAID
      , (TOTAL_PAID - FEES_PAID) AS PRINCIPAL_PAID
      , STATUS_VALUE:IS_AFTER_CO::INT AS IS_AFTER_CO
FROM  bi.FINANCE.LOAN_STATUSES
WHERE STATUS_NAME = 'GOOD_DEBIT_PAYMENT'
AND LOAN_KEY > 0)
WHERE WEEK_END_DATE>='2020-12-31'
GROUP BY 1,2
ORDER BY 1,2) C
ON DACD.fbbid=C.fbbid
AND DACD.week_end_date=C.week_end_date

LEFT JOIN 
    (
SELECT
    FBBID,
    week_end_date,
    MAX(IS_AFTER_CO) AS IS_AFTER_CHARGEOFF,
    SUM(TOTAL_PAID) OVER (PARTITION BY FBBID ORDER BY week_end_date) AS CUMULATIVE_TOTAL_PAID,
    SUM(FEES_PAID) OVER (PARTITION BY FBBID ORDER BY week_end_date) AS CUMULATIVE_FEES_PAID,
    SUM(PRINCIPAL_PAID) OVER (PARTITION BY FBBID ORDER BY week_end_date) AS CUMULATIVE_PRINCIPAL_PAID
FROM (
    SELECT
        FBBID,
        CASE
            WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', EDATE::date + 4)::date + 2
            WHEN datediff('day', EDATE::date, DATE_TRUNC('WEEK', current_date() + 4)::date - 5) < 0 AND datediff('day', EDATE::date, current_date()) <= 0 THEN NULL
            WHEN datediff('day', EDATE::date, DATE_TRUNC('WEEK', current_date() + 4)::date - 5) < 0 THEN current_date() - 1
            ELSE DATE_TRUNC('WEEK', EDATE::date + 4)::date + 2
        END AS week_end_date,
        IS_AFTER_CO,
        TOTAL_PAID,
        FEES_PAID,
        PRINCIPAL_PAID
    FROM
(SELECT FBBID
      , EDATE
      , LOAN_KEY
      , STATUS_VALUE:PAYMENT_AMOUNT::FLOAT AS TOTAL_PAID
      , STATUS_VALUE:FEES_AMOUNT::FLOAT AS FEES_PAID
      , (TOTAL_PAID - FEES_PAID) AS PRINCIPAL_PAID
      , STATUS_VALUE:IS_AFTER_CO::INT AS IS_AFTER_CO
FROM  bi.FINANCE.LOAN_STATUSES
WHERE STATUS_NAME = 'GOOD_DEBIT_PAYMENT'
AND LOAN_KEY > 0)
WHERE WEEK_END_DATE>='2020-12-31'
GROUP BY 1,2
ORDER BY 1,2
) AS subquery
GROUP BY
    FBBID, week_end_date
ORDER BY
    FBBID, week_end_date )rev_run
ON DACD.fbbid=rev_run.fbbid
AND DACD.week_end_date=rev_run.week_end_date

WHERE DACD.WEEK_END_DATE>='2020-12-31'
OR CD.registration_time>='2013-08-30'
ORDER BY CD.FBBID, DACD.WEEK_END_DATE
)
;