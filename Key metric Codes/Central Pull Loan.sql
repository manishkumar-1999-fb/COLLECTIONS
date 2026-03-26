--USE WAREHOUSE key_metrics_wh


CREATE OR REPLACE TABLE analytics.credit.loan_level_data_pb AS 
(WITH first_table AS (
SELECT loan_key
     , loan_operational_status
     , ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate desc) rnk
FROM BI.FINANCE.FINANCE_METRICS_DAILY
QUALIFY rnk = 1
),
FINANCE_METRICS AS (
    SELECT fmd.*
    	 , CASE 
	WHEN is_charged_off = 1 AND DPD_days IS NULL THEN 98
	WHEN is_charged_off = 0 AND DPD_days IS NULL THEN 0 
	ELSE dpd_days 

	
END AS dpd_days_corrected
         , CASE WHEN ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%Term Loan%' THEN 1 ELSE 0 END AS is_term_loan
         , CASE WHEN ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%12%' THEN '12 Week'
				WHEN ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%24%' THEN '24 Week'
				WHEN ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%52%' THEN '52 Week'
				ELSE 'Others'
  				END payment_plan
         , DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS week_end_date
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft
    ON fmd.loan_key = ft.loan_key
    WHERE PRODUCT_TYPE <> 'Flexpay'
    AND DAYOFWEEK(edate) = 3 
    AND edate >= '2020-12-30'
    AND IS_TERM_LOAN = 0
    AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
),
---
WEEKLY_METRICS AS (
    SELECT fbbid
         , loan_key
         , week_end_date
         , DATEADD(DAY, -6, week_end_date) AS week_start_date
         , IS_CHARGED_OFF
         , OUTSTANDING_PRINCIPAL_DUE
         , dpd_days_corrected
         , loan_created_date
         , charge_off_date
         , credited_amount
         , originated_amount
         , is_term_loan
         , payment_plan
         , first_planned_transmission_date
         , ifnull(LAG(dpd_days_corrected) OVER (PARTITION BY loan_key ORDER BY week_end_date),0) AS lag_dpd_days_corrected
         , ifnull(LAG(IS_CHARGED_OFF) OVER (PARTITION BY loan_key ORDER BY week_end_date),0) AS lag_is_charged_off
         , ifnull(LAG(OUTSTANDING_PRINCIPAL_DUE) OVER (PARTITION BY loan_key ORDER BY week_end_date),0) AS lag_outstanding_principal_due
    FROM FINANCE_METRICS
),
BUCKET_CALCULATIONS AS (
    SELECT *
     , CASE WHEN IS_CHARGED_OFF=0 AND dpd_days_corrected=0 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_0
	 , CASE WHEN lag_IS_CHARGED_OFF=0 AND lag_dpd_days_corrected=0 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_0
  	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 1 AND 7 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_1_7
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 1 AND 7 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_p_1_7
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 8 AND 14 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_8_14
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 8 AND 14 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_8_14
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 15 AND 21 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_15_21
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 15 AND 21 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_15_21
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 22 AND 28 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_22_28
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 22 AND 28 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_22_28
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 29 AND 35 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_29_35
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 29 AND 35 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_29_35
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 36 AND 42 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_36_42
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 36 AND 42 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_36_42
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 43 AND 49 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_43_49
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 43 AND 49 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_43_49
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 50 AND 56 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_50_56
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 50 AND 56 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_50_56
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 57 AND 63 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_57_63
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 57 AND 63 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_57_63
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 64 AND 70 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_64_70
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 64 AND 70 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_64_70
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 71 AND 77 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_71_77
 	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 71 AND 77 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_71_77
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 78 AND 84 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_78_84
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 78 AND 84 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_78_84
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 85 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_85_90
	 , CASE WHEN dpd_days_corrected  BETWEEN 85 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_85_91
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 15 AND 35 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_15_35
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 36 AND 63 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_36_63
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 64 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_64_90
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 64 AND 91 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_64_90
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 84 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_84_90
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 85 AND 91 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_85_90
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 84 AND 91 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_84_90
	 , CASE WHEN IS_CHARGED_OFF = 1 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_91
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 0 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_0_90
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 0 AND 91 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_0_90
	 , CASE WHEN lag_IS_CHARGED_OFF =0 AND lag_dpd_days_corrected  BETWEEN 1 AND 91 THEN lag_OUTSTANDING_PRINCIPAL_DUE ELSE 0 END AS os_p_1_90
	 , CASE WHEN IS_CHARGED_OFF =0 AND dpd_days_corrected  BETWEEN 1 AND 91 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_1_90
	 , lag_is_charged_off AS P_is_charged_off
	 , CASE WHEN IS_CHARGED_OFF = 1 AND P_is_charged_off = 0 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_91_new
	 , CASE WHEN (IS_CHARGED_OFF = 1) AND os_p_64_90 > 0 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_91_from_64_90
	 , CASE WHEN (IS_CHARGED_OFF = 1) AND os_p_85_90 > 0 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END os_91_from_85_90
	 , CASE WHEN os_0 = 0 THEN NULL ELSE loan_key END f_0
	 , CASE WHEN os_p_0 = 0 THEN NULL ELSE loan_key END f_p_0
	 , CASE WHEN os_1_7 = 0 THEN NULL ELSE loan_key END f_1_7
	 , CASE WHEN os_p_1_7 = 0 THEN NULL ELSE loan_key END f_p_1_7
	 , CASE WHEN os_8_14 = 0 THEN NULL ELSE loan_key END f_8_14
	 , CASE WHEN os_p_8_14 = 0 THEN NULL ELSE loan_key END f_p_8_14
	 , CASE WHEN os_15_21 = 0 THEN NULL ELSE loan_key END f_15_21
	 , CASE WHEN os_p_15_21 = 0 THEN NULL ELSE loan_key END f_p_15_21
	 , CASE WHEN os_22_28 = 0 THEN NULL ELSE loan_key END f_22_28
	 , CASE WHEN os_p_22_28 = 0 THEN NULL ELSE loan_key END f_p_22_28
	 , CASE WHEN os_29_35 = 0 THEN NULL ELSE loan_key END f_29_35
	 , CASE WHEN os_p_29_35 = 0 THEN NULL ELSE loan_key END f_p_29_35
	 , CASE WHEN os_36_42 = 0 THEN NULL ELSE loan_key END f_36_42
	 , CASE WHEN os_p_36_42 = 0 THEN NULL ELSE loan_key END f_p_36_42
	 , CASE WHEN os_43_49 = 0 THEN NULL ELSE loan_key END f_43_49
	 , CASE WHEN os_p_43_49 = 0 THEN NULL ELSE loan_key END f_p_43_49
	 , CASE WHEN os_50_56 = 0 THEN NULL ELSE loan_key END f_50_56
	 , CASE WHEN os_p_50_56 = 0 THEN NULL ELSE loan_key END f_p_50_56
	 , CASE WHEN os_57_63 = 0 THEN NULL ELSE loan_key END f_57_63
	 , CASE WHEN os_p_57_63 = 0 THEN NULL ELSE loan_key END f_p_57_63
	 , CASE WHEN os_64_70 = 0 THEN NULL ELSE loan_key END f_64_70
	 , CASE WHEN os_p_64_70 = 0 THEN NULL ELSE loan_key END f_p_64_70
	 , CASE WHEN os_71_77 = 0 THEN NULL ELSE loan_key END f_71_77
	 , CASE WHEN os_p_71_77 = 0 THEN NULL ELSE loan_key END f_p_71_77
	 , CASE WHEN os_78_84 = 0 THEN NULL ELSE loan_key END f_78_84
	 , CASE WHEN os_p_78_84 = 0 THEN NULL ELSE loan_key END f_p_78_84
	 , CASE WHEN os_85_90 = 0 THEN NULL ELSE loan_key END f_85_90
	 , CASE WHEN os_85_91 = 0 THEN NULL ELSE loan_key END f_85_91
	 , CASE WHEN os_15_35 = 0 THEN NULL ELSE loan_key END f_15_35
	 , CASE WHEN os_36_63 = 0 THEN NULL ELSE loan_key END f_36_63
	 , CASE WHEN os_64_90 = 0 THEN NULL ELSE loan_key END f_64_90
	 , CASE WHEN os_84_90 = 0 THEN NULL ELSE loan_key END f_84_90
	 , CASE WHEN os_p_84_90 = 0 THEN NULL ELSE loan_key END f_p_84_90
	 , CASE WHEN os_p_85_90 = 0 THEN NULL ELSE loan_key END f_p_85_90
	 , CASE WHEN os_91 = 0 THEN NULL ELSE loan_key END f_91
	 , CASE WHEN os_91_new = 0 THEN NULL ELSE loan_key END f_91_new
	 , CASE WHEN os_P_64_90 = 0 THEN NULL ELSE loan_key END f_P_64_90
	 , CASE WHEN os_91_from_64_90 = 0 THEN NULL ELSE loan_key END f_91_from_64_90
	 , CASE WHEN os_91_from_85_90 = 0 THEN NULL ELSE loan_key END f_91_from_85_90
	 , CASE WHEN os_0_90 = 0 THEN NULL ELSE loan_key END f_0_90
	 , CASE WHEN os_1_90 = 0 THEN NULL ELSE loan_key END f_1_90
	---
    FROM WEEKLY_METRICS
)
SELECT a.* 
     , pay.total_paid
     , pay.fees_paid
     , pay.principal_paid
     , pay.is_after_co
     , f.sub_product
     , f.new_cust_filter
     , f.industry_type
     , f.termunits
     , f.partner
     , f.channel
     , f.intuit_flow
     , f.nav_flow
     , f.national_funding_flow
	 , f.evercommerce_flow
     , f.lendio_flow
     , f.FICO
     , f.VANTAGE4
--	 , f.customer_annual_revenue
	 /*, CASE WHEN f.customer_annual_revenue>0 and f.customer_annual_revenue<=150000 THEN '$0 - $150K'
           WHEN f.customer_annual_revenue>150000 and f.customer_annual_revenue<=500000 THEN '$150K - $500K'
           WHEN f.customer_annual_revenue>500000 and f.customer_annual_revenue<=1000000 THEN '$500K - $1M'
           WHEN f.customer_annual_revenue>1000000 THEN '> $1M'
           ELSE 'Other/No Data'
           END AS customer_annual_revenue_group*/
	 , CASE WHEN f.customer_annual_revenue>0 and f.customer_annual_revenue<=500000 THEN '$0 - $500K'
           WHEN f.customer_annual_revenue>500000 and f.customer_annual_revenue<=1500000 THEN '$500K - $1.5M'
           WHEN f.customer_annual_revenue>1500000 THEN '> $1.5M'
--           WHEN customer_annual_revenue>1000000 THEN '> $1M'
           ELSE 'Other/No Data'
        END AS customer_annual_revenue_group
	 ---25 MAR'25 ADDITION
	/*, CASE WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) < 60 THEN '1-60'
		   WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) < 120 THEN '60-120'
		   WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) < 180 THEN '120-180'
		   WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) < 240 THEN '180-240'
		   WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) < 360 THEN '240-360'
		   WHEN DATEDIFF('day',F.APP_DATE,A.LOAN_CREATED_DATE) >= 360 THEN '360+'
		   WHEN A.LOAN_CREATED_DATE IS NULL THEN 'OTHERS'
		   END Loan_TENURE_BUCKET*/

	, CASE
	    WHEN DATEDIFF('day', F.APP_DATE,A.LOAN_CREATED_DATE) < 60 THEN '1-60'
	    WHEN DATEDIFF('day', F.APP_DATE,A.LOAN_CREATED_DATE) < 180 THEN '60-180'
	    WHEN DATEDIFF('day', F.APP_DATE,A.LOAN_CREATED_DATE) < 365 THEN '180-365'
	    WHEN DATEDIFF('year', F.APP_DATE,A.LOAN_CREATED_DATE) < 3 THEN '1-3 years'
	    WHEN DATEDIFF('year', F.APP_DATE,A.LOAN_CREATED_DATE) < 5 THEN '3-5 years'
	    ELSE '5+ years'
	END Loan_TENURE_BUCKET

     , case when cl_delta>0 then 'SL Increase'
            when cl_delta<0 then 'SL Decrease'
            when b.fbbid is not null then 'SL Other'
            when auw_pre_doc_review_status__c ilike '%Complete - Increase%'  then 'Low Risk Increase'
            when a.fbbid in (1386479,1387045,1388962, 1389231) then 'Low Risk Increase'
            when c.fbbid is not null then 'Low Risk No Change'
            else 'Non AUW' end as auw_segment
     , CASE WHEN auw_segment = 'Non AUW' THEN 'Automated' ELSE 'Manual' END AS is_auw
     , CASE WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_group_retro
			WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_group
			END bucket_group
	 , CASE WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_retro
			WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_retro
			END risk_bucket
--				
FROM BUCKET_CALCULATIONS a
-----
LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2  f 
ON a.fbbid = f.fbbid
AND a.LOAN_CREATED_DATE::date = f.edate 
-----
LEFT JOIN 
analytics.credit.second_look_accounts b
on a.fbbid =b.fbbid
--
LEFT JOIN analytics.credit.auw_increase_accts c
on a.fbbid =c.fbbid 
-----
LEFT JOIN 
(
SELECT FBBID
      , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', edate::date+4)::date+2
			 WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', edate, current_date()) <= 0 THEN NULL 
	  		 WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
			 ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
			 END week_end_date
      , LOAN_KEY
      , sum(STATUS_VALUE:PAYMENT_AMOUNT::FLOAT) AS TOTAL_PAID
      , sum(STATUS_VALUE:FEES_AMOUNT::FLOAT) AS FEES_PAID
      , (TOTAL_PAID - FEES_PAID) AS PRINCIPAL_PAID
      , max(STATUS_VALUE:IS_AFTER_CO::INT) AS IS_AFTER_CO
FROM  bi.FINANCE.LOAN_STATUSES
WHERE STATUS_NAME = 'GOOD_DEBIT_PAYMENT'
AND LOAN_KEY > 0
GROUP BY 1,2,3
) PAY
ON a.fbbid = pay.fbbid
AND a.loan_key =  pay.loan_key
AND a.week_end_date = pay.week_end_date
WHERE f.is_test = 0
AND f.sub_product <> 'Credit Builder'
--AND f.sub_product <> 'mca'
)
--
;





