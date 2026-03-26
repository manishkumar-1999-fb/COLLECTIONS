create or replace view INDUS.PUBLIC.V_INDUS_KEY_METRICS_LO_DAILY_AGG(
	CURR_DATE,
	CHANNEL,
	PARTNER,
	NEW_CUST_FILTER,
	BUCKET_GROUP,
	LENDIO_FLOW,
	NATIONAL_FUNDING_FLOW,
	NUM_LOANS,
	ORIG,
	ORIG_FICO_NOT_NULL,
	ORIG_BUCKET_NOT_NULL,
	FICO_ORIG_PROD,
	BUCKET_ORIG_PROD
) as 


WITH lo AS (

SELECT dacd.fbbid

, dacd.lt_acquisition_channel

, f.channel
, f.partner
, f.lendio_flow
, f.national_funding_flow
, f.new_cust_filter

, CASE 
	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_dal_bucket
	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_risk_bucket
END risk_bucket

,CASE 
    WHEN ld.loan_created_date = f.MODEL_RUN_START_TIME::DATE 
         AND fdl.loan_created_time::TIMESTAMP < f.MODEL_RUN_START_TIME 
    THEN 
        CASE  
            WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_group_dal  
            WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_group_prev  
        END  
    ELSE  
        CASE  
            WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_group_dal  
            WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_group 
        END  
END AS bucket_group

/*
, CASE 
	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_risk_bucket_approved
	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_risk_bucket
END risk_bucket

, CASE 
	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_group_approved
	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_group
END bucket_group
*/
--, rmr.event_type rmr_event_type
--
--, CASE 
--	WHEN rmr_event_type IN ('RiskRMRReviewEvent', 'RiskEnterRMREvent') THEN 1
--	ELSE 0
--END AS rmr_flag

-- REGISTRATION TIME
, dacd.registration_time
, dacd.registration_time::date AS registration_date

-- APPROVAL TIME
, dacd.first_approved_time
, dacd.first_approved_time::date AS first_approved_date

-----26--mar--------
-- LOAN DATA
, ld.loan_key
--, ld.loan_status_v2 AS loan_status

--------26-mar--------
-- CREATED TIME
--, ld.loan_created_time
--, ld.loan_created_time::date AS loan_created_date
, ld.loan_created_date

, ld.originated_amount
, COALESCE (dacd.risk_review_fico_score, dacd.fico_onboarding) AS fico_at_draw
, fico_at_draw * ld.originated_amount AS fico_orig_product
--, ld.outstanding_principal
--, ld.default_principal
--, ld.outstanding_principal - ld.default_principal good_os
--, ld.delinquent_principal
--, ld.CHARGEOFF_PRINCIPAL_DPD_90
, ld.CREDITED_AMOUNT
--, ld.FIRST_ORIGINAL_PLANNED_DEBIT_DATE
--, ld.LAST_ACTIVE_PLANNED_DEBIT_DATE
--, ld.FIRST_GOOD_DEBIT_DATE
--, ld.LAST_GOOD_DEBIT_DATE
--, ld.LAST_FAILED_DEBIT_DATE
--, ld.LAST_ORIGINAL_PAYMENT_DATE


--FROM bi.FINANCE.LOAN_DATA ld 
-- FROM indus."PUBLIC".loan_data_indus ld ---- 26 MARCH CHANGE
FROM indus.PUBLIC.Loan_data_indus_daily_new ld

LEFT JOIN BI.FINANCE.DIM_LOAN fdl
ON ld.loan_key = fdl.loan_key

--LEFT JOIN bi."PUBLIC".DAILY_APPROVED_CUSTOMERS_DATA dacd 
LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd 


ON ld.fbbid = dacd.fbbid 
AND ld.loan_created_date = dacd.edate 

--LEFT JOIN ANALYTICS.CREDIT.eg_key_metrics_filters f 
LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f 

ON ld.fbbid = f.fbbid
AND ld.loan_created_date = f.edate 

WHERE TRUE 
AND ld.loan_key IS NOT NULL 
AND dacd.fbbid IS NOT NULL 
AND dacd.is_test_user = 0
-- AND ld.loan_status_v2 <> 'CNCL'
AND (loan_operational_status <> 'CNCL' OR loan_operational_status IS NULL)
-----------------------------31st May ---------------------
AND dacd.sub_product <> 'Credit Builder'
AND dacd.sub_product <> 'mca'
----------------------------------------------------------
)


SELECT loan_created_date curr_date
, channel
, partner
, new_cust_filter
, bucket_group
, lendio_flow
, national_funding_flow
, count(loan_key) num_loans
, sum(ORIGINATED_AMOUNT) orig 
, sum(CASE WHEN FICO_AT_DRAW IS NULL THEN 0 ELSE ORIGINATED_AMOUNT END) orig_fico_not_null
, sum(CASE WHEN risk_bucket IS NULL THEN 0 ELSE originated_amount END) orig_bucket_not_null
, sum(originated_amount*fico_at_draw) fico_orig_prod
, sum(originated_amount*risk_bucket) bucket_orig_prod

FROM lo 
WHERE curr_date BETWEEN DATEADD('day',-40,current_date()) AND current_date()
GROUP BY 1,2,3,4,5,6,7
;