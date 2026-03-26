CREATE OR REPLACE TABLE indus.PUBLIC.Loan_QM_data_QM_DPD AS 
(
SELECT 
A.loan_key,
 CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', loan_created_date+4)::date+2
	WHEN datediff('day', loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', loan_created_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', loan_created_date::date+4)::date+2
END loan_created_week_end_date,
fbbid ,
LOAN_CREATED_DATE,
LOAN_CREDITED_DATE,
LOAN_OPERATIONAL_STATUS,
ORIGINATED_AMOUNT,
FIRST_PLANNED_TRANSMISSION_DATE,
--FIRST_GOOD_DEBIT_PAYMENT ,
edate ,
OUTSTANDING_PRINCIPAL_DUE, 
OUTSTANDING_PRINCIPAL_FUNDED, 
CASE 
	WHEN is_charged_off = 1 AND DPD_days IS NULL THEN 98
	WHEN is_charged_off = 0 AND DPD_days IS NULL THEN 0 
	ELSE dpd_days 
END
DPD_DAYS,
CHARGEOFF_PRINCIPAL,
CREDITED_AMOUNT ,
IS_CHARGED_OFF,
--ROW_NUMBER () over( PARTITION BY loan_key ORDER BY edate DESC ) rnk 
-- Edit this line as it is taking a lot of time.
FROM (SELECT 
loan_key,
max(edate) AS m_edate 
FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE 
PRODUCT_TYPE <> 'Flexpay'
GROUP BY 1) A
INNER JOIN
(
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) B
ON A.loan_key = B.loan_key AND A.m_edate = B.edate
ORDER BY A.loan_key
);

select max(edate) from indus.PUBLIC.Loan_QM_data_QM_DPD;


CREATE OR REPLACE TABLE INDUS.PUBLIC.qm_test1 AS
(
WITH lo AS (
select ld.loan_key,
ld.LOAN_CREATED_DATE,
--ld.CHARGE_OFF_DATE,
ld.OUTSTANDING_PRINCIPAL_DUE,
ld.ORIGINATED_AMOUNT
,ld.LOAN_OPERATIONAL_STATUS AS loan_status
,f.new_cust_filter
,date_trunc('week', ld.loan_created_date + 4)::date - 4 loan_created_week
,CASE 
	WHEN ld.is_charged_off = 1 AND ld.DPD_days IS NULL THEN 98
	WHEN ld.is_charged_off = 0 AND ld.DPD_days IS NULL THEN 0 
	ELSE ld.dpd_days 
END DPD_DAYS_corrected

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', ld.loan_created_date+4)::date+2
	WHEN datediff('day', ld.loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', ld.loan_created_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', ld.loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', ld.loan_created_date::date+4)::date+2
END loan_created_week_end_date

, wob6.edate wob6_edate
, wob6.outstanding_principal_due wob6_newos
, wob6.dpd_days AS wob6_dpd
, wob6.is_charged_off wob6_charge_off

FROM indus.PUBLIC.Loan_QM_data_QM_DPD ld 

LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f 
ON ld.fbbid = f.fbbid
AND ld.loan_created_date::date = f.edate 

LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) wob6
ON ld.loan_key = wob6.LOAN_KEY 
AND DATEADD('WEEK' , 6, ld.LOAN_CREATED_DATE) = wob6.edate

WHERE TRUE 
AND ld.loan_key IS NOT NULL 
AND (ld.loan_operational_status <> 'CNCL' OR ld.loan_operational_status IS NULL)
AND f.sub_product <> 'Credit Builder'
--AND WOB6.PRODUCT_TYPE <> 'Flexpay'
)

SELECT *

FROM(
SELECT loan_created_week_end_date week_end_date
, loan_key
, new_cust_filter
, sum(ORIGINATED_AMOUNT) orig 
, sum(CASE WHEN wob6_dpd > 7 and wob6_charge_off = 0 THEN wob6_newos ELSE 0 END) wob6_os
from lo 
WHERE week_end_date <= current_timestamp()
GROUP BY 1,2,3));

/*SELECT WEEK_END_DATE, SUM(WOB6_OS) FROM INDUS.PUBLIC.qm_test1 WHERE WEEK_END_DATE >= '2021-01-13'  GROUP BY 1 ORDER BY 1;
SELECT WEEK_END_DATE, LOAN_KEY , SUM(ORIG) FROM INDUS.PUBLIC.qm_test1 WHERE WEEK_END_DATE = '2022-05-04' GROUP BY 1,2 ORDER BY 2;
SELECT  SUM(orig) FROM INDUS.PUBLIC.qm_test1 WHERE WEEK_END_DATE = '2024-12-25' ;
SELECT  SUM(orig) FROM INDUS.PUBLIC.qm_test1 WHERE WEEK_END_DATE = '2021-01-13' ;
SELECT  SUM(wob6_os) FROM INDUS.PUBLIC.qm_test1 WHERE WEEK_END_DATE = '2021-01-13' ;
SELECT  max(week_end_date) FROM INDUS.PUBLIC.qm_test1; WHERE WEEK_END_DATE = '2024-12-25' ;
select * from INDUS.PUBLIC.qm_test1 where loan_key = 1440681;*/





--------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------LOAN_ORIGINATION_INDUS----------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------

--SELECT top 10*
--FROM indus.PUBLIC.INDUS_KEY_METRICS_LOAN_ORIGINATIONS_AGG_test_1;

--DROP TABLE IF EXISTS indus.PUBLIC.Loan_data_NEW_DPD_TRIAL;

-------------------------last updated on 22nd may -----------------
--USE WAREHOUSE analytics_wh;

CREATE OR REPLACE TABLE indus.PUBLIC.Loan_data_NEW_DPD AS 
(
SELECT 
A.loan_key,
fbbid ,
LOAN_CREATED_DATE,
--LOAN_CREDITED_DATE,
LOAN_OPERATIONAL_STATUS,
ORIGINATED_AMOUNT,
FIRST_PLANNED_TRANSMISSION_DATE,
FIRST_GOOD_DEBIT_PAYMENT_DATE ,
ORIGINAL_PAYMENT_PLAN_DESCRIPTION,
edate ,
OUTSTANDING_PRINCIPAL_DUE, 
OUTSTANDING_PRINCIPAL_FUNDED, 
CASE 
	WHEN is_charged_off = 1 AND DPD_days IS NULL THEN 98
	WHEN is_charged_off = 0 AND DPD_days IS NULL THEN 0 
	ELSE dpd_days 
END
DPD_DAYS,
CHARGEOFF_PRINCIPAL,
CREDITED_AMOUNT ,
IS_CHARGED_OFF,
--ROW_NUMBER () over( PARTITION BY loan_key ORDER BY edate DESC ) rnk 
-- Edit this line as it is taking a lot of time.
FROM (SELECT loan_key,max(edate) AS m_edate FROM (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) GROUP BY 1) A
INNER JOIN
(
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) B
ON A.loan_key = B.loan_key AND A.m_edate = B.edate
ORDER BY A.loan_key
);

--SELECT * FROM indus.PUBLIC.Loan_data_NEW_DPD WHERE LOAN_KEY LIKE 1552466;
--SELECT * FROM indus.PUBLIC.Loan_data_NEW_DPD_TRIAL WHERE LOAN_KEY LIKE 1552466;
--
--SELECT LOAN_KEY,FBBID,LOAN_CREATED_DATE,LOAN_CREDITED_DATE,LOAN_OPERATIONAL_STATUS,ORIGINATED_AMOUNT,FIRST_PLANNED_TRANSMISSION_DATE,FIRST_GOOD_DEBIT_PAYMENT,EDATE,OUTSTANDING_PRINCIPAL_DUE,OUTSTANDING_PRINCIPAL_FUNDED,DPD_DAYS,CHARGEOFF_PRINCIPAL,CREDITED_AMOUNT,IS_CHARGED_OFF
--FROM indus.PUBLIC.Loan_data_NEW_DPD
--EXCEPT
--SELECT LOAN_KEY,FBBID,LOAN_CREATED_DATE,LOAN_CREDITED_DATE,LOAN_OPERATIONAL_STATUS,ORIGINATED_AMOUNT,FIRST_PLANNED_TRANSMISSION_DATE,FIRST_GOOD_DEBIT_PAYMENT,EDATE,OUTSTANDING_PRINCIPAL_DUE,OUTSTANDING_PRINCIPAL_FUNDED,DPD_DAYS,CHARGEOFF_PRINCIPAL,CREDITED_AMOUNT,IS_CHARGED_OFF
--FROM indus.PUBLIC.Loan_data_NEW_DPD_TRIAL;

--SELECT count(loan_key),count(DISTINCT loan_key) 




CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_LOAN_ORIGINATIONS_AGG_test_1 AS ( 
WITH lo AS (
SELECT dacd.fbbid
--, dacd.lt_acquisition_channel
, f.channel
, f.partner
, f.TERMUNITS
, f.lendio_flow
, f.national_funding_flow
, f.new_cust_filter
----------11th march dal bucket change----------------
--, CASE 
--	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_risk_bucket_approved
--	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_risk_bucket
--END risk_bucket
, CASE 
	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_dal_bucket
	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_risk_bucket
END risk_bucket
-------------------------------------------------------------------------
/*
, CASE 
	WHEN f.new_cust_filter = 'New Customer' THEN f.ob_bucket_group_approved
	WHEN f.new_cust_filter = 'Existing Customer' THEN f.og_bucket_group
END bucket_group
*/
--- 7 Feb change for DAL OB Bucket
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
END AS bucket_group,
rmr.event_type rmr_event_type, 

CASE 
	WHEN rmr_event_type IN ('RiskRMRReviewEvent', 'RiskEnterRMREvent') THEN 1
	ELSE 0
END AS rmr_flag,

CASE 
	WHEN ld.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%12%' THEN '12 Week'
	WHEN ld.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%24%' THEN '24 Week'
	WHEN ld.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%52%' THEN '52 Week'
	ELSE 'Others'
  END payment_plan,

-- REGISTRATION TIME
 dacd.registration_time
, dacd.registration_time::date AS registration_date
, date_trunc('week', registration_date + 4)::date - 4 registration_week,

CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_date+4)::date+2
	WHEN datediff('day', registration_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', registration_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', registration_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', registration_date::date+4)::date+2
END registration_week_end_date, 
left(registration_date,7) AS registration_month
, CONCAT(year(registration_date)::varchar(), CASE WHEN month(registration_date) IN (1,2,3) THEN '_Q1' WHEN month(registration_date) IN (4,5,6) THEN '_Q2' WHEN month(registration_date) IN (7,8,9) THEN '_Q3' WHEN month(registration_date) IN (10,11,12) THEN '_Q4' END) AS registration_quarter

-- APPROVAL TIME
, dacd.first_approved_time
, dacd.first_approved_time::date AS first_approved_date
, date_trunc('week', first_approved_date + 4)::date - 4 first_approved_week
, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_date+4)::date+2
	WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_approved_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', first_approved_date::date+4)::date+2
END first_approved_week_end_date
, left(first_approved_date,7) AS first_approved_month
, CONCAT(year(first_approved_date)::varchar(), CASE WHEN month(first_approved_date) IN (1,2,3) THEN '_Q1' WHEN month(first_approved_date) IN (4,5,6) THEN '_Q2' WHEN month(first_approved_date) IN (7,8,9) THEN '_Q3' WHEN month(first_approved_date) IN (10,11,12) THEN '_Q4' END) AS first_approved_quarter

-- LOAN DATA
, ld.loan_key
,ld.is_charged_off
, ld.LOAN_OPERATIONAL_STATUS AS loan_status
-- CREATED TIME
--, ld.LOAN_CREATED_DATE
, fdl.LOAN_CREATED_TIME::TIMESTAMP as loan_created_time
, ld.LOAN_CREATED_DATE::date AS loan_created_date
, date_trunc('week', ld.loan_created_date + 4)::date - 4 loan_created_week
, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', ld.loan_created_date+4)::date+2
	WHEN datediff('day', ld.loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', ld.loan_created_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', ld.loan_created_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', ld.loan_created_date::date+4)::date+2
END loan_created_week_end_date
, left(ld.loan_created_date,7) AS loan_created_month
, CONCAT(year(ld.loan_created_date)::varchar(), CASE WHEN month(ld.loan_created_date) IN (1,2,3) THEN '_Q1' WHEN month(ld.loan_created_date) IN (4,5,6) THEN '_Q2' WHEN month(ld.loan_created_date) IN (7,8,9) THEN '_Q3' WHEN month(ld.loan_created_date) IN (10,11,12) THEN '_Q4' END) AS loan_created_quarter

/*
-- CREDITED TIME 
, ld.LOAN_CREDITED_DATE
, date_trunc('week', ld.LOAN_CREDITED_DATE + 4)::date - 4 loan_credited_week
, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', ld.loan_credited_date+4)::date+2
	WHEN datediff('day', ld.loan_credited_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', ld.loan_credited_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', ld.loan_credited_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', ld.loan_credited_date::date+4)::date+2
END loan_credited_week_end_date
, left(ld.loan_credited_date,7) AS loan_credited_month
, CONCAT(year(ld.loan_credited_date)::varchar(), CASE WHEN month(ld.loan_credited_date) IN (1,2,3) THEN '_Q1' WHEN month(ld.loan_credited_date) IN (4,5,6) THEN '_Q2' WHEN month(ld.loan_credited_date) IN (7,8,9) THEN '_Q3' WHEN month(ld.loan_credited_date) IN (10,11,12) THEN '_Q4' END) AS loan_credited_quarter
*/
-- LOAN DATA CONT.

, ld.ORIGINATED_AMOUNT
, COALESCE (dacd.risk_review_fico_score, dacd.fico_onboarding) AS fico_at_draw_n
, fico_at_draw_n * ld.ORIGINATED_AMOUNT AS fico_orig_product
-- 17 Oct 2024 Addition: Vantage
, dacd.CREDIT_SCORE_JSON:"VantageScore 4.0"."score" AS VANTAGE4
, VANTAGE4 * ld.ORIGINATED_AMOUNT AS vantage_orig_product
-- 23 Oct 2024 Addition: Industry Type
, CASE
	WHEN (left(cd.industry_naics_code,3) BETWEEN 441 AND 453 OR left(cd.industry_naics_code,3) = 722) THEN 'Retail / Restaurants'
	WHEN left(cd.industry_naics_code,3) = 454 THEN 'E-Commerce'
	WHEN left(cd.industry_naics_code,3) BETWEEN 236 AND 238 THEN 'Construction'
	WHEN left(cd.industry_naics_code,3) BETWEEN 481 AND 492 THEN 'Transportation'
	WHEN left(cd.industry_naics_code,3) = 541 THEN 'Professional Services'
	ELSE 'Others'
END industry_type 
, ld.OUTSTANDING_PRINCIPAL_DUE
, ld.CHARGEOFF_PRINCIPAL
, ld.OUTSTANDING_PRINCIPAL_DUE - ld.CHARGEOFF_PRINCIPAL good_os
--, ld.delinquent_principal
-- , ld.CHARGEOFF_PRINCIPAL
, ld.CREDITED_AMOUNT
, ld.FIRST_PLANNED_TRANSMISSION_DATE
--, ld.LAST_ACTIVE_PLANNED_DEBIT_DATE
, ld.FIRST_GOOD_DEBIT_PAYMENT_DATE
--, ld.LAST_GOOD_DEBIT_PAYMENT
--, ld.LAST_FAILED_DEBIT_DATE
--, ld.LAST_ORIGINAL_PAYMENT_DATE
, dld7.edate fdd_plus_7
, dld7.outstanding_principal_due fdd_plus_7_newos
, dld7.is_charged_off fdd_plus_7_charge_off
, dld14.edate fdd_plus_14
, dld14.outstanding_principal_due fdd_plus_14_newos
, dld14.is_charged_off fdd_plus_14_charge_off
, dld21.edate fdd_plus_21
, dld21.outstanding_principal_due fdd_plus_21_newos
, dld21.is_charged_off fdd_plus_21_charge_off
, dld28.edate fdd_plus_28
, dld28.outstanding_principal_due fdd_plus_28_newos
, dld28.is_charged_off fdd_plus_28_charge_off
, dld56.edate fdd_plus_56
, dld56.outstanding_principal_due fdd_plus_56_newos
, dld56.is_charged_off fdd_plus_56_charge_off

, dld7.dpd_days AS fdd7_dpd
, dld14.dpd_days AS fdd14_dpd
, dld21.dpd_days AS fdd21_dpd
, dld28.dpd_days AS fdd28_dpd
, dld56.dpd_days AS fdd56_dpd
-- 29Jan addition for Quarterly Metric
, q.wob6_os as OS_6WOB_V2
--, q.DPD_identifier_v1 as DPD_identifier_v1
, q.orig  as quarterly_metric_ORIGINATED_AMOUNT

FROM indus.PUBLIC.LOAN_DATA_NEW_DPD ld 

LEFT JOIN BI.FINANCE.DIM_LOAN fdl
ON ld.loan_key = fdl.loan_key

LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f 
ON ld.fbbid = f.fbbid
AND ld.loan_created_date::date = f.edate 

LEFT JOIN BI.PUBLIC.CUSTOMERS_DATA cd
ON ld.fbbid = cd.fbbid 

LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd 
ON ld.fbbid = dacd.fbbid 
AND ld.loan_created_date::date = dacd.edate 

-- 18 Sep Addition: Payment plan
/*LEFT JOIN INDUS."PUBLIC".DAILY_LOAN_DATA_INDUS dld
ON ld.loan_key = dld.loan_key
AND ld.loan_created_date::date = dld.edate  */

 -- RMR flag
LEFT JOIN (
SELECT DATA:tag_option::varchar flow_Type, 
DATA:segment::varchar rmr_segment,
DATA:event_type::varchar event_type, 
DATA:fbbid AS fbbid,
DATA:event_time::timestamp event_time, 
DATA:comment::varchar AS comment,
--CASE WHEN DATA:comment::varchar IN ('No balance','Inactive') THEN 'No balance' ELSE 'Balance' end AS balance_flag, 
DATA:balance_paid_off balance_to_be_paid

FROM CDC_V2.RISK_HIST.EVENTS_OUTBOX eo
WHERE DATA:entity_type = 'RMR_FLOW'
qualify row_number() over(partition by fbbid order BY event_time  desc) = 1
) rmr
ON dacd.fbbid = rmr.fbbid
LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) dld7
ON ld.loan_key = dld7.LOAN_KEY 
AND dateadd('day',7,ld.FIRST_PLANNED_TRANSMISSION_DATE) = dld7.edate

LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) dld14
ON ld.loan_key = dld14.LOAN_KEY 
AND dateadd('day',14,ld.FIRST_PLANNED_TRANSMISSION_DATE) = dld14.edate

LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) dld21
ON ld.loan_key = dld21.LOAN_KEY 
AND dateadd('day',21,ld.FIRST_PLANNED_TRANSMISSION_DATE) = dld21.edate

LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) dld28
ON ld.loan_key = dld28.LOAN_KEY 
AND dateadd('day',28,ld.FIRST_PLANNED_TRANSMISSION_DATE) = dld28.edate

LEFT JOIN (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) dld56
ON ld.loan_key = dld56.LOAN_KEY 
AND dateadd('day',56,ld.FIRST_PLANNED_TRANSMISSION_DATE) = dld56.edate

--29 Jan addition (Quarterly Metric into LO query)
LEFT JOIN INDUS.PUBLIC.qm_test1 q 
ON ld.LOAN_KEY = q.LOAN_KEY
WHERE TRUE 
AND ld.loan_key IS NOT NULL 
AND dacd.fbbid IS NOT NULL 
AND (ld.loan_operational_status <> 'CNCL' OR ld.loan_operational_status IS NULL)
and dacd.is_test_user = 0
-------------------------------31th May Changes--------------
AND f.sub_product <> 'Credit Builder'
AND f.sub_product <> 'mca'
-------------------------------------------------------------
)

SELECT a.*
, b.fdd7_orig        AS fdd7_orig_vert
, b.fdd7_dpd1_orig   AS fdd7_dpd1_orig_vert   
, b.fdd7_dpd1_os AS fdd7_dpd1_os_vert
, c.fdd14_orig       AS fdd14_orig_vert
, c.fdd14_dpd7_orig  AS fdd14_dpd7_orig_vert
, c.fdd14_dpd7_os AS fdd14_dpd7_os_vert
, d.fdd21_orig       AS fdd21_orig_vert
, d.fdd21_dpd7_orig  AS fdd21_dpd7_orig_vert
, d.fdd21_dpd7_os AS fdd21_dpd7_os_vert
, e.fdd28_orig       AS fdd28_orig_vert
, e.fdd28_dpd7_orig  AS fdd28_dpd7_orig_vert
, e.fdd28_dpd14_orig AS fdd28_dpd14_orig_vert
, e.fdd28_dpd7_os AS fdd28_dpd7_os_vert
, e.fdd28_dpd14_os AS fdd28_dpd14_os_vert
, f.fdd56_orig       AS fdd56_orig_vert
, f.fdd56_dpd14_orig AS fdd56_dpd14_orig_vert
, f.fdd56_dpd35_orig AS fdd56_dpd35_orig_vert
, f.fdd56_dpd14_os AS fdd56_dpd14_os_vert
, f.fdd56_dpd35_os AS fdd56_dpd35_os_vert
, g.quarterly_metric_numerator
, g.quarterly_metric_denominator

FROM(
SELECT loan_created_week_end_date week_end_date
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, count(loan_key) num_loans
, sum(ORIGINATED_AMOUNT) orig 
, sum(OUTSTANDING_PRINCIPAL_DUE) os
, sum(CASE WHEN FICO_AT_DRAW_n IS NULL THEN 0 ELSE ORIGINATED_AMOUNT END) orig_fico_not_null
, sum(CASE WHEN risk_bucket IS NULL THEN 0 ELSE originated_amount END) orig_bucket_not_null
, sum(originated_amount*fico_at_draw_n) fico_orig_prod
-- 17 Oct: Vantage Addition
, sum(CASE WHEN VANTAGE4 IS NULL THEN 0 ELSE ORIGINATED_AMOUNT END) orig_vantage_not_null
, sum(originated_amount*VANTAGE4) vantage_orig_prod
, sum(originated_amount*risk_bucket) bucket_orig_prod
, sum(CASE WHEN fdd7_dpd  > 1 THEN originated_amount ELSE 0 END) fdd7_dpd1_orig
, sum(CASE WHEN fdd14_dpd > 7 THEN originated_amount ELSE 0 END) fdd14_dpd7_orig
, sum(CASE WHEN fdd21_dpd > 7 THEN originated_amount ELSE 0 END) fdd21_dpd7_orig
, sum(CASE WHEN fdd28_dpd > 7 THEN originated_amount ELSE 0 END) fdd28_dpd7_orig
, sum(CASE WHEN fdd28_dpd > 14 THEN originated_amount ELSE 0 END) fdd28_dpd14_orig
, sum(CASE WHEN fdd56_dpd > 14 THEN originated_amount ELSE 0 END) fdd56_dpd14_orig
, sum(CASE WHEN fdd56_dpd > 35 THEN originated_amount ELSE 0 END) fdd56_dpd35_orig
, sum(CASE WHEN fdd_plus_7_charge_off = 0 and  fdd7_dpd  > 1 THEN fdd_plus_7_newos ELSE 0 END) fdd7_dpd1_os
, sum(CASE WHEN fdd_plus_14_charge_off = 0 and fdd14_dpd > 7 THEN fdd_plus_14_newos ELSE 0 END) fdd14_dpd7_os
, sum(CASE WHEN fdd_plus_21_charge_off = 0 and fdd21_dpd > 7 THEN fdd_plus_21_newos ELSE 0 END) fdd21_dpd7_os
, sum(CASE WHEN fdd_plus_28_charge_off = 0 and fdd28_dpd > 7  THEN fdd_plus_28_newos ELSE 0 END) fdd28_dpd7_os
, sum(CASE WHEN fdd_plus_28_charge_off = 0 and fdd28_dpd > 14 THEN fdd_plus_28_newos ELSE 0 END) fdd28_dpd14_os
, sum(CASE WHEN fdd_plus_56_charge_off = 0 and fdd56_dpd > 14 THEN fdd_plus_56_newos ELSE 0 END) fdd56_dpd14_os
, sum(CASE WHEN fdd_plus_56_charge_off = 0 and fdd56_dpd > 35 THEN fdd_plus_56_newos ELSE 0 END) fdd56_dpd35_os
FROM lo
WHERE week_end_date <= current_timestamp()
GROUP BY 1,2,3,4,5,6,7,8,9,10) a 

LEFT JOIN (
SELECT fdd_plus_7 analysis_week
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(originated_amount) fdd7_orig
, SUM(OUTSTANDING_PRINCIPAL_DUE) fdd7_os
, sum(CASE WHEN fdd7_dpd > 1 THEN originated_amount ELSE 0 END) fdd7_dpd1_orig
, sum(CASE WHEN fdd7_dpd > 1 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd7_dpd1_os
FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) b 
ON a.week_end_date = b.analysis_week
AND a.channel = b.channel
AND a.TERMUNITS = b.TERMUNITS
AND a.partner = b.partner
AND a.lendio_flow = b.lendio_flow
AND a.national_funding_flow = b.national_funding_flow
AND a.new_cust_filter = b.new_cust_filter 
AND a.bucket_group = b.bucket_group
AND a.payment_plan = b.payment_plan
AND a.industry_type = b.industry_type

LEFT JOIN (
SELECT fdd_plus_14 analysis_week
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(originated_amount) fdd14_orig
, sum(CASE WHEN fdd14_dpd > 7 THEN originated_amount ELSE 0 END) fdd14_dpd7_orig
, sum(CASE WHEN fdd14_dpd > 7 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd14_dpd7_os
FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) c 
ON a.week_end_date = c.analysis_week
AND a.channel = c.channel
AND a.TERMUNITS = c.TERMUNITS
AND a.partner = c.partner
AND a.lendio_flow = c.lendio_flow
AND a.national_funding_flow = c.national_funding_flow
AND a.new_cust_filter = c.new_cust_filter 
AND a.bucket_group = c.bucket_group
AND a.payment_plan = c.payment_plan
AND a.industry_type = c.industry_type

LEFT JOIN (
SELECT fdd_plus_21 analysis_week
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(originated_amount) fdd21_orig
, sum(CASE WHEN fdd21_dpd > 7 THEN originated_amount ELSE 0 END) fdd21_dpd7_orig
, sum(CASE WHEN fdd21_dpd > 7 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd21_dpd7_os
FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) d 
ON a.week_end_date = d.analysis_week
AND a.channel = d.channel
AND a.TERMUNITS = d.TERMUNITS
AND a.partner = d.partner
AND a.lendio_flow = d.lendio_flow
AND a.national_funding_flow = d.national_funding_flow
AND a.new_cust_filter = d.new_cust_filter 
AND a.bucket_group = d.bucket_group
AND a.payment_plan = d.payment_plan
AND a.industry_type = d.industry_type

LEFT JOIN (
SELECT fdd_plus_28 analysis_week
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(originated_amount) fdd28_orig
, sum(CASE WHEN fdd28_dpd > 7 THEN originated_amount ELSE 0 END) fdd28_dpd7_orig
, sum(CASE WHEN fdd28_dpd > 14 THEN originated_amount ELSE 0 END) fdd28_dpd14_orig
, sum(CASE WHEN fdd28_dpd > 7 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd28_dpd7_os
, sum(CASE WHEN fdd28_dpd > 14 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd28_dpd14_os

FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) e 
ON a.week_end_date = e.analysis_week
AND a.channel = e.channel
AND a.TERMUNITS = e.TERMUNITS
AND a.partner = e.partner
AND a.lendio_flow = e.lendio_flow
AND a.national_funding_flow = e.national_funding_flow
AND a.new_cust_filter = e.new_cust_filter 
AND a.bucket_group = e.bucket_group
AND a.payment_plan = e.payment_plan
AND a.industry_type = e.industry_type

LEFT JOIN (
SELECT fdd_plus_56 analysis_week
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(originated_amount) fdd56_orig
, sum(CASE WHEN fdd56_dpd > 14 THEN originated_amount ELSE 0 END) fdd56_dpd14_orig
, sum(CASE WHEN fdd56_dpd > 35 THEN originated_amount ELSE 0 END) fdd56_dpd35_orig
, sum(CASE WHEN fdd56_dpd > 14 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd56_dpd14_os
, sum(CASE WHEN fdd56_dpd > 35 THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) fdd56_dpd35_os
FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) f 
ON a.week_end_date = f.analysis_week
AND a.channel = f.channel
AND a.TERMUNITS = f.TERMUNITS
AND a.partner = f.partner
AND a.lendio_flow = f.lendio_flow
AND a.national_funding_flow = f.national_funding_flow
AND a.new_cust_filter = f.new_cust_filter 
AND a.bucket_group = f.bucket_group
AND a.payment_plan = f.payment_plan
AND a.industry_type = f.industry_type

-- 29Jan addition for quarterly metrics
LEFT JOIN (
SELECT loan_created_week_end_date
, channel
, TERMUNITS
, partner
, lendio_flow
, national_funding_flow
, new_cust_filter
, bucket_group
, payment_plan
, industry_type
, sum(OS_6WOB_V2) as quarterly_metric_numerator
, sum(quarterly_metric_ORIGINATED_AMOUNT) as quarterly_metric_denominator
FROM lo
GROUP BY 1,2,3,4,5,6,7,8,9,10) g
ON a.week_end_date = g.loan_created_week_end_date
AND a.channel = g.channel
AND a.TERMUNITS = g.TERMUNITS
AND a.partner = g.partner
AND a.lendio_flow = g.lendio_flow
AND a.national_funding_flow = g.national_funding_flow
AND a.new_cust_filter = g.new_cust_filter 
AND a.bucket_group = g.bucket_group
AND a.payment_plan = g.payment_plan
AND a.industry_type = g.industry_type
--ORDER BY 1 DESC, 2, 3 DESC, 4
);


