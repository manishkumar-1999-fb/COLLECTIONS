CREATE OR REPLACE TABLE INDUS.PUBLIC.alt_lenders_wallet_share AS 
(SELECT 
loan_created_date,
fbbid ,
sum(originated_amount) as fbx_orig,
--max(IS_CHARGED_OFF) AS IS_CHARGED_OFF,
--max(CHARGE_OFF_DATE) AS CHARGE_OFF_DATE ,
--sum(OUTSTANDING_PRINCIPAL_DUE) AS OUTSTANDING_PRINCIPAL_DUE,
--sum(CHARGEOFF_PRINCIPAL) AS CHARGEOFF_PRINCIPAL,
--max(loan_key) AS loan_key,
--max(LOAN_CREATED_DATE) AS LOAN_CREATED_DATE
--------11th april change--------------------
-- FROM INDUS.PUBLIC.summary_v1
FROM BI.FINANCE.FINANCE_METRICS_DAILY
WHERE PRODUCT_TYPE <> 'Flexpay'
and loan_created_date = edate
-- WHERE rnk = 1
GROUP BY loan_created_date,fbbid );

create or replace table indus.public.alternative_lenders_vol as
(

with cte as(

select yt.fbbid
--,yt.transaction_type
--,yt.tdate as edate
,CASE 
		WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', yt.tdate::date+4)::date+2
		WHEN datediff('day', yt.tdate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', yt.tdate, current_date()) <= 0 THEN NULL 
		WHEN datediff('day', yt.tdate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
		ELSE DATE_TRUNC('WEEK', yt.tdate::date+4)::date+2
	END week_of_edate


,yt.amount as non_fbx_orig
--, fil.partner
--, fil.lendio_flow
--, fil.national_funding_flow

--, case when dacd.account_status = 'active' and dacd.is_chargeoff = 0 and dacd.dpd_days_corrected < 98 then 'OPEN' else 'CLOSE' end AS open_status

--, case when dacd.account_status = 'active' and p.dpd_days_corrected_fmd  BETWEEN 0 AND 91 and p.IS_CHARGED_OFF = 0 and p.OUTSTANDING_PRINCIPAL_DUE > 0 then 'ACTIVE_ACC' else 'INACTIVE_ACC' end AS active_status


from 
(   
select *
,transaction_date::date as tdate
from cdc_v2.fi_connect.yodlee_transactions 
)
yt
left join data_science.yodlee_transactions_features.features f
on yt.id = f.transaction_primary_id



where is_business_alternative_loan_v1 = 1
and transaction_date::date >= '2021-01-01'

and transaction_type in ('credit')
--AND FIL.SUB_PRODUCT <> 'Credit Builder'
)

, fbx_cte AS (
    SELECT 
       a.*, b.* from 
       (select *, )
)
SELECT 
    transaction_type,
    week_end_date,
    open_status,
    active_status,
    1 AS ONE,
    SUM(amount) AS total_amount,
    MAX(m_week) AS median_week, 
    MAX(m_trans) AS median_trans,
    --MAX(m_open) AS median_open,
    --MAX(m_active) AS median_active,
    MAX(m_trans_active) AS median_trans_active,
    MAX(m_trans_open) AS median_trans_open
FROM 
    median_cte
GROUP BY 
    transaction_type, week_end_date, open_status, active_status
);