

---- Take all delinquent accounts on FBBID level
---- map First time delinquency, then map next 4 delinquency, if it is continuous default or 3/4 delinquency, flag them
---- Map Call logs 
---- Inbound call logs
---- See if we can map dashboard login/event history 
---- Bankruptcy, DNC and settlement failed

---- no of dials attempted outbound & inbound
--- no of RPC
--- no of Payment
---- no of PTP
--- no of Third party
--- no of DNC
---- no of bankruptcy
--- no of Invalid
--- no of settlement

--- no of payments
create or replace table tableau.credit.legal_placement_accounts as 
with 
fmd_agg AS (
  SELECT
      fmd.fbbid
    , fmd.edate
    , loan_created_date
    , first_planned_transmission_date
    -- , min(fmd.loan_created_date) as loan_created_date
    -- , min(first_planned_transmission_date) as first_planned_transmission_date
    , max(charge_off_date) as charge_off_date
    , MAX(fmd.dpd_days)     AS dpd_days        
    , MAX(fmd.dpd_bucket)   AS dpd_bucket
    , SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans
    , MAX(fmd.is_charged_off) AS is_charged_off_any
  FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
  GROUP BY all
)
-- 1. Identify the 1,800 accounts currently delinquent
,base_inventory AS (
    SELECT 
        fbbid,
        edate AS as_of_date,
        max(charge_off_date) as charge_off_date,
        sum(outstanding_balance_due_all_loans) as outstanding_balance_due_all_loans,
        max(dpd_days) AS current_dpd,
        max(dpd_bucket) as current_dpd_bucket,
        max(is_charged_off_any) as is_charged_off_any,
    FROM fmd_agg
    WHERE edate = CURRENT_DATE-1
      AND dpd_days > 0 
      --AND is_charged_off_any = 0
    group by all
)
-- Select * from fmd_agg where fbbid = 1139462 order by edate;
,
base_inventory1 as 
(
select a.*,
-- dacd.recovery_suggested_state,
-- dacd.recovery_suggested_substate,
-- case when dacd.recovery_suggested_state = 'ELR' then 1 else 0 end as Internal_recovery_accnts
from base_inventory a
left join bi.public.daily_approved_customers_data dacd 
on a.fbbid = dacd.fbbid 
and dacd.edate = a.as_of_date
where dacd.recovery_suggested_state <>'ELR'
)
-- 2. Find the specific dates for each DPD milestone for these accounts
,delinquency_milestones AS (
    SELECT 
        fbbid,
        min(loan_created_date) as loan_created_date,
        min(first_planned_transmission_date) as first_planned_transmission_date,
        MIN(CASE WHEN dpd_bucket = 1  THEN edate END) AS dpd_1,
        MIN(CASE WHEN dpd_bucket = 2  THEN edate END) AS dpd_2,
        MIN(CASE WHEN dpd_bucket = 3 THEN edate END) AS DPD_3,
        MIN(CASE WHEN dpd_bucket = 4 THEN edate END) AS DPD_4
    FROM fmd_agg
    WHERE fbbid IN (SELECT fbbid FROM base_inventory)
    GROUP BY 1
)
,
-- 3. Combine base inventory with the milestone dates
Default_flag as
(
SELECT 
    b.fbbid,
    m.loan_created_date,
    m.first_planned_transmission_date,
    b.as_of_date,
    b.outstanding_balance_due_all_loans as OS,
    b.current_dpd,
    b.is_charged_off_any,
    b.charge_off_date,
    m.DPD_1,
    m.DPD_2,
    m.DPD_3,
    m.DPD_4,
    datediff(day,first_planned_transmission_date,m.dpd_1) as days_for_1_dq,
    datediff(day,m.dpd_1,m.dpd_2) as days_for_2_dq,
    datediff(day,m.dpd_2,m.dpd_3) as days_for_3_dq,
    datediff(day,m.dpd_3,m.dpd_4) as days_for_4_dq,
    case 
    when m.dpd_4 is not null then 
    datediff(day,first_planned_transmission_date,m.dpd_4) 
    when m.dpd_4 is null and m.dpd_3 is not null then 
    datediff(day,first_planned_transmission_date,m.dpd_3)
    end as Default_dates_since_plan    
FROM base_inventory1 b
LEFT JOIN delinquency_milestones m ON b.fbbid = m.fbbid
)
--select * from default_flag;
--All call logs 
,call_data_five_nine as (
    select 
        fbbid,
        date_time_call,
        call_type,
        campaign,
        contacted,
        disposition,
        list_name,
        dpd_bucket
    from analytics.credit.v_five9_call_log
),

call_data_salesforce as (
    select
        case when fundbox_id__c = 'Not Linked' then 0 else try_to_number(fundbox_id__c) end as fbbid,
        lastmodifieddate,
        calltype,
        calldisposition,
        call_disposition__c,
        is_dm_contact__c,
        task_logging_method__c
    from external_data_sources.salesforce_nova.task
)
-- select * from call_data_five_nine
-- where fbbid in (2734646,
-- 2809028) and call_type = 'Inbound'
-- order by date_time_call desc;


,
-- Aggregate Five9 Metrics (Source 1)
s1_metrics as (
    select 
        fbbid,
        count(*) as s1_dials_attempted,
        count(case when call_type in('Outbound','Manual') then 1 end) as s1_outbound_attempts,
        count(case when call_type = 'Inbound' then 1 end) as s1_inbound_attempts,
        count(case when disposition ilike '%RPC%' then 1 end) as s1_rpc,
        count(case when disposition ilike '%Payment%' then 1 end) as s1_payment,
        count(case when disposition ilike '%PTP%' or disposition ilike '%Promise%' then 1 end) as s1_ptp,
        count(case when disposition ilike '%Third Party%' then 1 end) as s1_third_party,
        count(case when disposition ilike '%DNC%' or disposition ilike '%Do Not Call%' then 1 end) as s1_dnc,
        count(case when disposition ilike '%Bankruptcy%' then 1 end) as s1_bankruptcy,
        count(case when disposition ilike '%Invalid%' or disposition ilike '%Wrong Number%' then 1 end) as s1_invalid,
        count(case when disposition ilike '%Settlement%' then 1 end) as s1_settlement,
        max(case when (disposition ilike '%RPC%' or disposition ilike '%PTP%' or disposition ilike '%Payment%') 
            then date_time_call end) as s1_last_contacted,
        max(case when call_type = 'Inbound' 
            then date_time_call end) as s1_last_inbound      
    from call_data_five_nine
    group by 1
),
-- Aggregate Salesforce Metrics (Source 2)
s2_metrics as (
    select 
        fbbid,
        count(*) as s2_dials_attempted,
        count(case when calltype in('Outbound','Manual') then 1 end) as s2_outbound_attempts,
        count(case when calltype = 'Inbound' then 1 end) as s2_inbound_attempts,
        count(case when calldisposition ilike '%RPC%' then 1 end) as s2_rpc,
        count(case when calldisposition ilike '%Payment%' then 1 end) as s2_payment,
        count(case when calldisposition ilike '%PTP%' or calldisposition ilike '%Promise%' then 1 end) as s2_ptp,
        count(case when calldisposition ilike '%Third Party%' then 1 end) as s2_third_party,
        count(case when calldisposition ilike '%DNC%' or calldisposition ilike '%Do Not Call%' then 1 end) as s2_dnc,
        count(case when calldisposition ilike '%Bankruptcy%' then 1 end) as s2_bankruptcy,
        count(case when calldisposition ilike '%Invalid%' or calldisposition ilike '%Wrong Number%' then 1 end) as s2_invalid,
        count(case when calldisposition ilike '%Settlement%' then 1 end) as s2_settlement,
        max(case when (calldisposition ilike '%RPC%' or calldisposition ilike '%PTP%' or calldisposition ilike '%Payment%') 
            then lastmodifieddate end) as s2_last_contacted,
        max(case when calltype = 'Inbound' 
            then lastmodifieddate end) as s2_last_inbound  
    from call_data_salesforce
    group by 1
),
with_call_flags 
as
(select 
    base.*,
    -- Source 1 (Five9)
    coalesce(s1.s1_dials_attempted, 0) as s1_dials_attempted,
    coalesce(s1.s1_outbound_attempts,0) as s1_outbound_attempts,
    coalesce(s1.s1_inbound_attempts,0) as s1_inbound_attempts,
    coalesce(s1.s1_rpc, 0) as s1_rpc,
    coalesce(s1.s1_payment, 0) as s1_payment,
    coalesce(s1.s1_ptp, 0) as s1_ptp,
    coalesce(s1.s1_third_party, 0) as s1_third_party,
    coalesce(s1.s1_dnc, 0) as s1_dnc,
    coalesce(s1.s1_bankruptcy, 0) as s1_bankruptcy,
    coalesce(s1.s1_invalid, 0) as s1_invalid,
    coalesce(s1.s1_settlement, 0) as s1_settlement,
    
    -- Source 2 (Salesforce)
    coalesce(s2.s2_dials_attempted, 0) as s2_dials_attempted,
    coalesce(s2.s2_outbound_attempts,0) as s2_outbound_attempts,
    coalesce(s2.s2_inbound_attempts,0) as s2_inbound_attempts,
    coalesce(s2.s2_rpc, 0) as s2_rpc,
    coalesce(s2.s2_payment, 0) as s2_payment,
    coalesce(s2.s2_ptp, 0) as s2_ptp,
    coalesce(s2.s2_third_party, 0) as s2_third_party,
    coalesce(s2.s2_dnc, 0) as s2_dnc,
    coalesce(s2.s2_bankruptcy, 0) as s2_bankruptcy,
    coalesce(s2.s2_invalid, 0) as s2_invalid,
    coalesce(s2.s2_settlement, 0) as s2_settlement,
    
    -- Combined Last Contact Date
    s1.s1_last_contacted, 
    s2.s2_last_contacted,
    s1.s1_last_inbound,
    s2.s2_last_inbound

from Default_flag base
left join s1_metrics s1 on base.fbbid = s1.fbbid
left join s2_metrics s2 on base.fbbid = s2.fbbid
),
payment_metrics as (
    select 
        fbbid, 
        max(payment_planned_transmission_date) as payment_date,
        count(*) as total_payment_count,
        sum(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)) AS payment_amount,
    from bi.finance.payments_model
    where payment_status = 'FUND' 
      and direction = 'D'
    group by 1
)
,with_payment as 
(
select a.*,
b.total_payment_count,
b.payment_amount,
b.payment_date as latest_payment
from with_call_flags a
left join payment_metrics b 
on a.fbbid = b.fbbid
),
settlement as
(
select 
fbbid,
settlement_created_date,
settlement_end_time,
current_status
from analytics.credit.cjk_v_backy_settlements
qualify row_number() over(partition by fbbid order by event_time desc) = 1
),
with_settlement as 
(
select a.*,
b.current_status,
b.settlement_created_date,
case when b.current_status = 'Failed' then 1 else 0 end as settlement_flag
from with_payment a
left join settlement b
on a.fbbid = b.fbbid 
),
with_pg as
(
select a.*,
    RT.PG_FIRST_NAME as PG_Signee_First_Name,
    RT.PG_LAST_NAME,
case when RT.PG_FIRST_NAME is not null then 1 else 0 end PG_flag
from with_settlement a 
LEFT JOIN BI.PUBLIC.CUSTOMERS_RT_DATA RT ON A.FBBID = RT.FBBID
)
,final_data 
as
(
select 
FBBID,
LOAN_CREATED_DATE,
FIRST_PLANNED_TRANSMISSION_DATE,
AS_OF_DATE,
OS,
CURRENT_DPD,
IS_CHARGED_OFF_ANY,
CHARGE_OFF_DATE,
PG_Signee_First_Name,
PG_flag,
case when OS < 15000 then '1.LE15k'
when OS>=15000 and OS<25000 then '2.15-25K'
when OS>=25000 and OS <50000 then '3.25-50K'
when OS>=50000 and OS<75000 then '4.50-75K'
when os>-75000 and OS<100000 then '5.75-100K'
else '6.GE100k'
end as OS_bucket,
DAYS_FOR_1_DQ,
DAYS_FOR_2_DQ,
DAYS_FOR_3_DQ,
DAYS_FOR_4_DQ,
DEFAULT_DATES_SINCE_PLAN,
case when DEFAULT_DATES_SINCE_PLAN <=45 then 1 else 0 end as default_flag,
greatest(S1_DIALS_ATTEMPTED,S2_DIALS_ATTEMPTED) as total_dials,
case when (S1_DIALS_ATTEMPTED >0 or S2_DIALS_ATTEMPTED>0) then 1 else 0 end as dial_flag,
greatest(S2_INBOUND_ATTEMPTS,S1_INBOUND_ATTEMPTS) as inbound_dials,
case when S2_INBOUND_ATTEMPTS>0 or S1_INBOUND_ATTEMPTS>0 then 1 else 0 end as inbound_flag,
case when (
        S1_RPC+S1_PAYMENT+S1_PTP+S1_THIRD_PARTY+
        S2_RPC+S2_PAYMENT+S2_PTP+S2_THIRD_PARTY
        )>0 then 1 else 0 end as contacted_flag,
case when (S2_DNC+S1_DNC)>0 then 1 else 0 end as DNC_flag,
case when (S2_BANKRUPTCY+S1_BANKRUPTCY)>0 then 1 else 0 end as Bankruptcy_flag,
case when (S2_INVALID+S1_INVALID)>0 then 1 else 0 end as Invalid_flag,
case when (S2_SETTLEMENT+S1_SETTLEMENT)>0 then 1 else 0 end as Settlement_dispo_flag,
Case when  
s2_last_contacted is null or (S1_LAST_CONTACTED> S2_LAST_CONTACTED) then S1_LAST_CONTACTED
     else S2_LAST_CONTACTED end as last_contacted,
case when s2_last_inbound is null or (s1_last_inbound>s2_last_inbound) then s1_last_inbound
    else s2_last_inbound end as last_inbound,
TOTAL_PAYMENT_COUNT,	
PAYMENT_AMOUNT,
LATEST_PAYMENT,
SETTLEMENT_FLAG
from with_pg
),
--- mapping business address, city and state
--- other columns such as vantage score, debt payments,
with_vantage as
(
select a.*,
f.fico fico,
f.vantage4 vantage,
CASE
        WHEN f.vantage4 IS NULL THEN NULL
        WHEN f.vantage4 < 550 THEN NULL
        WHEN f.vantage4 < 600 THEN '550-600'
        WHEN f.vantage4 < 650 THEN '600-650'
        WHEN f.vantage4 < 700 THEN '650-700'
        WHEN f.vantage4 < 750 THEN '700-750'
        WHEN f.vantage4 < 800 THEN '750-800'
        WHEN f.vantage4 <= 850 THEN '800-850'
        ELSE NULL
    END AS vantage_buckets,
    f.edate,
    f.customer_annual_revenue_group as rev_bands,
    f.registration_flow_completed_date,
    da.credit_limit as First_CL,
    da1.credit_limit as CL_Charged_off
    
from final_data a
LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 f 
ON a.fbbid=f.fbbid and AS_OF_DATE= dateadd(day,6,f.edate)  

LEFT JOIN bi.public.daily_approved_customers_data da
    ON da.fbbid=a.fbbid 
    AND LOAN_CREATED_DATE = DATEADD(day, 1, da.edate)

LEFT JOIN bi.public.daily_approved_customers_data da1
    ON da1.fbbid=a.fbbid 
    AND CHARGE_OFF_DATE = DATEADD(day, -1, da1.edate)
),
with_industry as
(
Select a.*,
b.industry_naics_code_edate,
left(b.industry_naics_code_edate,4) as industry_code
-- d.naics_title
from with_vantage a
left join bi.public.daily_approved_customers_data b
on a.fbbid = b.fbbid and b.edate = current_date
),
with_industry_2 as
(
select a.*,
d.naics_title,
e.naics_title as industry
from with_industry a 
left join CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING d
on a.industry_code = d.naics_code
left join CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING e
on a.industry_naics_code_edate = e.naics_code
)
SELECT 
    a.*, 
    b.total_debt_payments_last_12_months, 
    b.repayment_bucket_last_12_months, 
    b.alt_loan_payments_last_12_months,
    b.alt_loan_payment_bucket,
    b.revenue, 
    b.dscr * 100 AS dscr_percent,
    d.zip_code as business_address,
    d.state as state,
    d.city,
CASE 
    WHEN (b.dscr * 100) <= 5  THEN '1. 0-5'
    WHEN (b.dscr * 100) <= 10 THEN '2. 5-10'
    WHEN (b.dscr * 100) <= 20 THEN '3. 10-20'
    WHEN (b.dscr * 100) <= 30 THEN '4. 20-30'
    WHEN (b.dscr * 100) <= 40 THEN '5. 30-40'
    WHEN (b.dscr * 100) <= 50 THEN '6. 40-50'
    WHEN (b.dscr * 100) > 50  THEN '7. >50'
    ELSE NULL
END AS dscr_bucket,
case
    when last_contacted is null and last_inbound is null then null
    when last_inbound > last_contacted then last_inbound 
    else Last_contacted 
    end as last_inb_out_contacted,
CASE 
        WHEN LATEST_PAYMENT IS NULL THEN 'No Payment'
        WHEN DATEDIFF('day', LATEST_PAYMENT, CURRENT_DATE()) < 30 THEN 'LE30'
        WHEN DATEDIFF('day', LATEST_PAYMENT, CURRENT_DATE()) < 60 THEN 'LE60'
        ELSE 'GE60days'
    END AS payment_status,
CASE 
        WHEN last_inb_out_contacted IS NULL THEN 'No Contact'
        WHEN DATEDIFF('day', last_inb_out_contacted, CURRENT_DATE()) < 30 THEN 'LE30'
        WHEN DATEDIFF('day', last_inb_out_contacted, CURRENT_DATE()) < 60 THEN 'LE60'
        ELSE 'GE60days'
    END AS rpc_status_bucket
FROM with_industry_2 a
LEFT JOIN ANALYTICS.CREDIT.DEBT_PAYMENTS b 
    ON a.fbbid = b.fbbid
left join bi.public.customers_data d
on a.fbbid = d.fbbid
    ;








CREATE OR REPLACE TABLE ANALYTICS.CREDIT.DEBT_PAYMENTS AS
WITH base AS (
    SELECT DISTINCT 
        fbbid, 
        LAST_DAY(edate, 'month')::DATE as cohort, 
        sum(principal_paid_total) as p, 
        sum(revenue) as f, 
        (p+f) as fbx_loan_repayment_amt
    FROM bi.public.daily_approved_customers_data
    group by 1,2
),
transactions as (

    select
        last_day(transaction_date::date, 'month') as cohort,
        fbbid,
        sum(amount) as alt_loan_repayment_amt
    
    from cdc_v2.fi_connect.yodlee_transactions yt
    left join data_science.yodlee_transactions_features.features f
    on yt.id = f.transaction_primary_id
    -- and transaction_date::date >= '2022-11-30'

    where true 
    and transaction_type = 'debit'
    and (IS_BUSINESS_ALTERNATIVE_LOAN_V1 = 1 or IS_CONSUMER_ALTERNATIVE_LOAN_V1 = 1)

    group by 1,2),

final AS 
(
SELECT
b.fbbid, b.cohort, t.alt_loan_repayment_amt, b.fbx_loan_repayment_amt
FROM base b
LEFT JOIN transactions t
ON b.fbbid=t.fbbid
AND b.cohort=t.cohort
ORDER BY 1,2
)
SELECT
    f.fbbid,
    -- 1. Total debt payments in the last 12 months
    SUM(
        CASE
            -- Filter payments where the cohort is in the last 12 months (e.g., from '2024-12-01' to '2025-11-30')
            WHEN cohort >= DATEADD('year', -1, LAST_DAY(CURRENT_DATE(), 'month'))
            THEN COALESCE(fbx_loan_repayment_amt, 0) + COALESCE(alt_loan_repayment_amt, 0)
            ELSE 0
        END
    ) AS total_debt_payments_last_12_months,
CASE
        WHEN total_debt_payments_last_12_months = 0 THEN '1. 0'
        WHEN total_debt_payments_last_12_months > 0 AND total_debt_payments_last_12_months <= 50000 THEN '2. 0 - 50,000'
        WHEN total_debt_payments_last_12_months > 50000 AND total_debt_payments_last_12_months <= 100000 THEN '3. 50,000 - 100,000'
        WHEN total_debt_payments_last_12_months > 100000 AND total_debt_payments_last_12_months <= 200000 THEN '4. 100,000 - 200,000'
        WHEN total_debt_payments_last_12_months > 200000 AND total_debt_payments_last_12_months <= 500000 THEN '5. 200,000 - 500,000'
        WHEN total_debt_payments_last_12_months > 500000 AND total_debt_payments_last_12_months <= 1000000 THEN '6. 500,000 - 1,000,000'
        WHEN total_debt_payments_last_12_months > 1000000 AND total_debt_payments_last_12_months <= 3000000 THEN '7. 1,000,000 - 3,000,000'
        WHEN total_debt_payments_last_12_months > 3000000 AND total_debt_payments_last_12_months <= 5000000 THEN '8. 3,000,000 - 5,000,000'
        WHEN total_debt_payments_last_12_months > 5000000 THEN '9. 5,000,000+'
        ELSE 'N/A' -- Catch-all for any NULLs or unexpected values
    END AS repayment_bucket_last_12_months,
    SUM(
        CASE
            -- Filter payments where the cohort is in the last 12 months (e.g., from '2024-12-01' to '2025-11-30')
            WHEN cohort >= DATEADD('year', -1, LAST_DAY(CURRENT_DATE(), 'month'))
            THEN COALESCE(alt_loan_repayment_amt, 0)
            ELSE 0
        END
    ) AS alt_loan_payments_last_12_months,
CASE
        WHEN alt_loan_payments_last_12_months = 0 THEN '1. 0'
        WHEN alt_loan_payments_last_12_months > 0 AND alt_loan_payments_last_12_months <= 50000 THEN '2. 0 - 50,000'
        WHEN alt_loan_payments_last_12_months > 50000 AND alt_loan_payments_last_12_months <= 100000 THEN '3. 50,000 - 100,000'
        WHEN alt_loan_payments_last_12_months > 100000 AND alt_loan_payments_last_12_months <= 200000 THEN '4. 100,000 - 200,000'
        WHEN alt_loan_payments_last_12_months > 200000 AND alt_loan_payments_last_12_months <= 500000 THEN '5. 200,000 - 500,000'
        WHEN alt_loan_payments_last_12_months > 500000 AND alt_loan_payments_last_12_months <= 1000000 THEN '6. 500,000 - 1,000,000'
        WHEN alt_loan_payments_last_12_months > 1000000 AND alt_loan_payments_last_12_months <= 3000000 THEN '7. 1,000,000 - 3,000,000'
        WHEN alt_loan_payments_last_12_months > 3000000 AND alt_loan_payments_last_12_months <= 5000000 THEN '8. 3,000,000 - 5,000,000'
        WHEN alt_loan_payments_last_12_months > 5000000 THEN '9. 5,000,000+'
        ELSE 'N/A' -- Catch-all for any NULLs or unexpected values
    END AS alt_loan_payment_bucket,
    KM.CUSTOMER_ANNUAL_REVENUE AS REVENUE,
    total_debt_payments_last_12_months / NULLIF(REVENUE, 0) AS DSCR
FROM
    final f
LEFT JOIN (SELECT EDATE, FBBID, CUSTOMER_ANNUAL_REVENUE FROM indus.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2 WHERE EDATE='2026-01-07') KM
ON f.FBBID = KM.FBBID

GROUP BY
    f.fbbid, CUSTOMER_ANNUAL_REVENUE
ORDER BY
    f.fbbid;
;


  


