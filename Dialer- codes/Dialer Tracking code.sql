WITH dialer AS (
    SELECT
        t.fbbid,
        t.edate,
        t.dpd_days,
        t.dpd_bucket,
        t.outstanding_balance_due,
        LAG(t.edate, 1, NULL) OVER (PARTITION BY t.fbbid ORDER BY t.edate) AS prev_edate,
        CASE
            WHEN LAG(t.edate, 1, NULL) OVER (PARTITION BY t.fbbid ORDER BY t.edate) IS NOT NULL
                 AND DATEADD(day, 1, LAG(t.edate, 1, NULL) OVER (PARTITION BY t.fbbid ORDER BY t.edate)) = t.edate
            THEN 1
            ELSE 0 
        END AS was_present_prev_day_flag
    FROM (
        SELECT DISTINCT
            fmd.fbbid,
            fmd.edate,
            max(fmd.dpd_days)::INTEGER AS dpd_days,
            max(fmd.dpd_bucket)::INTEGER AS dpd_bucket,
            sum(fmd.outstanding_balance_due) AS outstanding_balance_due
        FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
        LEFT JOIN bi.public.customers_Data cd
            ON fmd.fbbid = cd.fbbid
        WHERE
            fmd.dpd_days IS NOT NULL
            AND is_charged_off = 0
            AND fmd.edate >= '2026-01-01'
        GROUP BY fmd.fbbid, fmd.edate
    ) t
    WHERE
        t.dpd_days BETWEEN 1 AND 98
)   
--select edate, count(*) from dialer group by edate order by edate;
,
call_data_five_nine as
(
Select 
FBBID,
Date_time_call,
CALL_TYPE,
CAMPAIGN,
CONTACTED,
DISPOSITION,
LIST_NAME,
DIAL_TIME,HANDLE_TIME,CALL_TIME,
AGENT_NAME,
AGENT_GROUP,
DPD_BUCKET
from analytics.credit.v_five9_call_log
where 
date(date_time_call) >= '2025-10-01'
AND TRIM(LOWER(campaign)) LIKE '%collection%'
-- and campaign in ('Collections - 1-2 OB',
-- 'Collections - 3-12 OB',
-- 'Collections - 14 plus OB',
-- 'Collections - Broken PTP OB',
-- 'Collections - Missed Payment Priority OB')
),
call_data_salesforce
as 
(
select
case when fundbox_id__c = 'Not Linked' then 0 else TRY_TO_NUMBER(fundbox_id__c) end as Fbbid,
lastmodifieddate,
calldurationinseconds,
calltype,
calldisposition,
assignee_name__c,
call_disposition__c,
call_hour__c,
is_dm_contact__c,
role_id_name__c,
task_logging_method__c,
skill__c
from external_data_sources.salesforce_nova.task
where date(lastmodifieddate) >= '2025-10-01'
),
Call_data_settlement as
(
Select
FBBID,
DPD_bucket,
DISPOSTION,
last_ob_attempt_date
from tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW
where date(last_ob_attempt_date) >= '2025-10-01'
),
WITH_CALL_DATA AS
(
    SELECT
        t.dialer_fbbid,
        t.edate,
        t.dpd_bucket,
        t.outstanding_balance_due,
        t.was_present_prev_day_flag,
        -- total_dials: Counts the number of call attempts (rows from Five9)
        COUNT(b_fbbid) AS total_dials, 
        -- connected: Counts calls where an agent was involved
        SUM(CASE WHEN t.agent_name IS NOT NULL THEN 1 ELSE 0 END) AS connected
    FROM 
    (
        -- Subquery: Join dialer base with Five9 calls
        SELECT
            a.fbbid AS dialer_fbbid,
            a.edate,
            a.DPD_bucket,
            a.outstanding_balance_due,
            a.was_present_prev_day_flag,
            b.fbbid AS b_fbbid, -- Include b.fbbid to count only matched rows
            b.agent_name
        FROM dialer a
        LEFT JOIN call_data_five_nine b
            ON a.fbbid = b.fbbid 
            AND a.edate = DATE(b.date_time_call)
    ) t
    GROUP BY 
        t.dialer_fbbid, 
        t.edate, 
        t.dpd_bucket, 
        t.outstanding_balance_due,
        t.was_present_prev_day_flag
)
--Select * from with_call_data where edate = '2025-10-11' 
--and connected > 1;

,
settlement_source as
(
select 
fbbid,
date(last_ob_attempt_date) as call_date,
count(*) as total_dials_s3,
sum(case when dispostion ILIKE '%F-RPC%' then 1 else 0 end) as RPC_count_s3,
sum(case when dispostion ILIKE '%B-Payment%' then 1 else 0 end) as Payment_count_s3,
sum(case when dispostion ILIKE '%D-Promise to Pay%' then 1 else 0 end) as PTP_count_s3
from call_data_settlement
group by 1,2
),
salesforce_source as
(
Select 
fbbid,
date(lastmodifieddate) as call_date,
count(*) as total_dials_s2,
sum(case when calldisposition ILIKE '%RPC%' then 1 else 0 end) as RPC_count_s2,
sum(case when calldisposition ILIKE '%Payment%' then 1 else 0 end) as Payment_count_s2,
sum(case when calldisposition ILIKE '%Promise to Pay%' then 1 else 0 end) as PTP_count_s2
from call_data_salesforce
group by 1,2
),
five_nine_source as(
Select 
fbbid,
date(date_time_call) as call_date,
count(*) as total_dials_s1,
sum(case when disposition ILIKE '%RPC%' then 1 else 0 end) as RPC_count_s1,
sum(case when disposition ILIKE '%Payment%' then 1 else 0 end) as Payment_count_s1,
sum(case when disposition ILIKE '%PTP%' then 1 else 0 end) as PTP_count_s1
from call_data_five_nine
group by 1,2
),
with_disposition as
(
Select a.*,
b.total_dials_s1,rpc_count_s1, payment_count_s1,ptp_count_s1,
c.total_dials_s2,rpc_count_s2,payment_count_s2,ptp_count_s2,
-- d.total_dials_s3,rpc_count_s3,payment_count_s3,ptp_count_s3
from WITH_CALL_DATA a
left join five_nine_source b
on a.dialer_fbbid = b.fbbid and a.edate = b.call_date
left join salesforce_source c
on a.dialer_fbbid = c.fbbid and a.edate = c.call_date
left join settlement_source d
on a.dialer_fbbid = d.fbbid and a.edate = d.call_date
),
Final_data as
(
select l.*,
case when connected =0 and (rpc_count >0 or payment_count>0 or ptp_count>0) then coalesce(rpc_count,payment_count,ptp_count) else connected end as connected_final
from 
(
SELECT
    dialer_fbbid,
    edate,
    dpd_bucket,
    outstanding_balance_due,
    connected,
    was_present_prev_day_flag,
    -- rpc_count_s1,rpc_count_s2,rpc_count_s3,
    GREATEST(
        COALESCE(total_dials_s1, 0),
        COALESCE(total_dials_s2, 0)
        -- COALESCE(total_dials_s3, 0)
    ) AS total_dials,
    
    -- Find the MAXIMUM count across the three RPC columns
    GREATEST(
        COALESCE(rpc_count_s1, 0),
        COALESCE(rpc_count_s2, 0)
        -- COALESCE(rpc_count_s3, 0)
    ) AS rpc_count,
    
    -- Find the MAXIMUM count across the three Payment columns
    GREATEST(
        COALESCE(payment_count_s1, 0),
        COALESCE(payment_count_s2, 0)
        -- COALESCE(payment_count_s3, 0)
    ) AS payment_count,
    
    -- Find the MAXIMUM count across the three PTP columns
    GREATEST(
        COALESCE(ptp_count_s1, 0),
        COALESCE(ptp_count_s2, 0)
        -- COALESCE(ptp_count_s3, 0)
    ) AS ptp_count  
FROM with_disposition
)l
),
fmd_agg AS (
  SELECT
      fmd.fbbid
    , fmd.edate
    , MAX(fmd.dpd_days)     AS dpd_days        
    , MAX(fmd.dpd_bucket)   AS dpd_bucket
    , SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans
    , MAX(fmd.is_charged_off) AS is_charged_off_any
  FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
  -- WHERE fmd.edate = CURRENT_DATE
  GROUP BY fmd.fbbid, fmd.edate
),
With_cure as
(
select a.*,
case when a.dpd_bucket > b.dpd_bucket then 1 else 0 end as dpd_improvement,
case when b.dpd_bucket = 0 then 1 else 0 end as cured,
case when c.dpd_bucket = 0 then 1 else 0 end as cured_by_7_day,
c.edate as cured_7_edate,
b.dpd_bucket as next_day_dpd,
b.is_charged_off_any
from final_data a
left join fmd_agg b 
on a.dialer_fbbid = b.fbbid 
and a.edate = dateadd(day,-1,b.edate)
left join fmd_agg c 
on a.dialer_fbbid = c.fbbid 
and a.edate = dateadd(day,-7,c.edate)
),
total_overdue AS (
SELECT
    T2.EDATE AS OD_date,
    T1.FBBID,
    T1.LOAN_KEY,
    SUM(T1.STATUS_VALUE) AS total_overdue_balance
FROM BI.FINANCE.LOAN_STATUSES T1
JOIN BI.INTERNAL.DATES T2
    ON T2.EDATE BETWEEN T1.FROM_DATE AND T1.TO_DATE
    --AND DAYOFWEEK(T2.EDATE) = 3
    AND T2.EDATE <= CURRENT_DATE
JOIN (
    SELECT DISTINCT loan_key, edate
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE loan_operational_status <> 'CNCL'
    AND is_charged_off = 0
  ) t3
    ON t3.loan_key = T1.loan_key
    AND t3.edate = T2.EDATE
WHERE T1.STATUS_NAME = 'APD_LOAN_TOTAL_AMOUNT'
GROUP BY 1,2,3
),
overdue_balance as
(
Select 
fbbid,
OD_date,
sum(total_overdue_balance) as tob
from total_overdue
-- where week_end_date = '2025-09-17'
group by all
),
with_overdue as 
(
select a.*,
b.tob 
from with_cure a
left join overdue_balance b
on a.dialer_fbbid = b.fbbid 
and a.edate = b.OD_date
),
Payment as 
(
SELECT fbbid,
date(transaction_transmission_time) as payment_date,
sum(payment_total_amount) as payment
FROM ANALYTICS.CREDIT.AGENT_COLLECTION_V2
WHERE PAYMENT_TOTAL_AMOUNT > '0'
group by 1,2
),
with_payment as 
(
select l.*,
case when rpc_count>0 then 1 else 0 end as rpc_flag,
case when ptp_count >0 then 1 else 0 end as ptp_flag,
case when connected_final>0 then 1 else 0 end as connected_flag
from 
    (
    select a.*,
    b.payment
    from with_overdue a
    left join payment b 
    on a.dialer_fbbid = b.fbbid 
    and a.edate = b.payment_date
    )l
)
select * from with_payment; 

--where ca = '2025-11-10'


