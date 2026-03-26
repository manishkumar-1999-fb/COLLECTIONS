CREATE OR REPLACE TASK TABLEAU.CREDIT.run_collections_dialer_charge_off_list_table
    WAREHOUSE = KEY_METRICS_WH
    SCHEDULE = 'USING CRON 30 16 * * * Asia/Kolkata'
AS
BEGIN



CREATE OR REPLACE TABLE tableau.credit.collections_dialer_charge_off AS
WITH fmd_agg AS (
  SELECT
      fmd.fbbid
    , fmd.edate
    , max(charge_off_date) as charge_off_date
    , MAX(fmd.dpd_days)     AS dpd_days        
    , MAX(fmd.dpd_bucket)   AS dpd_bucket
    , SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans
    , MAX(fmd.is_charged_off) AS is_charged_off_any
  FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
  WHERE fmd.edate = CURRENT_DATE
  GROUP BY fmd.fbbid, fmd.edate
)
,
latest_per_cust AS (
  SELECT
      a.fbbid
    , a.edate
    ,a.charge_off_date
    , a.dpd_days
    , a.dpd_bucket
    , a.outstanding_balance_due_all_loans
    , cd.zip_code as Business_Addr1_ZIP5
    , cd.partner_attribution
    , cd.phone as original_phone_number
  FROM fmd_agg a
  LEFT JOIN bi.public.customers_data cd
    ON a.fbbid = cd.fbbid
  WHERE a.is_charged_off_any = 1
)
-- filtering for customers who are not sent to agency
, delq AS (
  SELECT
      a.fbbid
    , a.edate
    , a.charge_off_date
    , a.dpd_days
    , a.dpd_bucket
    , a.partner_attribution
    , a.outstanding_balance_due_all_loans AS outstanding_balance_due
    , a.Business_Addr1_ZIP5
    ,a.original_phone_number
    ,b.recovery_suggested_state
    ,b.recovery_suggested_substate
  FROM latest_per_cust a
  left join bi.public.daily_approved_customers_data b
  on a.fbbid = b.fbbid and a.edate= b.edate  
  and recovery_max_suggested_state is not null
),
-- Since this is audit script, we only want the customers who are not on current dialer inventory but should be
-- Hence below logic from row 61 to 163 is used to create dataset labelld "dialer_inventor" that is customers who are already on the current dialer
-- We will supress the customers from "delq" dataset who are present on the current "dialer_inventory" dataset
-- This will give us only those customers who are not present on the current dialer inventory but should be.
-- In the next step, we'll need to supress the customers based on their latest call dispositions and hence finally arrving at final audit list customers to be added to the dialer
--Collection Dialer inventory logic for dpd 1-2
-- base with flags
final_base AS (
  SELECT d.*
  -- t2.dpd_bucket as dialer_bucket,
  --   CASE WHEN (t2.fbbid IS NULL )THEN 0 ELSE 1 END AS dialer_present_flag,
  --   CASE WHEN ((t2.last_ob_attempt_date IS NULL 
  -- OR t2.last_ob_attempt_date < t2.last_delq_time::date) ) THEN 0 ELSE 1 END AS call_flag -- call_flag to determine if the customer has recieved a call after his latest deliqunecy update date (currently not being used in the logic)
  FROM delq d 
  -- LEFT JOIN dialer_inventory t2 
  -- ON d.fbbid=t2.fbbid  
)
-- Select * from final_base;
-- select * from final_base
-- where charge_off_date >='2024-01-01' and recovery_suggested_substate is null;
-- Consolidate call logs from four different data sources to ensure no disposition data is lost.
-- source_1 for dispositions is created using analytics.credit.v_five9_call_log
,Source_1 as
(
Select 
FBBID,
Date_call as call_date,
Call_type,
Campaign,
CAMPAIGN_TYPE,
CONTACTED,
Disposition,
List_name,
Dialer_status,
DPD_BUCKET,
--count(*) as call_attempts
from analytics.credit.v_five9_call_log
where date_call < CURRENT_DATE
-- qualify row_number() over(partition by fbbid order by date_call desc) = 1
 qualify row_number() over(
         partition by fbbid 
         order by date_call desc,
                  CASE 
                      WHEN TRIM(LOWER(disposition)) IN ('do not call','dnc','invalid number','bankruptcy','settlement accepted') THEN 1
                      WHEN TRIM(LOWER(disposition)) IN ('d-promise to pay','promise to pay','b-payment','payment') THEN 2
                      WHEN TRIM(LOWER(disposition)) IN ('f-rpc','rpc') THEN 3
                      ELSE 4
                  END ASC
     ) = 1 
--where campaign_type = 'Outbound'
),

-- source_2 for dispositions is created using ANALYTICS.CREDIT.FIVE9_CALL_LOG_REPORT
Source_2 as 
(
SELECT
        cd.fbbid AS fbbid,
        _data:"Fundbox_ID"::int AS dialer_fbbid,
        _DATA:DNIS AS dialer_phone_number,
        _data:DISPOSITION AS disposition,
        CASE 
            WHEN TRIM(LOWER(disposition)) LIKE '%promise to pay%' OR TRIM(LOWER(disposition)) LIKE '%ptp%' THEN 'PTP'
            WHEN TRIM(LOWER(disposition)) LIKE '%payment%' THEN 'Payment'
            WHEN TRIM(LOWER(disposition)) LIKE '%settlement%' THEN 'Settlement'
            when TRIM(LOWER(disposition)) LIKE '%invalid number%' THEN 'Invalid Number'
            WHEN TRIM(LOWER(disposition)) LIKE '%no answer%' OR TRIM(LOWER(disposition)) LIKE '%not answer%' 
                 OR TRIM(LOWER(disposition)) LIKE '%voicemail%' OR TRIM(LOWER(disposition)) LIKE '%answering machine%' 
                 OR TRIM(LOWER(disposition)) LIKE '%hangup%' OR TRIM(LOWER(disposition)) LIKE '%busy%' 
                 OR TRIM(LOWER(disposition)) LIKE '%abandon%' OR TRIM(LOWER(disposition)) LIKE '%dial error%' 
                 OR TRIM(LOWER(disposition)) LIKE '%caller disconnected%' 
                 THEN 'No Answer'
            WHEN TRIM(LOWER(disposition)) LIKE '%no disposition%' THEN 'No Disposition'
            WHEN TRIM(LOWER(disposition)) LIKE '%rpc%' THEN 'RPC'
            ELSE 'Others'
        END AS customer_call_outcome,
        TO_DATE(_data:"DATE"::VARCHAR, 'YYYY/MM/DD') AS call_date,
        _data:CAMPAIGN AS campaign,
        _data:"CALL TYPE" AS calltype,
        _DATA:"CALL ID" AS call_id,
        _DATA:CONTACTED::boolean AS contacted,
        CASE 
            WHEN TRIM(LOWER(disposition)) LIKE '%no answer%' OR TRIM(LOWER(disposition)) LIKE '%not answer%' 
                 OR TRIM(LOWER(disposition)) LIKE '%voicemail%' OR TRIM(LOWER(disposition)) LIKE '%answering machine%' 
                 OR TRIM(LOWER(disposition)) LIKE '%hangup%' OR TRIM(LOWER(disposition)) LIKE '%busy%' 
                 OR TRIM(LOWER(disposition)) LIKE '%abandon%' OR TRIM(LOWER(disposition)) LIKE '%dial error%' 
                 OR TRIM(LOWER(disposition)) LIKE '%invalid number%' OR TRIM(LOWER(disposition)) LIKE '%caller disconnected%' 
                 THEN 0
            ELSE 1
        END AS disposition_contacted
    FROM bi.public.customers_data cd
    LEFT JOIN ANALYTICS.CREDIT.FIVE9_CALL_LOG_REPORT fr
        ON REGEXP_REPLACE(cd.phone, '\\D', '') = _DATA:DNIS::string
    WHERE TRIM(LOWER(_DATA:CAMPAIGN::string)) LIKE '%collections%'
      AND TRIM(LOWER(_DATA:"CALL TYPE"::string)) IN ('outbound', 'manual')
      and call_date < CURRENT_DATE
     qualify row_number() over(
         partition by fbbid 
         order by call_date desc,
                  CASE 
                      WHEN TRIM(LOWER(disposition)) IN ('do not call','dnc','invalid number','bankruptcy','settlement accepted') THEN 1
                      WHEN TRIM(LOWER(disposition)) IN ('d-promise to pay','promise to pay','b-payment','payment') THEN 2
                      WHEN TRIM(LOWER(disposition)) IN ('f-rpc','rpc') THEN 3
                      ELSE 4
                  END ASC
     ) = 1 
),

-- source_3 for dispositions is created using tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW (this is the table which is being created and used in current dialer script for call dispositions)
source_3 as 
(
SELECT 
fbbid,
dispostion as disposition ,
last_ob_attempt_date as call_date
from tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW t
where last_ob_attempt_date < CURRENT_DATE
qualify row_number() over(partition by fbbid order by last_ob_attempt_date desc) = 1
),

-- source_4 for dispositions is created using external_data_sources.salesforce_nova.task
source_4 as 
(
SELECT 
TRY_TO_NUMBER(fundbox_id__c) AS fbbid,
calldisposition as disposition ,
LASTMODIFIEDDATE::DATE as call_date
from external_data_sources.salesforce_nova.task sf
where call_date < CURRENT_DATE
qualify row_number() over(
         partition by fbbid 
         order by call_date desc,
                  CASE 
                      WHEN TRIM(LOWER(disposition)) IN ('do not call','dnc','invalid number','bankruptcy','settlement accepted') THEN 1
                      WHEN TRIM(LOWER(disposition)) IN ('d-promise to pay','promise to pay','b-payment','payment') THEN 2
                      WHEN TRIM(LOWER(disposition)) IN ('f-rpc','rpc') THEN 3
                      ELSE 4
                  END ASC
     ) = 1 
),
latest_call AS 
(
Select 
l.*
from 
(
    SELECT fbbid, call_date FROM Source_1
    UNION
    SELECT fbbid, call_date FROM Source_2
    UNION
    SELECT fbbid, call_date FROM Source_3
    UNION
    SELECT fbbid, call_date FROM Source_4
)l
qualify row_number() over(partition by fbbid order by call_date desc) = 1
)
-- select * from latest_call where fbbid in (select fbbid from latest_call group by fbbid having count(*)>1) order by fbbid;
--Mapping call logs from each source with delinquent accounts
-- In base data, now adding latest call dates and dispositions from all 4 datasets created above
,with_dialer as
(
select 
    base.*,
    -- Source 1 data
    s1.disposition as s1_disposition,
    s1.call_date as s1_call_date,
    -- Source 2 data  
    s2.disposition as s2_disposition,
    s2.call_date as s2_call_date,
    -- Source 3 data
    s3.disposition as s3_disposition, 
    s3.call_date as s3_call_date,
    -- Source 4 data
    s4.disposition as s4_disposition,
    s4.call_date as s4_call_date
from final_base base 
left join Source_1 s1 on base.fbbid = s1.fbbid
left join Source_2 s2 on base.fbbid = s2.fbbid  
left join Source_3 s3 on base.fbbid = s3.fbbid
left join Source_4 s4 on base.fbbid = s4.fbbid
),
--Pulling all accounts from all sources where DNC has came in any point of time
DNC_FBBIDS AS (
    -- Source 1: analytics.credit.v_five9_call_log
    SELECT
        distinct FBBID,date_call
    FROM analytics.credit.v_five9_call_log
    WHERE date_call < CURRENT_DATE
      AND TRIM(LOWER(Disposition)) IN ('do not call', 'dnc')

    UNION
    -- Use UNION (distinct) here to automatically remove duplicate FBBIDs across sources.

    -- Source 2: ANALYTICS.CREDIT.FIVE9_CALL_LOG_REPORT (via bi.public.customers_data)
    SELECT
        distinct cd.fbbid AS fbbid,
        TO_DATE(_data:"DATE"::VARCHAR, 'YYYY/MM/DD') AS call_date,
    FROM bi.public.customers_data cd
    LEFT JOIN ANALYTICS.CREDIT.FIVE9_CALL_LOG_REPORT fr
        ON REGEXP_REPLACE(cd.phone, '\\D', '') = _DATA:DNIS::string
    WHERE call_date < CURRENT_DATE
      AND TRIM(LOWER(_data:DISPOSITION)) IN ('do not call', 'dnc')

    UNION

    -- Source 3: tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW
    SELECT
        distinct fbbid,last_ob_attempt_date 
    FROM tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW
    WHERE last_ob_attempt_date < CURRENT_DATE
      AND TRIM(LOWER(dispostion)) IN ('do not call', 'dnc')

    UNION

    -- Source 4: external_data_sources.salesforce_nova.task
    SELECT
        distinct TRY_TO_NUMBER(fundbox_id__c) AS fbbid,lastmodifieddate
    FROM external_data_sources.salesforce_nova.task
    WHERE LASTMODIFIEDDATE::DATE < CURRENT_DATE
      AND TRIM(LOWER(calldisposition)) IN ('do not call', 'dnc')
),
-- Final result: Distinct list of all FBBIDs that have a DNC disposition in their history
DNC_accounts as 
(
SELECT
    distinct fbbid,date_call,
    1 AS dnc_flag
FROM DNC_FBBIDS
GROUP BY 1,2
qualify row_number() over(partition by fbbid order by date_call) = 1
),
settlement_failed as (
Select 
fbbid,
settlement_created_date,
settlement_end_time,
current_status,
from analytics.credit.cjk_v_backy_settlements
where current_status in ('FAILED','CANCELLED') and settlement_created_date >'2023-03-01'
qualify row_number() over(partition by fbbid order by event_time desc) = 1
),
settlement_funded as (
Select 
fbbid,
settlement_created_date,
settlement_end_time,
current_status,
from analytics.credit.cjk_v_backy_settlements 
where current_status = 'FUNDED' and settlement_created_date >'2023-03-01'
qualify row_number() over(partition by fbbid order by event_time desc) = 1
),
settlement_processing as (
Select 
fbbid,
CORRECTED_CREATED_TIME as settlement_created_date,
settlement_end_time,
current_status,
from analytics.credit.cjk_v_backy_settlements 
where current_status = 'ACTIVE' and settlement_created_date >'2023-03-01'
qualify row_number() over(partition by fbbid order by event_time desc) = 1
)
,
with_payments as
(
select a.fbbid,
b.payment_total_amount,
date(payment_last_status_change_time) as payment_last_status_change_time,
datediff(month,date(payment_last_status_change_time),current_date) as months_since_last_payment
from with_dialer a
left join bi.finance.payments_data b
on a.fbbid = b.fbbid 
and a.charge_off_date<b.payment_last_status_change_time
where payment_status = 'FUND'
qualify row_number() over(partition by a.fbbid order by b.payment_last_status_change_time desc) = 1
),

--Creating flags for disposition, bankruptcy and settlement from all 4 data sources and creating a suppression flag based on latest available call disposition 
final_data AS (
SELECT 
    f.*,
    dacd.is_bankruptcy,
    dacd.bankruptcy_status,
    dnc.dnc_flag,
    sf.current_status as failed_status,
    sp.settlement_created_date,
    sp.current_status as active_current_status,
    sf.settlement_end_time,
    dacd.sub_product,
    Case when dacd.sub_product = 'LOAN_API_TERM_LOAN' then 1 else 0 end Term_loan_exclusion,
    case when sfu.settlement_end_time is not null then 1 else 0 end as settlement_funded_flag,
    case 
    when (sp.settlement_created_date is not null and sf.settlement_end_time is null) then 1
    when (sp.settlement_created_date >= sf.settlement_end_time) then 1 else 0 end settlement_active_flag,
    CASE WHEN (dnc.dnc_flag = 1) then 1 else 0 end as flag_is_dnc,
    CASE WHEN (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS')) THEN 1 ELSE 0 END AS flag_is_bankruptcy,
    -- Source 1 suppression flags
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('do not call','dnc','invalid number')) THEN 1 ELSE 0 END AS s1_flag_hard_disposition_exclusion,

    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('settlement accepted')) and 
    (sp.settlement_created_date is not null and sp.settlement_created_date >= s1_call_date) 
    and (sf.settlement_end_time is null or sf.settlement_end_time <= s1_call_date and s1_call_date>'2023-03-01') 
    AND (sf.settlement_end_time IS NULL OR sf.settlement_end_time >= sp.settlement_created_date)
    THEN 1 ELSE 0 END AS s1_flag_settlement_exclusion,

    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s1_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s1_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s1_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s1_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('b-payment','payment') AND (DATEDIFF(day, s1_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s1_flag_payment_datediff,
    CASE WHEN (s1_disposition) IN ('Third Party','G-Third Party') AND (DATEDIFF(day, s1_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s1_flag_third_party_datediff,
    Case when ((TRIM(LOWER(s1_disposition)) = 'bankruptcy' and s1_call_date>'2023-01-01') and (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS'))) then 1 else 0 end as s1_flag_bankruptcy_exclusion,
    
    -- Source 2 suppression flags
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('do not call','dnc','invalid number')) THEN 1 ELSE 0 END AS s2_flag_hard_disposition_exclusion,
        
    CASE 
    WHEN (TRIM(LOWER(s2_disposition)) ilike ('%settlement accepted%'))
    and (sp.settlement_created_date is not null and sp.settlement_created_date >= s2_call_date) 
    and (sf.settlement_end_time is null or sf.settlement_end_time <= s2_call_date and s2_call_date>'2023-03-01') 
    AND (sf.settlement_end_time IS NULL OR sf.settlement_end_time >= sp.settlement_created_date)
    THEN 1 ELSE 0 END AS s2_flag_settlement_exclusion,
    
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s2_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s2_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s2_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s2_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s2_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s2_flag_payment_datediff,
        CASE WHEN ((s2_disposition)) IN ('"G-Third Party"','"Third Party"') AND (DATEDIFF(day, s2_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s2_flag_third_party_datediff,
            Case when ((TRIM(LOWER(s1_disposition)) = 'bankruptcy' and s2_call_date>'2023-01-01') and (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS'))) then 1 else 0 end as s2_flag_bankruptcy_exclusion,
            
    -- Source 3 suppression flags
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('do not call','dnc','invalid number')) THEN 1 ELSE 0 END AS s3_flag_hard_disposition_exclusion,
        CASE WHEN (TRIM(LOWER(s3_disposition)) ilike ('%A-Settlement accepted%'))and(sp.settlement_created_date is not null and sp.settlement_created_date >= s3_call_date ) and (sf.settlement_end_time is null or sf.settlement_end_time <= s3_call_date and s3_call_date>'2023-03-01') AND (sf.settlement_end_time IS NULL OR sf.settlement_end_time >= sp.settlement_created_date) THEN 1 ELSE 0 END AS s3_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s3_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s3_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s3_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s3_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s3_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s3_flag_payment_datediff,
        CASE WHEN ((s3_disposition)) IN ('G-Third Party','Third Party') AND (DATEDIFF(day, s3_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s3_flag_third_party_datediff,
        Case when ((TRIM(LOWER(s1_disposition)) = 'bankruptcy' and s3_call_date>'2023-01-01') and (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS'))) then 1 else 0 end as s3_flag_bankruptcy_exclusion,
    
    
    -- Source 4 suppression flags
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('do not call','dnc','invalid number')) THEN 1 ELSE 0 END AS s4_flag_hard_disposition_exclusion,
        CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('settlement accepted','Settlement Payment')) and (sp.settlement_created_date is not null and sp.settlement_created_date >= s4_call_date ) and (sf.settlement_end_time is null or sf.settlement_end_time <= s4_call_date and s4_call_date>'2023-03-01') AND (sf.settlement_end_time IS NULL OR sf.settlement_end_time >= sp.settlement_created_date) THEN 1 ELSE 0 END AS s4_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s4_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s4_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s4_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s4_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s4_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s4_flag_payment_datediff,
        CASE WHEN ((s4_disposition)) IN ('G-Third Party','Third Party') AND (DATEDIFF(day, s4_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s4_flag_third_party_datediff,
        Case when ((TRIM(LOWER(s1_disposition)) = 'bankruptcy' and s4_call_date>'2023-01-01') and (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS'))) then 1 else 0 end as s4_flag_bankruptcy_exclusion,

    
    -- Combined suppression flag - 1 if any source has any suppression flag as 1
    CASE 
        WHEN (
            Term_loan_exclusion+ settlement_funded_flag + settlement_active_flag +
            s1_flag_hard_disposition_exclusion + s1_flag_promise_pay_datediff + s1_flag_rpc_datediff + s1_flag_payment_datediff + s1_flag_settlement_exclusion + s1_flag_third_party_datediff +s1_flag_bankruptcy_exclusion+
            s2_flag_hard_disposition_exclusion + s2_flag_promise_pay_datediff + s2_flag_rpc_datediff + s2_flag_payment_datediff + s2_flag_settlement_exclusion + s2_flag_third_party_datediff +s2_flag_bankruptcy_exclusion+
            s3_flag_hard_disposition_exclusion + s3_flag_promise_pay_datediff + s3_flag_rpc_datediff + s3_flag_payment_datediff + s3_flag_settlement_exclusion + s3_flag_third_party_datediff +s3_flag_bankruptcy_exclusion+
            s4_flag_hard_disposition_exclusion + s4_flag_promise_pay_datediff + s4_flag_rpc_datediff + s4_flag_payment_datediff +s4_flag_settlement_exclusion + s4_flag_third_party_datediff +s4_flag_bankruptcy_exclusion
        ) >= 1 THEN 1 
        ELSE 0 
    END AS combined_suppression_flag,
    
    CASE 
        WHEN (  
            flag_is_bankruptcy +
            combined_suppression_flag +
            flag_is_dnc            
        ) >=1 then 0
        --when cfs.fbbid is not null then 0
        when  (TRIM(LOWER(partner_attribution)) = 'autobooks' or TRIM(LOWER(partner_attribution)) LIKE 'stripe_au_mca' ) then 0
        ELSE 1
    END AS final_inclusion_flag,
    wp.payment_total_amount,
    wp.payment_last_status_change_time,
    wp.months_since_last_payment,
    sfu.settlement_end_time as settlement_funded_date
FROM with_dialer f
LEFT JOIN tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW t
ON f.fbbid=t.fbbid
left join bi.public.daily_approved_customers_data dacd 
on f.fbbid = dacd.fbbid and f.edate = dacd.edate
left join dnc_accounts dnc
on f.fbbid = dnc.fbbid
left join settlement_failed sf
on f.fbbid = sf.fbbid
left join with_payments wp 
on f.fbbid = wp.fbbid
left join settlement_funded sfu
on f.fbbid = sfu.fbbid
left join settlement_processing sp
on f.fbbid = sp.fbbid
)
,
-- data created in below step is loaded into dialer audit list on tableau and below suppression flags are applied on tableau dashboard before publishing
-- Flags on tableau dashboard from below data:
-- 1. FLAG_IS_BANKRUPTCY=0 (if the customer is bankrupt, then he is not included in the dialer audit list)
-- 2. FLag_IS_Settled=0 (if the customer is settled, then he is not included in the dialer audit list)
-- 3. FINAL_INCLUSION_FLAG=1 (if the customer is not suppressed by any of the suppression, then he is included in the dialer audit list)
-- 4. DIALER_PRESENT_FLAG=0 (if the customer is present in the current dialer inventory, then he is not included in the dialer audit list)
-- 5. DNC_FLag = 0(if the customer is DNC, then he is not included in the dialer audit list)
final_table as 
(
Select distinct
fbbid,
edate,
charge_off_date,
year(charge_off_date) as charge_off_year,
recovery_suggested_state,
recovery_suggested_substate,
dpd_days::INTEGER AS dpd_days,
TRY_TO_NUMBER(dpd_bucket::VARCHAR) AS dpd_bucket,
partner_attribution,
OUTSTANDING_BALANCE_DUE as current_os,
s1_disposition,
s2_disposition,
s3_disposition,
s4_disposition,
REGEXP_REPLACE(original_phone_number, '[^0-9]', '') as original_phone_number,
Business_Addr1_ZIP5,
s1_call_date as s1_last_disposition_date,
s2_call_date as s2_last_disposition_date,
s3_call_date as s3_last_disposition_date,
s4_call_date as s4_last_disposition_date,
settlement_end_time,
payment_total_amount,
payment_last_status_change_time,
months_since_last_payment,
CASE WHEN s1_flag_hard_disposition_exclusion = 1 THEN 1
    When s1_flag_settlement_exclusion =1 Then 1
    WHEN s1_flag_promise_pay_datediff = 1 THEN 1
    WHEN s1_flag_rpc_datediff = 1 THEN 1
    WHEN s1_flag_payment_datediff = 1 THEN 1
    when s1_flag_third_party_datediff = 1 Then 1
    when s1_flag_bankruptcy_exclusion = 1 then 1
    ELSE 0
END AS s1_suppression_flag,
CASE WHEN s2_flag_hard_disposition_exclusion = 1 THEN 1
    WHEN s2_flag_settlement_exclusion =1 Then 1 
    WHEN s2_flag_promise_pay_datediff = 1 THEN 1
    WHEN s2_flag_rpc_datediff = 1 THEN 1
    WHEN s2_flag_payment_datediff = 1 THEN 1
    WHEN s2_flag_third_party_datediff = 1 Then 1
    when s2_flag_bankruptcy_exclusion = 1 then 1
    ELSE 0
END AS s2_suppression_flag,
CASE WHEN s3_flag_hard_disposition_exclusion = 1 THEN 1
    WHEN s3_flag_settlement_exclusion =1 Then 1 
    WHEN s3_flag_promise_pay_datediff = 1 THEN 1
    WHEN s3_flag_rpc_datediff = 1 THEN 1
    WHEN s3_flag_payment_datediff = 1 THEN 1
    WHEN s3_flag_third_party_datediff = 1 Then 1
    When s3_flag_bankruptcy_exclusion = 1 then 1
    ELSE 0
END AS s3_suppression_flag,
CASE WHEN s4_flag_hard_disposition_exclusion = 1 THEN 1
    WHEN s4_flag_settlement_exclusion =1 Then 1 
    WHEN s4_flag_promise_pay_datediff = 1 THEN 1
    WHEN s4_flag_rpc_datediff = 1 THEN 1
    WHEN s4_flag_payment_datediff = 1 THEN 1
    WHEN s4_flag_third_party_datediff = 1 Then 1
    When s4_flag_bankruptcy_exclusion = 1 then 1
    ELSE 0
END AS s4_suppression_flag,
case when outstanding_balance_due <5000 then '1.<5k'
    when outstanding_balance_due >=5000 and outstanding_balance_due<10000 then '2.5k-10k'
    when outstanding_balance_due>=10000 and outstanding_balance_due<20000 then '3. 10k-20k'
    else '20k+' end as OS_Principal_Bucket,
--case when OFFER_DATE is not null then 1 else 0 end as FLag_IS_Settled,
FLAG_IS_BANKRUPTCY,	
FINAL_INCLUSION_FLAG,
flag_is_dnc,
settlement_funded_date,
settlement_created_date,
failed_status,
active_current_status
from final_data
WHERE outstanding_balance_due>0 and RECOVERY_SUGGESTED_STATE not in ('ELR', 'EOL') 
--and dpd_bucket = 14
and year(charge_off_date)  >=2024
)
,
final_table2 as
(
select *,
case 
    when current_os <= 1000 then '1.0-1000'
    when current_os > 1000 and current_os <= 2000 then '2.1001-2000'
    when current_os > 2000 and current_os <= 3000 then '3.2001-3000'
    when current_os > 3000 and current_os <= 5000 then '4.3001-5000'
    when current_os > 5000 and current_os <= 10000 then '5.5000-10000' 
    else '6.10000+'
    end as OS_bucket,
case when settlement_end_time > charge_off_date then 1 else 0 end as settlement_failed_flag,
datediff(day,settlement_end_time,current_date) as days_since_failed_settlement,
case 
WHEN TRIM(LOWER(S1_DISPOSITION)) ilike '%payment%' or TRIM(LOWER(S1_DISPOSITION)) ilike '%promise to pay%'
or TRIM(LOWER(S1_DISPOSITION)) ilike '%RPC%' or TRIM(LOWER(S1_DISPOSITION)) ilike '%settlement accepted%'
or TRIM(LOWER(S1_DISPOSITION)) ilike '%Settlement payment%' then S1_LAST_DISPOSITION_DATE end as S1_rpc_date,
case 
WHEN TRIM(LOWER(S2_DISPOSITION)) ilike '%payment%' or TRIM(LOWER(S2_DISPOSITION)) ilike '%promise to pay%'
or TRIM(LOWER(S2_DISPOSITION)) ilike '%RPC%' or TRIM(LOWER(S2_DISPOSITION)) ilike '%settlement accepted%'
or TRIM(LOWER(S2_DISPOSITION)) ilike '%Settlement payment%' then S2_LAST_DISPOSITION_DATE end as S2_rpc_date,
case 
WHEN TRIM(LOWER(S3_DISPOSITION)) ilike '%payment%' or TRIM(LOWER(S3_DISPOSITION)) ilike '%promise to pay%'
or TRIM(LOWER(S3_DISPOSITION)) ilike '%RPC%' or TRIM(LOWER(S3_DISPOSITION)) ilike '%settlement accepted%'
or TRIM(LOWER(S3_DISPOSITION)) ilike '%Settlement payment%' then S3_LAST_DISPOSITION_DATE end as S3_rpc_date,
case 
WHEN TRIM(LOWER(S4_DISPOSITION)) ilike '%payment%' or TRIM(LOWER(S4_DISPOSITION)) ilike '%promise to pay%'
or TRIM(LOWER(S4_DISPOSITION)) ilike '%RPC%' or TRIM(LOWER(S4_DISPOSITION)) ilike '%settlement accepted%'
or TRIM(LOWER(S4_DISPOSITION)) ilike '%Settlement payment%' then S4_LAST_DISPOSITION_DATE end as S4_rpc_date  
from final_table
),
final_table3 as 
(
select l.*,
datediff(day,rpc_date,current_date) as days_since_RPC,
datediff(day,payment_last_status_change_time,current_date) as days_since_payment,
case when payment_last_status_change_time> charge_off_date then 1 else 0 end as payment_flag,
case when rpc_date> charge_off_date then 1 else 0 end as rpc_flag
from 
    (
    select *,
    greatest(s1_rpc_date,s2_rpc_date,s3_rpc_date,s4_rpc_date) as rpc_date
    from final_table2
    )l
)
--select * from final_table3;
,final_table4 as 
(
select l.*,
case when call_time_flag = 'Other' then 1 else 2 end as Call_group
from 
    (select a.*,
    b.call_date,
    CASE 
     -- Flag if call was Yesterday
        WHEN b.call_date = CURRENT_DATE - 1 THEN 'Called Yesterday'        
    -- Generic "Last Friday" logic regardless of today's date
        WHEN b.call_date = DATE_TRUNC('WEEK', CURRENT_DATE) - 3 THEN 'Called Last Friday'      
        ELSE 'Other'
        END AS call_time_flag
    from final_table3 a
    left join latest_call b
    on a.fbbid = b.fbbid
    )l
)
,
flags as
(
select l.*,
row_number() over(order by priority_flag asc, current_os desc) as priority_flag_final
from 
(
    select *,
    case 
    when call_group = 1 then 1
    when rpc_flag = 1 and payment_flag = 1 and SETTLEMENT_FAILED_FLAG = 1 then 2
    when SETTLEMENT_FAILED_FLAG = 1 and payment_flag = 1 and rpc_flag =0 then 3
    when SETTLEMENT_FAILED_FLAG = 1 and payment_flag = 0 and rpc_flag =0 then 4
    when SETTLEMENT_FAILED_FLAG = 0 and payment_flag = 1 and rpc_flag =1 then 5
    when SETTLEMENT_FAILED_FLAG = 1 and payment_flag = 0 and rpc_flag =0 then 6
    when SETTLEMENT_FAILED_FLAG = 0 and payment_flag = 1 and rpc_flag =0 then 7
    when SETTLEMENT_FAILED_FLAG = 0 and payment_flag = 0 and rpc_flag =1 then 8
    when SETTLEMENT_FAILED_FLAG = 0 and payment_flag = 0 and rpc_flag = 0 then 9
    else 10 end as priority_flag
    from final_table4
    where charge_off_year >=2024 
    and final_inclusion_flag = 1
)l
)
select * from flags 
where current_os>=100
order by priority_flag_final;
END;
 
 ALTER TASK TABLEAU.CREDIT.run_collections_dialer_charge_off_list_table RESUME;
--Show tasks like 'run_collections_dialer_charge_off_list_table'


Select * from tableau.credit.collections_dialer_charge_off