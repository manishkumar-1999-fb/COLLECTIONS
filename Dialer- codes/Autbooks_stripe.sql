CREATE OR REPLACE TASK TABLEAU.CREDIT.run_collections_dialer_audit_list_table_Autobooks
    WAREHOUSE = KEY_METRICS_WH
    SCHEDULE = 'USING CRON 00 16 * * * Asia/Kolkata'
AS
BEGIN

------------------------------------------------------------------------------------------------
--ALL customers currently Delinquent
CREATE OR REPLACE TABLE tableau.credit.collections_dialer_audit_list_Autobooks AS

-- Select * from tableau.credit.collections_dialer_audit_list_Autobooks

WITH fmd_agg AS (
  SELECT
      fmd.fbbid
    , fmd.edate
    , DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS collection_week
    , MAX(fmd.dpd_days)     AS dpd_days        
    , MAX(fmd.dpd_bucket)   AS dpd_bucket
    , SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans
    , MAX(fmd.is_charged_off) AS is_charged_off_any
  FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
  WHERE fmd.edate = CURRENT_DATE
  GROUP BY fmd.fbbid, fmd.edate
)
,  
all_accounts AS (
  select l.*,
  lag(dpd_bucket,2,null) over(partition by fbbid order by edate) as prev_dpd_bucket,
  --lag(DPD_bucket,1,null) over(partition by fbbid order by collection_week) as prev_week_dpd
  from 
    (     SELECT
          fmd.fbbid
        , fmd.edate
        , DATE_TRUNC('WEEK', edate::DATE + 4)::DATE + 2 AS collection_week
        , MAX(fmd.dpd_days)     AS dpd_days        
        , MAX(fmd.dpd_bucket)   AS dpd_bucket
        , SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans
        , MAX(fmd.is_charged_off) AS is_charged_off_any
      FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
      GROUP BY fmd.fbbid, fmd.edate
    )l
   )
,Weekly_DPD as
(
select l.*,
lag(DPD_bucket,1,null) over(partition by fbbid order by collection_week) as prev_week_dpd
from 
    (
    Select fbbid,
    collection_week,
    min(dpd_bucket) as dpd_bucket
    from all_accounts 
    group by all
    )l
   )  
   
,latest_per_cust AS (
  SELECT
      a.fbbid
    , a.edate
    , a.dpd_days
    , a.dpd_bucket
    , a.outstanding_balance_due_all_loans
    , a.collection_week
    , a.is_charged_off_any
    , cd.zip_code as Business_Addr1_ZIP5
    , cd.partner_attribution
    , cd.phone as original_phone_number
    , cd.sub_product
    , cd.registration_campaign_source
  FROM fmd_agg a
  LEFT JOIN bi.public.customers_data cd
    ON a.fbbid = cd.fbbid
  WHERE cd.sub_product = 'mca' or cd.partner_attribution in ('Relay') or sub_product = 'LOAN_API_TERM_LOAN' or registration_campaign_source = 'Relay'
   or sub_product = 'stripe_au_mca'
  and (dpd_bucket >0)
  --a.is_charged_off_any = 0 
)
--Select * from latest_per_cust;


--Select * from latest_per_cust;
-- filtering for customers who are in dpd 1 to 98 days
, delq AS (
  SELECT
      a.fbbid
    , a.edate
    , a.dpd_days
    , a.dpd_bucket
    , a.partner_attribution
    , a.is_charged_off_any
    , a.outstanding_balance_due_all_loans AS outstanding_balance_due
    , a.Business_Addr1_ZIP5
    , a.original_phone_number
    , b.prev_dpd_bucket
    , c.prev_week_dpd
    , case when (b.prev_dpd_bucket = 3 and a.dpd_bucket >=3 )
        and a.partner_attribution ilike 'Relay' and sub_product = 'LOAN_API_TERM_LOAN' and registration_campaign_source = 'Relay'
        then 1 else 0 end as Relay_dial_flag
  FROM latest_per_cust a
  left join all_accounts b
  on a.fbbid = b.fbbid 
  and a.edate = b.edate
  left join Weekly_DPD c
  on a.fbbid = c.fbbid
  and a.collection_week = c.collection_week
  WHERE a.dpd_days >= 1
  --   AND a.dpd_days < 98
)
--Select * from delq;

,final_base AS (
  SELECT d.* ,
  case when prev_week_dpd < dpd_bucket then 1 else 0 end as same_week_payment_missed
  FROM delq d 
),
-- Consolidate call logs from four different data sources to ensure no disposition data is lost.
-- source_1 for dispositions is created using analytics.credit.v_five9_call_log
Source_1 as
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
--Mapping call logs from each source with delinquent accounts
-- In base data, now adding latest call dates and dispositions from all 4 datasets created above
with_dialer as
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
)
,
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
where current_status = 'FAILED'
qualify row_number() over(partition by fbbid order by event_time desc) = 1
),
--Creating flags for disposition, bankruptcy and settlement from all 4 data sources and creating a suppression flag based on latest available call disposition 
final_data AS (
SELECT 
    f.*,
    dacd.is_bankruptcy,
    cfs.edate AS offer_date,
    cfs.status_name, 
    cfs.status_value,
    dnc.dnc_flag,
    sf.current_status,
    sf.settlement_end_time,
    Case when cd.sub_product = 'LOAN_API_TERM_LOAN' then 1 else 0 end Term_loan_exclusion,
    CASE WHEN (dnc.dnc_flag = 1) then 1 else 0 end as flag_is_dnc,
    CASE WHEN (dacd.is_bankruptcy = 1 and dacd.bankruptcy_status not in ('DISMISSED_RESUME_COLLECTIONS')) THEN 1 ELSE 0 END AS flag_is_bankruptcy,
    -- Source 1 suppression flags
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('do not call','dnc','invalid number','bankruptcy')) THEN 1 ELSE 0 END AS s1_flag_hard_disposition_exclusion,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('settlement accepted')) and (sf.settlement_end_time is null or sf.settlement_end_time <= s1_call_date) THEN 1 ELSE 0 END AS s1_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s1_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s1_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s1_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s1_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s1_disposition)) IN ('b-payment','payment') AND (DATEDIFF(day, s1_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s1_flag_payment_datediff,
    CASE WHEN (s1_disposition) IN ('Third Party','G-Third Party') AND (DATEDIFF(day, s1_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s1_flag_third_party_datediff,
    
    -- Source 2 suppression flags
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('do not call','dnc','invalid number','bankruptcy')) THEN 1 ELSE 0 END AS s2_flag_hard_disposition_exclusion,
        CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('settlement accepted'))and (sf.settlement_end_time is null or sf.settlement_end_time <= s2_call_date) THEN 1 ELSE 0 END AS s2_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s2_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s2_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s2_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s2_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s2_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s2_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s2_flag_payment_datediff,
        CASE WHEN ((s2_disposition)) IN ('"G-Third Party"','"Third Party"') AND (DATEDIFF(day, s2_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s2_flag_third_party_datediff,    
    -- Source 3 suppression flags
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('do not call','dnc','invalid number','bankruptcy')) THEN 1 ELSE 0 END AS s3_flag_hard_disposition_exclusion,
        CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('settlement accepted'))and (sf.settlement_end_time is null or sf.settlement_end_time <= s3_call_date) THEN 1 ELSE 0 END AS s3_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s3_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s3_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s3_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s3_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s3_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s3_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s3_flag_payment_datediff,
        CASE WHEN ((s3_disposition)) IN ('G-Third Party','Third Party') AND (DATEDIFF(day, s3_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s3_flag_third_party_datediff,
    
    -- Source 4 suppression flags
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('do not call','dnc','invalid number','bankruptcy')) THEN 1 ELSE 0 END AS s4_flag_hard_disposition_exclusion,
        CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('settlement accepted')) and (sf.settlement_end_time is null or sf.settlement_end_time <= s4_call_date) THEN 1 ELSE 0 END AS s4_flag_settlement_exclusion,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('d-promise to pay','promise to pay') AND DATEDIFF(day, s4_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s4_flag_promise_pay_datediff,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('f-rpc','rpc') AND DATEDIFF(day, s4_call_date, f.edate) <=3) THEN 1 ELSE 0 END AS s4_flag_rpc_datediff,
    CASE WHEN (TRIM(LOWER(s4_disposition)) IN ('b-payment','payment','G-Third Party','Third Party') AND (DATEDIFF(day, s4_call_date, f.edate) <= 6)) THEN 1 ELSE 0 END AS s4_flag_payment_datediff,
        CASE WHEN ((s4_disposition)) IN ('G-Third Party','Third Party') AND (DATEDIFF(day, s4_call_date, f.edate) <= 6) THEN 1 ELSE 0 END AS s4_flag_third_party_datediff,
    CASE when  (TRIM(LOWER(cd.partner_attribution)) = 'autobooks' or TRIM(LOWER(cd.partner_attribution)) LIKE 'stripe_au_mca' ) then 0 ELSE 1 END AS PARTNER_FLAG,
    -- Combined suppression flag - 1 if any source has any suppression flag as 1
    CASE 
        WHEN (
              s1_flag_hard_disposition_exclusion + s1_flag_promise_pay_datediff + s1_flag_rpc_datediff + s1_flag_payment_datediff + s1_flag_settlement_exclusion + s1_flag_third_party_datediff +
            s2_flag_hard_disposition_exclusion + s2_flag_promise_pay_datediff + s2_flag_rpc_datediff + s2_flag_payment_datediff + s2_flag_settlement_exclusion + s2_flag_third_party_datediff +
            s3_flag_hard_disposition_exclusion + s3_flag_promise_pay_datediff + s3_flag_rpc_datediff + s3_flag_payment_datediff + s3_flag_settlement_exclusion + s3_flag_third_party_datediff +
            s4_flag_hard_disposition_exclusion + s4_flag_promise_pay_datediff + s4_flag_rpc_datediff + s4_flag_payment_datediff +s4_flag_settlement_exclusion + s4_flag_third_party_datediff
        ) >= 1 THEN 1 
        ELSE 0 
    END AS combined_suppression_flag,
    
    CASE 
        WHEN (  
            flag_is_bankruptcy +
            combined_suppression_flag +
            flag_is_dnc            
        ) >=1 then 0
        when cfs.fbbid is not null then 0
        ELSE 1
    END AS final_inclusion_flag
FROM with_dialer f
LEFT JOIN tableau.credit.SETTLEMENT_MASTER_TABLE1_NEW t
ON f.fbbid=t.fbbid
left join bi.public.daily_approved_customers_data dacd 
on f.fbbid = dacd.fbbid and f.edate = dacd.edate
left join bi.public.customers_data cd 
on f.fbbid = cd.fbbid
left join 
(
SELECT FBBID, EDATE, status_name, status_group, status_value   
FROM bi.finance.customer_finance_statuses WHERE TRIM(LOWER(status_group)) = 'discounted_settlement'
and TRIM(LOWER(status_name)) = 'settlement_status'
qualify row_number() over(partition by fbbid order by EDATE desc) = 1
) cfs
on f.fbbid = cfs.fbbid
and TRIM(LOWER(status_group)) = 'discounted_settlement'
and TRIM(LOWER(status_name)) = 'settlement_status'
left join dnc_accounts dnc
on f.fbbid = dnc.fbbid
left join settlement_failed sf
on f.fbbid = sf.fbbid
)

-- data created in below step is loaded into dialer audit list on tableau and below suppression flags are applied on tableau dashboard before publishing
-- Flags on tableau dashboard from below data:
-- 1. FLAG_IS_BANKRUPTCY=0 (if the customer is bankrupt, then he is not included in the dialer audit list)
-- 2. FLag_IS_Settled=0 (if the customer is settled, then he is not included in the dialer audit list)
-- 3. FINAL_INCLUSION_FLAG=1 (if the customer is not suppressed by any of the suppression, then he is included in the dialer audit list)
-- 4. DIALER_PRESENT_FLAG=0 (if the customer is present in the current dialer inventory, then he is not included in the dialer audit list)
-- 5. DNC_FLag = 0(if the customer is DNC, then he is not included in the dialer audit list)


    Select distinct
    fbbid,
    edate,
    dpd_days::INTEGER AS dpd_days,
    TRY_TO_NUMBER(dpd_bucket::VARCHAR) AS dpd_bucket,
    same_week_payment_missed,
    partner_attribution,
    OUTSTANDING_BALANCE_DUE as current_os,
    s1_disposition,
    s2_disposition,
    s3_disposition,
    s4_disposition,
    PARTNER_FLAG,
    Term_loan_exclusion,
    Relay_dial_flag,
    REGEXP_REPLACE(original_phone_number, '[^0-9]', '') as original_phone_number,
    Business_Addr1_ZIP5,
    s1_call_date as s1_last_disposition_date,
    s2_call_date as s2_last_disposition_date,
    s3_call_date as s3_last_disposition_date,
    s4_call_date as s4_last_disposition_date,
    settlement_end_time,
    CASE WHEN s1_flag_hard_disposition_exclusion = 1 THEN 1
        When s1_flag_settlement_exclusion =1 Then 1
        WHEN s1_flag_promise_pay_datediff = 1 THEN 1
        WHEN s1_flag_rpc_datediff = 1 THEN 1
        WHEN s1_flag_payment_datediff = 1 THEN 1
        when s1_flag_third_party_datediff = 1 Then 1
        ELSE 0
    END AS s1_suppression_flag,
    CASE WHEN s2_flag_hard_disposition_exclusion = 1 THEN 1
        WHEN s2_flag_settlement_exclusion =1 Then 1 
        WHEN s2_flag_promise_pay_datediff = 1 THEN 1
        WHEN s2_flag_rpc_datediff = 1 THEN 1
        WHEN s2_flag_payment_datediff = 1 THEN 1
        WHEN s2_flag_third_party_datediff = 1 Then 1
        ELSE 0
    END AS s2_suppression_flag,
    CASE WHEN s3_flag_hard_disposition_exclusion = 1 THEN 1
        WHEN s3_flag_settlement_exclusion =1 Then 1 
        WHEN s3_flag_promise_pay_datediff = 1 THEN 1
        WHEN s3_flag_rpc_datediff = 1 THEN 1
        WHEN s3_flag_payment_datediff = 1 THEN 1
        WHEN s3_flag_third_party_datediff = 1 Then 1
        ELSE 0
    END AS s3_suppression_flag,
    CASE WHEN s4_flag_hard_disposition_exclusion = 1 THEN 1
        WHEN s4_flag_settlement_exclusion =1 Then 1 
        WHEN s4_flag_promise_pay_datediff = 1 THEN 1
        WHEN s4_flag_rpc_datediff = 1 THEN 1
        WHEN s4_flag_payment_datediff = 1 THEN 1
        WHEN s4_flag_third_party_datediff = 1 Then 1
        ELSE 0
    END AS s4_suppression_flag,
    case when outstanding_balance_due <5000 then '1.<5k'
        when outstanding_balance_due >=5000 and outstanding_balance_due<10000 then '2.5k-10k'
        when outstanding_balance_due>=10000 and outstanding_balance_due<20000 then '3. 10k-20k'
        else '20k+' end as OS_Principal_Bucket,
    case when OFFER_DATE is not null then 1 else 0 end as FLag_IS_Settled,
    FLAG_IS_BANKRUPTCY,	
    FINAL_INCLUSION_FLAG,
    flag_is_dnc
    from final_data
    WHERE outstanding_balance_due>0 ;
    end;
    


-- 
Select * from tableau.credit.collections_dialer_audit_list_Autobooks;
-- where partner_attribution = 'autobooks'

Select * from bi.public.daily_approved_customers_data where fbbid = 3774660 order by edate desc;
 

