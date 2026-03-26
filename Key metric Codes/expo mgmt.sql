CREATE OR REPLACE TEMPORARY TABLE indus.PUBLIC.og_auw_increases_expo AS (
select a.last_modified_time::date as increase_date,
date_trunc('week', increase_date::date+4)::date+2 AS week_end, 
           a.fbbid,
           a.total_limit as current_limit,
           b.TOTAL_LIMIt as previous_limit,
           current_limit - previous_limit as incr_amnt,
           a.reason,
           a.comment,
           a.system_user,
    from CDC_V2.credit.CREDIT_LIMITS as a
    left join CDC_V2.credit.CREDIT_LIMITS as b
    on true
    and a.fbbid  = b.fbbid
    and a.prev_credit_limit_id = b.id
    where true
    and a.comment ilike '%AUW OG%'
);


CREATE OR REPLACE TABLE indus.public.INDUS_customer_experience_metric_helper_expo as
(
SELECT a.fbbid
     , a.week_start
     , a.week_end
     , (CASE WHEN (to_date(a.first_approved_time) between a.week_start and a.week_end ) THEN 1 ELSE 0 END) AS new_approvals
     , (CASE WHEN a.is_chargeoff = 0 and a.account_status LIKE '%suspended%' and a.account_status_reason LIKE '%User request%' THEN 1 ELSE 0 END) account_closed_by_customer
     , (CASE WHEN (a.is_chargeoff = 0 or a.recovery_suggested_state = 'EOL') and a.account_status = 'suspended' and (a.account_status_reason IS NULL or a.account_status_reason <> 'User request') THEN 1 ELSE 0 END) account_closed_by_Fundbox
     , (CASE WHEN a.account_status = 'suspended' and a.account_status_reason in ('Inactive flow', 'Expedited ANC flow', 'Expedited Inactive flow', 'ANC flow') and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week
     , (CASE WHEN a.account_status = 'suspended' and a.account_status_reason = 'Inactive flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_inactive_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN a.account_status = 'suspended' and a.account_status_reason = 'Expedited ANC flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_expedited_anc_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN a.account_status = 'suspended' and a.account_status_reason =  'Expedited Inactive flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_expedited_inactive_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN a.account_status = 'suspended' and a.account_status_reason = 'ANC flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_anc_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN a.ACCOUNT_status='suspended' AND (c.account_status<>'suspended' or c.account_status is null) AND closed_exposure_management_this_week = 0 AND account_closed_by_customer = 0 AND account_closed_by_Fundbox = 0 THEN 1 ELSE 0 END ) closed_accounts_other_reasons
     , (CASE WHEN ( D.co_date IS null or d.co_date::date > a.edate) and a.account_status <> 'suspended' and a.credit_status = 'approved' and 
                  (a.is_locked_dashboard = 0 or a.is_locked_dashboard IS NULL or 
                  (a.is_locked_dashboard = 1 and a.dashboard_status_change_reason = 'full_utilization')) THEN 1 ELSE 0 END) able_to_draw_this_week
     , (CASE WHEN a.IS_CHARGEOFF = 1 AND D.co_date::Date BETWEEN a.week_start AND a.week_end THEN 1 ELSE 0 END) AS charged_off
     , a.credit_limit AS exposure_end_of_week 
     , a.credit_limit_change
     , a.first_approved_credit_limit AS new_credit_limit
     , (CASE WHEN e.accept_tm::date BETWEEN a.week_start and a.week_end and e.incr_amt > 0 THEN 1 ELSE 0 END) AS inactive_offer_accepted
     , (CASE WHEN e.accept_tm::date BETWEEN a.week_start and a.week_end and e.incr_amt > 0 THEN e.incr_amt ELSE 0 END) AS offer_increased_line
     , (CASE WHEN a.underwriting_flow = 'Invoice Clearing' AND 
		a.DASHBOARD_STATUS_CHANGE_REASON in ('bank_account_error','missing_bank_account',
		'accounting_software_disconnected','needs_sync','action_required','mandatory_ds_disconnected') THEN 1 ELSE 0 END) AS disabled_due_to_as_disconnection
	 , (CASE WHEN a.underwriting_flow = 'Direct Draw' 
		AND a.DASHBOARD_STATUS_CHANGE_REASON in ('bank_account_error','missing_bank_account',
		'accounting_software_disconnected','needs_sync','action_required','mandatory_ds_disconnected') THEN 1 ELSE 0 END) AS disabled_due_to_ba_disconnection
	 , (CASE WHEN f.fbbid is not null THEN 1 ELSE 0 END) AS rmr_it_is
	 , (CASE WHEN a.credit_status = 'disabled' AND a.account_status <> 'suspended'
		and a.credit_status_reason ilike '%collection%' THEN 1 ELSE 0 END)
		AS disabled_delq_this_week
	 , (CASE WHEN a.credit_status = 'disabled' THEN 1 ELSE 0 END) AS disabled_due_to_credit_reason
	 , (CASE WHEN a.is_locked_dashboard = 1 and a.dashboard_status_change_reason <> 'full_utilization'
		and disabled_due_to_ba_disconnection = 0 and disabled_due_to_as_disconnection = 0 
		and disabled_due_to_credit_reason = 0 
		THEN 1 ELSE 0 end) disabled_due_to_other_reason
	 , (CASE WHEN a.CREDIT_LIMIT_COMMENT LIKE '%Pre Doc%' AND a.last_increase_time::date BETWEEN a.week_start AND a.week_end then 1 else 0 END) as pre_doc_inc_this_week_flag
	 , (CASE WHEN a.CREDIT_LIMIT_COMMENT LIKE '%Post Doc%' AND a.last_increase_time::date BETWEEN a.week_start AND a.week_end then 1 else 0 END) as post_doc_inc_this_week_flag,
--
---
(CASE WHEN og_auw.increase_date BETWEEN a.week_start AND a.week_end
	AND og_auw.fbbid IS NOT NULL THEN 1 ELSE 0 END) AS og_auw_increases,-----
--
(CASE WHEN og_auw.increase_date BETWEEN a.week_start AND a.week_end
	AND og_auw.incr_amnt > 0 THEN og_auw.incr_amnt ELSE 0 END) AS og_auw_line_increase_amt,
---
ifnull(rmre.ENTER_RMR_THIS_WEEK,0) AS enter_rmr_this_week,
ifnull(rmre.UNRMR_THIS_WEEK,0) AS unrmr_this_week,
---
-- 31Jan John code add
   (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end )
             and a.credit_limit_comment IS null
             and a.credit_limit_reason in ('Credit increase', 'CLIP credit increase')
            THEN 1 
            ELSE 0 END) AS clip_this_week,
---
   (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end )
            and a.credit_limit_comment IS null
            and a.credit_limit_reason in ('Credit increase', 'CLIP credit increase')                
         THEN a.credit_limit_change 
            else 0 
            END) AS clip_increased_line_this_week,
---
  (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end)
             and a.credit_limit_reason = 'Credit increase' 
             and (a.credit_limit_comment ilike '%boost%' or a.credit_limit_comment ilike '%boots%' or a.credit_limit_comment ilike '%xl%')
            THEN 1 
            ELSE 0 
             END) AS boost_xl_this_week,
   ------
---
  (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end) and a.credit_limit_reason = 'Credit increase' 
             and (a.credit_limit_comment ilike '%boost%' or a.credit_limit_comment ilike '%boots%' or a.credit_limit_comment ilike '%xl%')
            THEN a.credit_limit_change ELSE 0
             END) AS boost_xl_increased_line_this_week,
  (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end)
             and a.credit_limit_reason = 'Testing' 
             and (a.credit_limit_comment ilike '%backbook batch increase%')
            THEN 1 
            ELSE 0 
             END) AS backbook_batch_increase_this_week,
  ------- 
---
  (CASE WHEN (a.last_increase_time::date BETWEEN a.week_start and a.week_end) and a.credit_limit_reason = 'Testing' 
             and (a.credit_limit_comment ilike 'Testing' or a.credit_limit_comment ilike '%backbook batch increase%')
            THEN a.credit_limit_change ELSE 0
             END) AS backbook_increased_line_this_week
             ------
	,(CASE WHEN a.account_status = 'active' AND a.credit_status = 'approved' AND a.credit_limit_reason = 'Credit increase - Spot CLIP' THEN 1 ELSE 0 END) AS is_spot_clip
	,(CASE WHEN a.account_status = 'active' AND a.credit_status = 'approved' AND a.credit_limit_comment = 'update credit and fees and turn on FF' THEN 1 ELSE 0 END) AS is_intuit_52_week_test
	,(CASE WHEN a.account_status = 'active' AND a.credit_status = 'approved' AND a.credit_limit_comment = 'ECLIP' THEN 1 ELSE 0 END) AS is_eclip
   ,(CASE WHEN a.account_status = 'active' AND a.credit_status = 'approved' AND a.credit_limit_reason = 'Credit decrease - Spot CLIP' THEN 1 ELSE 0 END) AS is_spot_clip_reversed
   , (CASE WHEN a.CREDIT_LIMIT_REASON LIKE '%Credit decrease%' AND a.last_decrease_time::date BETWEEN a.week_start AND a.week_end THEN 1 ELSE 0 END ) credit_decrease_this_week
   , (CASE WHEN a.CREDIT_LIMIT_REASON LIKE 'Credit decrease - Automated CLD' AND a.last_decrease_time::date BETWEEN a.week_start AND a.week_end THEN 1 ELSE 0 END ) credit_decrease_this_week_auto
--
FROM
(SELECT *,
-- LAST_DAY(edate, 'month') as month_end,
-- last_day(dateadd('month',-1,edate)) as month_start_helper,
-- dateadd('day',1, month_start_helper) as month_start
date_trunc('week', EDATE::date+4)::date-4 AS week_start,
CASE
WHEN edate = current_date() AND dayofweek(edate) <> 3 THEN NULL
WHEN datediff('day', edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
ELSE DATE_TRUNC('WEEK', edate::date+4)::date+2
END week_end,
dateadd('week',-1,week_end) as week_end_helper
from BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA
where week_end >= '2020-12-31' 
AND is_test_user = 0
AND edate = week_end
AND sub_product <> 'Credit Builder'
AND sub_product <> 'mca'
)a
LEFT JOIN
BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA c
ON a.fbbid = c.fbbid and c.edate = a.week_end_helper
LEFT JOIN
(select fundbox_id__c as fbbid,
createddate::date as offer_dt,
retention_offer__c as offer,
new_cl_offer__c as goto_cl,
cl_offer_delta__c as incr_amt,
offer_status__c as offer_status,
offer_accepted_date__c as accept_tm,
OFFERED_NEW_FEE_RATE_12_WEEKS__C as new_12w_pct,
OFFERED_NEW_FEE_RATE_24_WEEKS__C as new_24w_pct,
--Test as tc_flag,
b.credit_limit as icl
--from- external_data_sources.salesforce_nova.offer__c as a
from indus."PUBLIC".salesforce_nova_offer__c as a
--left join bi.public.daily_approved_customers_data as b
left join BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA as b
on  a.fundbox_id__c = b.fbbid
and b.edate = dateadd('day', -1, a.createddate::date)
--inner join analytics.credit.project_snow_rounds as c
inner join indus."PUBLIC".project_snow_rounds as c
on  a.fundbox_id__c = c.fbbid
and a.retention_enabled_date__c::date = c.retention_enabled_date
where  retention_enabled_date__c::date >= '2023-05-01'
) e
ON a.fbbid = e.fbbid
/*LEFT JOIN 
(SELECT *
from indus.public.INDUS_AUW_V6
where auw_pre_doc_review_complete_date IS NOT NULL) AUW0 
ON a.fbbid = AUW0.FUNDBOX_ID__C*/
LEFT JOIN
(SELECT rmr.*,
CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', rmr_time+4)::date+2
	WHEN datediff('day', rmr_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', rmr_time, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', rmr_time, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', rmr_time+4)::date+2
END rmr_week_end_date 
,(CASE WHEN rmr.event_type LIKE '%Enter%' THEN 1 ELSE 0 END ) enter_rmr_this_week
, (CASE WHEN rmr.event_type LIKE '%UnRMR%' AND flow_type NOT LIKE '%close%' THEN 1 ELSE 0 END) unrmr_this_week
FROM indus.PUBLIC.RMR_VIEW rmr
LEFT JOIN 
BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA DACD
ON rmr.fbbid = dacd.FBBID 
AND rmr.rmr_time = dacd.edate - 1
WHERE rmr.flow_type <> 'control'
AND dacd.sub_product = 'Line Of Credit') rmre 
on a.fbbid = rmre.fbbid and a.week_end = rmre.rmr_week_end_date
LEFT JOIN
(SELECT a.*,CASE WHEN (b.rmr_segment = '1. Bucket 11+ or FICO < 550' OR
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
ON a.fbbid = b.fbbid
)f
ON a.fbbid = f.fbbid AND a.week_end >= f.lock_start AND a.week_end < (f.lock_end-1)
LEFT JOIN indus.PUBLIC.og_auw_increases_expo og_auw
ON a.fbbid = og_auw.fbbid
AND a.week_end = og_auw.week_end
LEFT JOIN
( SELECT fbbid , min(CHARGE_OFF_DATE) co_date
FROM bi.FINANCE.FINANCE_METRICS_DAILY 
WHERE product_type <> 'Flexpay'
GROUP BY 1) D 
ON a.fbbid = d.fbbid
);


create or REPLACE table indus.public.INDUS_customer_experience_metric_helper_expo_v2 as(
select t1.*,
ifnull(t2.account_closed_by_fundbox,0) AS account_closed_by_fundbox_last_week,
ifnull(t2.disabled_due_to_as_disconnection,0) AS disabled_due_to_as_disconnection_last_week,
ifnull(t2.disabled_due_to_ba_disconnection,0) AS disabled_due_to_ba_disconnection_last_week,
ifnull(t2.able_to_draw_this_week,0) AS able_to_draw_last_week,
ifnull(t2.rmr_it_is,0) AS rmr_it_was, ---
ifnull(t2.disabled_due_to_credit_reason,0) AS disabled_due_to_credit_reason_last_week,
ifnull(t2.disabled_due_to_other_reason,0) AS disabled_due_to_other_reason_last_week,
ifnull(t2.closed_accounts_other_reasons,0) AS closed_accounts_other_reasons_last_week,
ifnull(t2.account_closed_by_customer,0) AS account_closed_by_customer_last_week,
ifnull(t2.exposure_end_of_week,0) AS exposure_end_of_last_week ---
--t2.disabled_due_to_credit_reason AS credit_enabled_to_disabled_last_week,
FROM indus.public.INDUS_customer_experience_metric_helper_expo t1
LEFT JOIN 
indus.public.INDUS_customer_experience_metric_helper_expo t2
ON 
t1.fbbid = t2.fbbid
AND t1.week_start = dateadd(week,1,t2.week_start) -- lag
);



create or REPLACE TABLE indus.public.INDUS_customer_experience_metric_AGG_expo as (
select 
--fbbid,
week_end as WEEK_END_DATE
,ifnull(sum(CASE WHEN able_to_draw_this_week = 1 THEN exposure_end_of_week END),0) able_to_draw_exposure
,sum(CASE WHEN able_to_draw_last_week = 1 THEN exposure_end_of_last_week ELSE 0 end) able_to_draw_exposure_last_week
,ifnull(sum(CASE WHEN new_approvals = 1 THEN new_credit_limit ELSE 0 END),0) new_approvals_exp
,ifnull(sum(CASE WHEN closed_exposure_management_this_week=1 AND RMR_it_is=0 
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) THEN -1*exposure_end_of_week end),0) AS exposure_from_cm
--
,ifnull(sum(CASE WHEN closed_exposure_management_this_week_inactive_flow=1 AND RMR_it_is=0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) THEN -1*exposure_end_of_week end),0) AS exposure_from_cm_inactive_flow
--
,ifnull(sum(CASE WHEN closed_exposure_management_this_week_expedited_anc_flow=1 AND RMR_it_is=0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) THEN -1*exposure_end_of_week end),0) AS exposure_from_cm_expedited_anc_flow
--
,ifnull(sum(CASE WHEN closed_exposure_management_this_week_expedited_inactive_flow=1 AND RMR_it_is=0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) THEN -1*exposure_end_of_week end),0) AS exposure_from_cm_expedited_inactive_flow
--
,ifnull(sum(CASE WHEN closed_exposure_management_this_week_anc_flow=1 AND RMR_it_is=0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) THEN -1*exposure_end_of_week end),0) AS closed_exposure_management_this_week_anc_flow
--
--
--
, ifnull(sum(CASE WHEN credit_decrease_this_week = 1 THEN credit_limit_change ELSE 0 END),0) credit_decrease_line_this_week
, ifnull(sum(CASE WHEN credit_decrease_this_week_auto = 1 THEN credit_limit_change ELSE 0 END),0) credit_decrease_line_this_week_auto
, ifnull(count(distinct CASE WHEN credit_decrease_this_week_auto = 1 THEN fbbid END),0) credit_decrease_line_this_week_auto_num
,ifnull(sum(CASE WHEN charged_off = 1 AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL) AND closed_exposure_management_this_week = 0 AND RMR_it_is=0
THEN -1*exposure_end_of_week end),0) exposure_from_charge_off
-----potentially add accounts closed by fbx
,ifnull(sum(CASE 
 WHEN (account_closed_by_customer=1 AND (account_closed_by_customer_last_week=0 OR account_closed_by_customer_last_week IS null))
AND (able_to_draw_last_week = 0 OR able_to_draw_last_week IS NULL) AND closed_exposure_management_this_week=0
AND RMR_it_is=0
AND charged_off = 0 AND new_approvals = 1 THEN -1*new_credit_limit
WHEN (account_closed_by_customer=1 AND (account_closed_by_customer_last_week=0 OR account_closed_by_customer_last_week IS null))
AND able_to_draw_last_week = 1 AND closed_exposure_management_this_week=0
AND RMR_it_is=0
AND charged_off = 0 THEN -1*exposure_end_of_week END),0) closed_accounts_exposure
---
,sum(
CASE WHEN (disabled_due_to_credit_reason=1 AND disabled_due_to_credit_reason_last_week=0 
AND charged_off = 0 AND rmr_it_is=0 AND enter_rmr_this_week = 0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL OR NEW_approvals = 1)
AND closed_exposure_management_this_week=0
AND account_closed_by_customer=0)
THEN -1*exposure_end_of_week
---
WHEN (disabled_due_to_credit_reason=0 AND disabled_due_to_credit_reason_last_week=1 
AND charged_off = 0 AND rmr_it_is=0 AND unrmr_this_week = 0
AND able_to_draw_this_week=1
AND closed_exposure_management_this_week=0
AND account_closed_by_customer=0)
THEN exposure_end_of_week 
ELSE 0 END
) net_credit_reason_exposure
---
,ifnull(sum(CASE WHEN (disabled_due_to_ba_disconnection=1
OR disabled_due_to_as_disconnection=1) AND disabled_due_to_as_disconnection_last_week = 0
AND disabled_due_to_ba_disconnection_last_week=0 
AND closed_exposure_management_this_week=0
AND charged_off = 0 
AND rmr_it_is=0 AND disabled_due_to_credit_reason=0
AND account_closed_by_customer=0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL)
THEN exposure_end_of_week*-1
---
WHEN (disabled_due_to_ba_disconnection=0
AND disabled_due_to_as_disconnection=0) AND (disabled_due_to_as_disconnection_last_week = 1
OR disabled_due_to_ba_disconnection_last_week=1)
AND closed_exposure_management_this_week=0
AND charged_off = 0
AND able_to_draw_this_week=1 AND disabled_due_to_credit_reason=0
AND rmr_it_is=0
AND account_closed_by_customer=0
THEN exposure_end_of_week END),0) net_ba_as_exposure
---
,ifnull(sum(CASE WHEN disabled_due_to_other_reason=1 AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL OR NEW_approvals = 1)
AND disabled_due_to_other_reason_last_week=0
AND disabled_due_to_ba_disconnection = 0
AND disabled_due_to_as_disconnection=0
AND charged_off = 0 AND rmr_it_is=0
AND disabled_due_to_credit_reason=0
AND closed_exposure_management_this_week=0
AND account_closed_by_customer=0
THEN -1*exposure_end_of_week 
---
WHEN disabled_due_to_other_reason=0 AND able_to_draw_this_week=1
AND disabled_due_to_other_reason_last_week=1
AND disabled_due_to_ba_disconnection = 0
AND disabled_due_to_as_disconnection=0
AND charged_off = 0 AND rmr_it_is=0
AND disabled_due_to_credit_reason_last_week=0
AND closed_exposure_management_this_week=0
AND account_closed_by_customer=0
THEN exposure_end_of_week
END
),0) net_other_reason_exposure
---
---
,ifnull(sum(CASE WHEN closed_accounts_other_reasons = 1 AND closed_accounts_other_reasons_last_week = 0
AND (able_to_draw_last_week=1 OR able_to_draw_last_week IS NULL)
AND disabled_due_to_ba_disconnection = 0
AND disabled_due_to_as_disconnection=0
AND charged_off = 0 AND rmr_it_is=0
AND disabled_due_to_credit_reason=0
AND closed_exposure_management_this_week=0
AND disabled_due_to_other_reason = 0
AND account_closed_by_customer=0
THEN -1*exposure_end_of_week 
END
),0) exposure_accts_suspended_other_reasons
---
, count(CASE WHEN inactive_offer_accepted=1 THEN fbbid end) offers_accepted
, sum(CASE WHEN inactive_offer_accepted = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_offers_this_week
, count(CASE WHEN clip_this_week=1 THEN fbbid end) accounts_clip_this_week
, sum(CASE WHEN clip_this_week = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END ) increased_line_clips_this_week
, count(CASE WHEN boost_xl_this_week=1 THEN fbbid end) accounts_boost_xl_this_week
, sum(CASE WHEN boost_xl_this_week = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_boost_xl_this_week
, count(CASE WHEN backbook_batch_increase_this_week=1 THEN fbbid end) accounts_backbook_batch_increase_this_week
, sum(CASE WHEN backbook_batch_increase_this_week = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_backbook_this_week
--
-- 2 Sep 2024 change - SPOT, ECLIP, Intuit 52 week
,count(CASE WHEN is_spot_clip = 1 THEN (fbbid) ELSE 0 END) accounts_spot_clip_this_week
,count(CASE WHEN is_intuit_52_week_test = 1 THEN (fbbid) ELSE 0 END) accounts_intuit_52_week_test_this_week
,count(CASE WHEN is_eclip = 1 THEN (fbbid) ELSE 0 END) accounts_eclip_this_week
,count(CASE WHEN is_spot_clip_reversed = 1 THEN (fbbid) ELSE 0 END) accounts_spot_clip_reversed_this_week
--
,sum(CASE WHEN is_spot_clip = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_spot_clip_this_week
,sum(CASE WHEN is_intuit_52_week_test = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_intuit_52_week_test_this_week
,sum(CASE WHEN is_eclip = 1 THEN (EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) increased_line_eclip_this_week
,sum(CASE WHEN is_spot_clip_reversed=1 THEN (EXPOSURE_END_OF_WEEK-EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END) decreased_line_spot_clip_this_week
---
---
, ifnull(sum(CASE WHEN pre_doc_inc_this_week_flag = 1 THEN (exposure_end_of_week - new_credit_limit) ELSE 0 END),0) pre_doc_auw_exp
, ifnull(sum(CASE WHEN post_doc_inc_this_week_flag = 1 THEN (exposure_end_of_week - EXPOSURE_END_OF_LAST_WEEK) ELSE 0 END),0) post_doc_auw_exp
---
---
, sum(CASE WHEN og_auw_increases=1 THEN EXPOSURE_END_OF_WEEK - EXPOSURE_END_OF_LAST_WEEK ELSE 0 END) AS og_auw_amount_increased
, count(og_auw_increases) og_auw_accts_increased 
---
, (pre_doc_auw_exp + post_doc_auw_exp + og_auw_amount_increased) auw_exp
---
, sum(CASE WHEN enter_rmr_this_week = 1 AND able_to_draw_last_week = 1 THEN -1 * exposure_end_of_last_week ELSE 0 END)  rmr_exp
, sum(CASE WHEN unrmr_this_week = 1 THEN 1 * exposure_end_of_week ELSE 0 END) unrmr_exp
---
, (new_approvals_exp + unrmr_exp + increased_line_clips_this_week + increased_line_offers_this_week + increased_line_boost_xl_this_week + auw_exp + increased_line_spot_clip_this_week + increased_line_intuit_52_week_test_this_week + increased_line_eclip_this_week+ increased_line_backbook_this_week) net_enabled_exposure
, (rmr_exp + credit_decrease_line_this_week + exposure_from_cm + closed_accounts_exposure + net_credit_reason_exposure + net_ba_as_exposure + exposure_accts_suspended_other_reasons + net_other_reason_exposure + exposure_from_charge_off + decreased_line_spot_clip_this_week) net_disabled_exposure
, (net_enabled_exposure + net_disabled_exposure) net_exposure_change
---
FROM
indus.public.INDUS_customer_experience_metric_helper_expo_v2
WHERE week_start >= to_date('2020-12-30')
group by 1
);


CREATE OR REPLACE TABLE indus.public.INDUS_customer_experience_metric_AGG_expo_final AS (
SELECT *, 
(able_to_draw_exposure - (ifnull(able_to_draw_exposure_last_week,0) + net_exposure_change)) able_to_draw_delta,
FROM
indus.public.INDUS_customer_experience_metric_AGG_expo
);

