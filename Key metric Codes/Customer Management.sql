CREATE OR REPLACE TABLE analytics.credit.customer_management_helper_pb as
(SELECT b.*
	  , CASE 
    WHEN b.customer_annual_revenue >= 1000000 OR b.credit_limit >= 100000 
    THEN 'HVC' 
    ELSE 'NOT HVC' 
  	END AS HVC_GROUP

      , CASE WHEN b.new_cust_filter = 'New Customer' THEN b.ob_bucket_group
		     WHEN b.new_cust_filter = 'Existing Customer' THEN b.og_bucket_group END bucket_group
	  , CASE WHEN b.first_draw_time is null and b.first_approved_time is not null then 'ANC'
			 WHEN b.first_draw_time is not null and b.first_approved_time is not null then 'FTD' end FTD_ANC
	  , CASE WHEN b.first_approved_time is not null then b.account_status_reason end FTD_ANC_REASON
	  , CASE WHEN FTD_ANC = 'ANC' AND b.account_status_reason = 'Expedited ANC flow' THEN 'Expedited' 
    		 WHEN FTD_ANC = 'ANC' AND b.account_status_reason = 'ANC flow' THEN 'Normal' ELSE NULL END AS ANC_Flow_Type
   	  , (CASE WHEN b.first_approved_time IS NOT NULL THEN 1 ELSE 0 END) account_approved

	  ---CALL INBOUND-----------

	  , CASE WHEN b.WEEK_END_DATE = b.call_weekend_date THEN b.call_count ELSE 0 END AS call_counts
      --------
      ---draw metrics
	  , (CASE WHEN b.originated_amount > 0 THEN 1 ELSE 0 END) AS customer_drew
	  , (CASE WHEN b.originated_amount > 0 and ((c.outstanding_principal + c.fees_due - c.discount_pending) = 0
	    or (c.outstanding_principal + c.fees_due - c.discount_pending) IS NULL) THEN 1 ELSE 0 END) AS customer_drew_no_balance
	  , (CASE WHEN b.originated_amount > 0 and (c.outstanding_principal + c.fees_due - c.discount_pending) > 0 THEN 1 ELSE 0 END)
		AS customer_drew_with_balance
	  , (CASE WHEN b.originated_amount > 0 THEN datediff('day', date(date_trunc('day', c.last_draw_time)), b.week_end_date) END) AS days_since_last_draw
	  , (CASE WHEN b.originated_amount > 0 THEN c.credit_utilization END) AS utilization_before_draw
	  -------- 
      ------BA/AS Disconnection
      , (CASE WHEN b.underwriting_flow = 'Invoice Clearing' AND b.DASHBOARD_STATUS_CHANGE_REASON in ('bank_account_error','missing_bank_account','accounting_software_disconnected','needs_sync','action_required','mandatory_ds_disconnected')
		THEN 1 ELSE 0 END) AS disabled_due_to_as_disconnection
	  , (CASE WHEN b.underwriting_flow = 'Direct Draw' AND b.DASHBOARD_STATUS_CHANGE_REASON in ('bank_account_error','missing_bank_account','accounting_software_disconnected','needs_sync','action_required','mandatory_ds_disconnected')
		THEN 1 ELSE 0 END) AS disabled_due_to_ba_disconnection
	  , (CASE WHEN disabled_due_to_ba_disconnection=1 OR disabled_due_to_as_disconnection=1 THEN 1 ELSE 0 end) disabled_due_to_ba_as_reason
	  , (CASE WHEN b.underwriting_flow = 'Direct Draw' AND datediff('day',to_date(b.FI_DATA_UPDATE_TO_TIME),b.week_end_date) >= 4 THEN 1 ELSE 0 END) AS bank_not_connected_this_week
	  , (CASE WHEN b.underwriting_flow = 'Invoice Clearing' AND datediff('day',to_date(b.PLATFORM_DATA_UPDATE_TO_TIME),b.week_end_date) >= 4 THEN 1 ELSE 0 END) AS as_not_connected_this_week
	  -------
	  ----suspension segments
	  , (CASE WHEN (b.is_chargeoff = 0 OR b.chargeoff_time::date > b.week_end_date) and b.account_status = 'suspended' and (c.account_status <> 'suspended' OR c.account_status IS NULL) THEN 1 ELSE 0 END) AS suspended_this_week
	  , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason in ('Inactive flow', 'Expedited ANC flow', 'Expedited Inactive flow', 'ANC flow')
	    and c.account_status <> 'suspended' THEN 1 ELSE 0 END) AS closed_exposure_management_this_week
	 , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason = 'Inactive flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_inactive_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason = 'Expedited ANC flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_expedited_anc_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason =  'Expedited Inactive flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_expedited_inactive_flow ---CLOSED EXPO MGMT!!
     , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason = 'ANC flow' and (c.account_status<>'suspended' or c.account_status is null) THEN 1 ELSE 0 END) AS closed_exposure_management_this_week_anc_flow
	  , (CASE WHEN b.account_status = 'suspended' and b.account_status_reason = 'User request' and c.account_status <> 'suspended' THEN 1 ELSE 0 END) AS closed_by_customer_this_week
	  , (CASE WHEN b.account_status = 'suspended' and (b.account_status_reason NOT IN  ('User request','Inactive flow', 'Expedited ANC flow', 'Expedited Inactive flow', 'ANC flow','RMR review 2')
	    OR b.account_status_reason IS NULL ) and c.account_status <> 'suspended' THEN 1 ELSE 0 END) AS closed_by_other_reason_this_week
	  , (CASE WHEN b.account_status = 'suspended' AND b.account_status_reason = 'RMR review 2' and c.account_status <> 'suspended' THEN 1 ELSE 0 END) AS closed_by_rmr_review_this_week
	  , (CASE WHEN b.IS_CHARGED_OFF_FMD = 1 AND B.chargeoff_time::date BETWEEN b.week_start_DATE and b.week_end_DATE THEN 1 ELSE 0 END) AS charged_off_this_week
	  , (CASE WHEN b.credit_status = 'disabled' and b.account_status <> 'suspended' and b.credit_status_reason ilike '%collection%' and c.credit_status <> 'disabled' THEN 1 ELSE 0 END) AS disabled_delq_this_week
	  , (CASE WHEN b.credit_status = 'disabled' and (NOT b.credit_status_reason ilike '%collection%' and NOT b.credit_status_reason ilike '%rmr%' and NOT b.credit_status_reason ilike '%balance%' and NOT b.credit_status_reason ilike '%review%' and NOT b.credit_status_reason = 'inactive' and NOT b.credit_status_reason ilike '%payoff%') 
	  	and c.credit_status <> 'disabled' THEN 1 ELSE 0 END) AS disabled_credit_other_reason_this_week
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.recovery_suggested_state = 'EOL') and b.account_status = 'suspended'
		and (b.account_status_reason IS NULL or b.account_status_reason <> 'User request') THEN 1 ELSE 0 END) account_closed_by_Fundbox
	  , (CASE WHEN b.credit_status = 'disabled' THEN 1 ELSE 0 END) AS disabled_due_to_credit_reason
	  , (CASE WHEN b.is_locked_dashboard = 1 and b.dashboard_status_change_reason <> 'full_utilization'
		and disabled_due_to_ba_disconnection = 0 and disabled_due_to_as_disconnection = 0 
		and disabled_due_to_credit_reason = 0 THEN 1 ELSE 0 end) disabled_due_to_other_reason
	  , (CASE WHEN (b.is_chargeoff = 0 OR b.chargeoff_time::date > b.week_end_date) AND b.credit_status = 'disabled' and (NOT b.credit_status_reason ilike '%collection%' and NOT b.credit_status_reason ilike '%rmr%' and NOT b.credit_status_reason ilike '%review%' and NOT b.credit_status_reason ilike '%balance%' and NOT b.credit_status_reason = 'inactive' and NOT b.credit_status_reason ilike '%payoff%') THEN 1 ELSE 0 END) disabled_credit_other_reason
	  ------
	  ---delq segments
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.credit_status = 'disabled' and b.credit_status_reason ilike '%collection%' and b.delinquency_bucket <= 2 THEN 1 END) early_delq_not_restr
	  ,	(CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.credit_status = 'disabled' and b.credit_status_reason ilike '%collection%' and b.delinquency_bucket BETWEEN 3 and 8 and b.is_urp_eligible = 1 THEN 1 END) delq_urp_eligible
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.credit_status = 'disabled' and b.credit_status_reason ilike '%collection%' and b.delinquency_bucket BETWEEN 3 and 8 and b.is_urp_eligible = 0 THEN 1 END) delq_urp_ineligible
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.credit_status = 'disabled' and b.credit_status_reason ilike '%collection%' and b.delinquency_bucket >= 9 THEN 1 END) delq_perm_restr
	  --------
	  ----rmr segments
	  , (CASE WHEN B.enter_rmr_this_week > 0 THEN 1 ELSE 0 END) AS disabled_rmr_this_week
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  '1. OG 11+/FICO < 550' THEN 1 ELSE 0 END) AS disabled_rmr_og_11_fico_less_550_this_week----
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  '2. MLE > 0.096' THEN 1 ELSE 0 END) AS disabled_rmr_mle_greater_96_this_week
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  '3. OG 9-10 with 5+ inq/DQ in 30 days/FICO < 600' THEN 1 ELSE 0 END) AS disabled_rmr_DQ_in_30_this_week
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  '4. OG 9-10 partner' THEN 1 ELSE 0 END) AS disabled_rmr_OG_9_10_partners
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  '5. Vantage < 570' THEN 1 ELSE 0 END) AS disabled_rmr_vantage_570_this_week
	  , (CASE WHEN B.enter_rmr_this_week > 0 AND B.rmr_group =  'others' THEN 1 ELSE 0 END) AS disabled_rmr_others_this_week
      ------
	  -----payoff
	  ,	(CASE WHEN b.lock_start between b.week_start_date AND b.week_end_date 
		AND b.lock_end > b.week_end_date THEN 1 ELSE 0 END) AS disabled_payoff_this_week
	  , (CASE WHEN b.lock_start IS NOT NULL AND (b.lock_end > b.week_end_date OR b.lock_end IS NULL) THEN 1 ELSE 0 END ) disabled_payoff
	  ----------
	  --------able to draw segments
	  , (CASE WHEN (b.charge_off_date_fmd IS NULL or b.charge_off_date_fmd > b.week_end_date) and b.account_status <> 'suspended'
		and b.credit_status = 'approved' and (b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization'))
		 THEN 1 ELSE 0 END) able_to_draw --lag

	  /*, LAG(CASE
        WHEN (
            b.charge_off_date_fmd IS NULL OR b.charge_off_date_fmd > b.week_end_date
        )
        AND b.account_status <> 'suspended'
        AND b.credit_status = 'approved'
        AND (
            b.is_locked_dashboard = 0
            OR b.is_locked_dashboard IS NULL
            OR (
                b.is_locked_dashboard = 1
                AND b.dashboard_status_change_reason = 'full_utilization'
            )
        ) THEN 1
        ELSE 0
    END, 1, 0) OVER (PARTITION BY b.fbbid ORDER BY b.week_end_date) AS able_to_draw_last_week*/

	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.account_status <> 'suspended'
		and b.credit_status = 'approved' and (b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization'))
		and ((c.outstanding_principal + c.fees_due - c.discount_pending) <= 0
		or (c.outstanding_principal + c.fees_due - c.discount_pending) IS NULL) THEN 1 ELSE 0 END) able_to_draw_no_balance
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.account_status <> 'suspended'
		and b.credit_status = 'approved' and (b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization'))
		and (c.outstanding_principal + c.fees_due - c.discount_pending) > 0 THEN 1 ELSE 0 END) able_to_draw_with_balance
	  , (CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.account_status <> 'suspended' 
	    and (b.credit_status = 'disabled' or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason <> 'full_utilization') or (b.credit_status = 'rejected' and b.is_locked_dashboard IS NULL)) and ((c.outstanding_principal + c.fees_due - c.discount_pending) <= 0
		or (c.outstanding_principal + c.fees_due - c.discount_pending) IS NULL) THEN 1 ELSE 0 END) unable_to_draw_no_balance
	  ,	(CASE WHEN (b.chargeoff_time::date IS NULL or b.chargeoff_time::date > b.week_end_date) and b.account_status <> 'suspended' and (b.credit_status = 'disabled' or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason <> 'full_utilization') or (b.credit_status = 'rejected' and b.is_locked_dashboard IS NULL))
		and (c.outstanding_principal + c.fees_due - c.discount_pending) > 0 THEN 1 ELSE 0 END) unable_to_draw_with_balance
	  --------
	  -----new approvals
	  , (CASE WHEN to_date(b.first_approved_time) between b.week_start_date and b.week_end_date THEN 1 ELSE 0 END) AS new_approvals
	  --------
	  ----inactive offers
	  , (CASE WHEN b.offer_dt::date BETWEEN b.week_start_date and b.week_end_date and b.incr_amt > 0 THEN 1 ELSE 0 END) AS inactive_offer_made
	  , (CASE WHEN b.accept_tm::date BETWEEN b.week_start_date and b.week_end_date and b.incr_amt > 0 THEN 1 ELSE 0 END) AS inactive_offer_accepted
	  , (CASE WHEN b.accept_tm::date BETWEEN b.week_start_date and b.week_end_date AND b.incr_amt > 0 THEN B.incr_amt END) AS offer_increased_line
	  ------------
	  ---------CLIP
	  , (CASE WHEN b.last_increase_time::date BETWEEN b.week_start_date and b.week_end_date
        and b.credit_limit_comment IS NULL and b.credit_limit_reason in ('Credit increase', 'CLIP credit increase')
        THEN 1 ELSE 0 END) AS clip_this_week
      , (CASE WHEN b.last_increase_time::date BETWEEN b.week_start_date and b.week_end_date
        and b.credit_limit_comment IS NULL and b.credit_limit_reason in ('Credit increase', 'CLIP credit increase')                
        THEN b.credit_limit_change else 0 END) AS clip_increased_line_this_week
       ---------
       ------BOOST     
   	  , (CASE WHEN b.credit_limit_reason = 'Credit increase' and (b.credit_limit_comment ilike '%boost%' or b.credit_limit_comment ilike '%boots%' or b.credit_limit_comment ilike '%xl%')
        and b.last_increase_time::date BETWEEN b.week_start_date and b.week_end_date THEN 1 ELSE 0 END) AS boost_xl_this_week
      , (CASE WHEN b.credit_limit_reason = 'Credit increase' and (b.credit_limit_comment ilike '%boost%' or b.credit_limit_comment ilike '%boots%' or b.credit_limit_comment ilike '%xl%')
         and b.last_increase_time::date BETWEEN b.week_start_date and b.week_end_date THEN b.credit_limit_change ELSE 0 END) AS boost_xl_increased_line_this_week
       --------

		------Automated CLD
		, (CASE WHEN b.last_decrease_time::date BETWEEN b.week_start_date and b.week_end_date
         and b.credit_limit_reason ilike '%automated cld%'
        THEN 1 ELSE 0 END) AS automated_cld_this_week

		, (CASE WHEN b.last_decrease_time::date BETWEEN b.week_start_date and b.week_end_date
         and b.credit_limit_reason ilike '%automated cld%'
        THEN b.credit_limit_change else 0 END) AS automated_cld_this_week_exp


       ------charge-off and tenure
      , (CASE WHEN b.chargeoff_time IS NULL and b.account_status = 'suspended' and b.account_status_reason = 'User request' THEN 1 ELSE 0 END) account_closed_by_customer
      , (CASE WHEN account_closed_by_customer = 0 and account_closed_by_fundbox = 0 and b.chargeoff_time <= b.week_end_date AND b.is_chargeoff=1 THEN 1 ELSE 0 END) account_charged_off
      , (CASE WHEN charged_off_this_week=1 AND DATEDIFF('day',b.first_approved_time,GETDATE())<=180 THEN 1 ELSE 0 END) co_this_week_and_mob_less_than_180_days
      , (CASE WHEN charged_off_this_week=1 AND DATEDIFF('day',b.first_approved_time,GETDATE())>180 AND DATEDIFF('year',b.first_approved_time,GETDATE())<=1 THEN 1 ELSE 0 END) co_this_week_and_mob_less_than_1_year
      , (CASE WHEN charged_off_this_week=1 AND DATEDIFF('year',b.first_approved_time,GETDATE())>1 AND DATEDIFF('year',b.first_approved_time,GETDATE())<=2 THEN 1 ELSE 0 END) co_this_week_and_mob_less_than_2_years
	  ,	(CASE WHEN charged_off_this_week=1 AND DATEDIFF('year',b.first_approved_time,GETDATE())>2 THEN 1 ELSE 0 END) co_this_week_and_mob_greater_than_2_years
	  , (CASE WHEN account_charged_off = 1 AND DATEDIFF('day',b.first_approved_time,GETDATE())<=180 THEN 1 ELSE 0 END) co_and_mob_less_than_180_days
	  ,	(CASE WHEN account_charged_off=1 AND DATEDIFF('day',b.first_approved_time,GETDATE())>180 AND DATEDIFF('year',b.first_approved_time,GETDATE())<=1 THEN 1 ELSE 0 END) co_and_mob_less_than_1_year
	  ,	(CASE WHEN account_charged_off=1 AND DATEDIFF('year',b.first_approved_time,GETDATE())>1 AND DATEDIFF('year',b.first_approved_time,GETDATE())<=2 THEN 1 ELSE 0 END) co_and_mob_less_than_2_years
	  , (CASE WHEN account_charged_off=1 AND DATEDIFF('year',b.first_approved_time,GETDATE())>2 THEN 1 ELSE 0 END) co_and_mob_greater_than_2_years  
	  ----------
	  ----------
	  , (CASE WHEN b.dpd_days_corrected BETWEEN 0 AND 91 and b.IS_CHARGED_OFF_fmd = 0 then b.OUTSTANDING_PRINCIPAL_DUE else 0 end) open_outstanding
	  , (CASE WHEN b.chargeoff_time IS NULL and b.account_status = 'suspended' and b.account_status_reason in ('Inactive flow', 'Expedited ANC flow', 'Expedited Inactive flow', 'ANC flow')
		THEN 1 ELSE 0 END) closed_exposure_management
	  , (CASE WHEN b.account_status = 'active' AND b.is_chargeoff = 0 and b.dpd_days_corrected < 98 THEN 1 ELSE 0 END) AS open_accounts
	  , (CASE WHEN b.account_status = 'active' AND b.dpd_days_corrected BETWEEN 0 AND 91 and b.IS_CHARGED_OFF_FMD = 0 and b.OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END) active_accounts
	  , (CASE WHEN suspended_this_week=0 AND charged_off_this_week=1 THEN 1 ELSE 0 END) co_not_suspended_this_Week
	  , (CASE WHEN (b.OUTSTANDING_PRINCIPAL_DUE = 0 OR b.OUTSTANDING_PRINCIPAL_DUE IS NULL) THEN 1 ELSE 0 END) no_balance_end_of_week
	  , (CASE WHEN b.OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END) balance_end_of_week
	  , (CASE WHEN to_date(b.first_draw_time) between b.week_start_date and b.week_end_date AND no_balance_end_of_week=0
		AND suspended_this_week=0 AND charged_off_this_week=0 THEN 1 ELSE 0 END) AS new_ftd
	  , (CASE WHEN new_approvals=1 and b.account_status <> 'suspended' and b.credit_status = 'approved' and 
	  	(b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization')) THEN 1 ELSE 0 END) new_approvals_able_to_draw
      , b.credit_utilization util_end_of_week
      , b.credit_limit  exposure_end_of_week
      , ifnull(b.credit_limit,0) - ifnull(b.OUTSTANDING_PRINCIPAL_DUE,0) otb_end_of_week_INT
	  , CASE WHEN otb_end_of_week_INT > 0 THEN otb_end_of_week_INT ELSE 0 END AS otb_end_of_week --- last week lag
	  , LAG(CASE
        WHEN IFNULL(b.credit_limit, 0) - IFNULL(b.OUTSTANDING_PRINCIPAL_DUE, 0) > 0
        THEN IFNULL(b.credit_limit, 0) - IFNULL(b.OUTSTANDING_PRINCIPAL_DUE, 0)
        ELSE 0
    	END, 1, 0) OVER (PARTITION BY b.fbbid ORDER BY b.week_end_date) AS otb_end_of_week_last_week
      , (CASE WHEN b.og_auw_this_week_flag = 1 AND c.og_auw_this_week_flag = 0 THEN 1 ELSE 0 END) AS og_auw_increases
      , (CASE WHEN og_auw_increases=1 THEN b.og_auw_increase ELSE 0 END) og_auw_line_increase_amts
      , (CASE WHEN b.credit_status = 'disabled' and b.account_status <> 'suspended' and b.credit_status_reason ilike '%collection%' THEN 1 ELSE 0 END) AS disabled_delq
FROM 
-----CUSTOMER LEVEL CENTRAL PULL
ANALYTICS.CREDIT.customer_level_data_td b
------
LEFT JOIN
-----last week's customer level pull
ANALYTICS.CREDIT.customer_level_data_td c
ON  c.fbbid = b.fbbid
and c.WEEK_END_DATE = dateadd(DAY, -1, b.week_start_date)
WHERE b.week_end_date >= '2020-12-31' AND b.sub_product_daily = 'Line Of Credit'
AND b.is_test = 0
)
;

select week_end_date, sum(case when rmr_lock_start between week_start_date and week_end_date then 1 else 0 end) 
from analytics.credit.customer_level_data_td 
where is_test=0 and sub_product='Line Of Credit' group by 1 order by 1 desc
;


CREATE OR REPLACE TABLE analytics.credit.customer_management_lagtable_pb as
(select b.*
	  ,	c.able_to_draw_no_balance able_to_draw_no_balance_last_week
	  , c.able_to_draw_with_balance able_to_draw_with_balance_last_week
	  , c.unable_to_draw_no_balance unable_to_draw_no_balance_last_week
	  , c.unable_to_draw_with_balance unable_to_draw_with_balance_last_week
	  , c.disabled_due_to_credit_reason disabled_due_to_credit_reason_last_week
	  , c.disabled_due_to_as_disconnection disabled_due_to_as_disconnection_last_week
	  , c.disabled_due_to_ba_disconnection disabled_due_to_ba_disconnection_last_week
	  , c.disabled_due_to_ba_as_reason disabled_due_to_ba_as_reason_last_week
	  , c.disabled_due_to_other_reason disabled_due_to_other_reason_last_week
	  , c.open_accounts open_accounts_last_week
	  , c.active_accounts active_accounts_last_week
	  , c.balance_end_of_week balance_end_of_week_last_week
	  , c.able_to_draw able_to_draw_last_week
	  , c.closed_exposure_management_this_week closed_exposure_management_last_week
	  , (CASE WHEN b.balance_end_of_week=1 AND balance_end_of_week_last_week=0 AND b.suspended_this_week=0
		AND b.charged_off_this_week=0 AND b.new_ftd=0 THEN 1 ELSE 0 end) AS new_customers_with_balance
	  , (CASE WHEN active_accounts_last_week=1 AND b.suspended_this_week=1 THEN 1 ELSE 0 end) active_last_suspended_this_week
	  , (CASE WHEN active_accounts_last_week=1 AND b.suspended_this_week=0 AND b.charged_off_this_week=1 THEN 1 ELSE 0 end) active_last_co_this_week
	  , (CASE WHEN active_accounts_last_week=1 and b.suspended_this_week=0 AND b.charged_off_this_week=0 AND b.no_balance_end_of_week=1
		AND balance_end_of_week_last_week=1 THEN 1 ELSE 0 end) AS paid_off_this_week
	  , (CASE WHEN b.able_to_draw=1 AND b.credit_status = 'approved' AND disabled_due_to_credit_reason_last_week=1
		THEN 1 ELSE 0 END) AS credit_disabled_to_enabled_this_week
	  , (CASE WHEN b.able_to_draw=1 AND credit_disabled_to_enabled_this_week=0 AND (b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization')) 
	  	AND disabled_due_to_ba_as_reason_last_week=1 THEN 1 ELSE 0 END) AS ba_as_reasons_disabled_to_enabled_this_week
	  , (CASE WHEN b.able_to_draw=1 AND credit_disabled_to_enabled_this_week=0 AND (b.is_locked_dashboard = 0 or b.is_locked_dashboard IS NULL or (b.is_locked_dashboard = 1 and b.dashboard_status_change_reason = 'full_utilization')) 
	  	AND disabled_due_to_other_reason_last_week=1 THEN 1 ELSE 0 END) AS other_reasons_disabled_to_enabled_this_week
	  , (CASE WHEN able_to_draw_last_week=1 AND b.suspended_this_week=1 THEN 1 ELSE 0 end) AS able_to_draw_suspended_this_week
	  , (CASE WHEN able_to_draw_last_week=1 AND able_to_draw_suspended_this_week=0 and b.credit_status = 'disabled' AND disabled_due_to_credit_reason_last_week=0
		THEN 1 ELSE 0 END) AS credit_enabled_to_disabled_this_week
	  , (CASE WHEN able_to_draw_last_week=1 AND able_to_draw_suspended_this_week=0 and credit_enabled_to_disabled_this_week=0 and
		(b.is_locked_dashboard = 1 and b.dashboard_status_change_reason <> 'full_utilization') AND b.disabled_due_to_ba_as_reason = 1
		THEN 1 ELSE 0 END) AS ba_as_reasons_enabled_to_disabled_this_week
	  , (CASE WHEN able_to_draw_last_week=1 AND able_to_draw_suspended_this_week=0 and credit_enabled_to_disabled_this_week=0 and
		(b.is_locked_dashboard = 1 and b.dashboard_status_change_reason <> 'full_utilization') AND b.disabled_due_to_other_reason = 1 
		THEN 1 ELSE 0 END) AS other_reasons_enabled_to_disabled_this_week
	  , c.bank_not_connected_this_week bank_not_connected_last_week
	  , CASE WHEN b.bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 THEN 1 ELSE 0 END AS bank_disconnection
	  , (CASE WHEN bank_disconnection = 1 AND b.week_end_date <= dateadd('day',-30,current_date()) and ds.fi_update_dt_30_aftr_disc > b.week_end_date THEN 1 ELSE 0 END) AS ba_reconnect_30_days
	  , c.as_not_connected_this_week as_not_connected_last_week
	  , (CASE WHEN b.as_not_connected_this_week=1 AND as_not_connected_last_week=0 THEN 1 ELSE 0 END) AS as_disconnection
	  , (CASE WHEN as_disconnection = 1 AND b.week_end_date <= dateadd('day',-30,current_date()) and ds.as_update_dt_30_aftr_disc > b.week_end_date THEN 1 ELSE 0 END) AS as_reconnect_30_days
	  , c.exposure_end_of_week exposure_end_of_last_week
	  , b.exposure_end_of_week -  c.exposure_end_of_week  as exposure_change
------
FROM 
analytics.credit.customer_management_helper_pb B
---------
LEFT JOIN 
analytics.credit.customer_management_helper_pb C
ON B.fbbid = C.fbbid 
AND DATEADD(DAY, -1, B.week_start_DATE) = C.week_END_date
-------BA & AS date on day 30-------------
LEFT JOIN
(SELECT fbbid
      , edate
      , to_date(FI_DATA_UPDATE_TO_TIME) fi_update_dt_30_aftr_disc
      , to_date(PLATFORM_DATA_UPDATE_TO_TIME) as_update_dt_30_aftr_disc
FROM BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA
WHERE edate >= '2020-12-30' AND is_test_user = 0
)ds
ON b.fbbid = ds.fbbid AND dateadd('day',30,b.WEEK_END_DATE) = ds.edate)
;



create or REPLACE table analytics.credit.customer_management_AGG_pb as (
WITH TEMP_TABLE AS(
select DISTINCT week_start_date
   , WEEK_END_DATE
   , MEDIAN(CASE WHEN able_to_draw = 1 then AVAILABLE_BALANCE_FI ELSE NULL END) OVER (PARTITION BY WEEK_END_DATE) AS able_to_draw_available_balance
FROM analytics.credit.customer_management_helper_pb
)
select T1.WEEK_END_DATE
, TERMUNITS
, Partner
, national_funding_flow
, new_cust_filter
, bucket_group
, industry_type
, customer_annual_revenue_group
, tenure_status
, TENURE_BUCKET
, HVC_GROUP
, exp_usage_bkt
, DEBT_LOAD_group
, SUM(CASE WHEN open_accounts = 1 THEN fbx_orig END) AS OPEN_WALLET_SHARE_NUMERATOR
, SUM(CASE WHEN open_accounts = 1 THEN DENOM_WALLET END) AS OPEN_WALLET_SHARE_DENOMINATOR
, SUM(CASE WHEN active_accounts = 1 THEN fbx_orig END) AS ACTIVE_WALLET_SHARE_NUMERATOR
, SUM(CASE WHEN active_accounts = 1 THEN DENOM_WALLET END) AS ACTIVE_WALLET_SHARE_DENOMINATOR
, sum(CASE WHEN able_to_draw_last_week = 1 then originated_amount else 0 end) orig
, sum(CASE WHEN able_to_draw_last_week = 1 then exposure_change else 0 end) atd_exp_growth
, sum(CASE WHEN able_to_draw_last_week = 1 and active_accounts_last_week = 1 then originated_amount else 0 end) orig_active
, sum(CASE WHEN able_to_draw = 1 THEN credit_limit ELSE NULL END ) atd_exp
, sum(CASE
            WHEN able_to_draw = 1 THEN open_outstanding
            ELSE NULL
        END) AS atd_os
, sum(CASE
            WHEN active_accounts=1 THEN open_outstanding
            ELSE NULL
        END) AS active_os
, sum(call_counts) call_customers
, count(CASE WHEN account_approved=1 THEN fbbid END ) approved_customers
, count(CASE WHEN account_closed_by_customer=1 THEN fbbid end) accounts_closed_by_customer
, count(CASE WHEN account_closed_by_fundbox=1 THEN fbbid end) accounts_closed_by_fundbox
, count(CASE WHEN account_charged_off=1 THEN fbbid end) accounts_charged_off
---
, (approved_customers - accounts_closed_by_customer - accounts_closed_by_fundbox - accounts_charged_off) total_open_accounts
---
, count(CASE WHEN able_to_draw_no_balance=1 THEN fbbid end) accounts_able_to_draw_no_balance
, count(CASE WHEN able_to_draw_with_balance=1 THEN fbbid end) accounts_able_to_draw_with_balance
, count(CASE WHEN unable_to_draw_no_balance=1 THEN fbbid end) accounts_unable_to_draw_no_balance
, count(CASE WHEN unable_to_draw_with_balance=1 THEN fbbid end) accounts_unable_to_draw_with_balance
---
,count(CASE WHEN unable_to_draw_no_balance = 1 AND disabled_due_to_credit_reason=1 THEN fbbid END) disabled_credit_reason_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 AND is_rmr=1 THEN fbbid END) disabled_rmr_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 AND disabled_payoff=1 THEN fbbid END) disabled_payoff_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and disabled_delq = 1 THEN fbbid END) disabled_delq_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and early_delq_not_restr = 1 THEN fbbid END) early_delq_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and delq_urp_eligible = 1 THEN fbbid END) delq_urp_eligible_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and delq_urp_ineligible = 1 THEN fbbid END) delq_urp_ineligible_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and delq_perm_restr = 1 THEN fbbid END) delq_perm_restr_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and disabled_credit_other_reason = 1 THEN fbbid END) disabled_credit_other_reason_no_balance
---
,count(CASE WHEN unable_to_draw_no_balance = 1 and (disabled_due_to_ba_disconnection = 1
OR disabled_due_to_as_disconnection=1) THEN fbbid END) disabled_other_reason_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and disabled_due_to_as_disconnection = 1 THEN fbbid END) disabled_as_disconnection_no_balance
,count(CASE WHEN unable_to_draw_no_balance = 1 and disabled_due_to_ba_disconnection = 1 THEN fbbid END) disabled_ba_disconnection_no_balance
---
---
,count(CASE WHEN unable_to_draw_with_balance = 1 AND disabled_due_to_credit_reason=1 THEN fbbid END) disabled_credit_reason_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 AND is_rmr=1 THEN fbbid END) disabled_rmr_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 AND disabled_payoff=1 THEN fbbid END) disabled_payoff_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and disabled_delq = 1 THEN fbbid END) disabled_delq_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and early_delq_not_restr = 1 THEN fbbid END) early_delq_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and delq_urp_eligible = 1 THEN fbbid END) delq_urp_eligible_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and delq_urp_ineligible = 1 THEN fbbid END) delq_urp_ineligible_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and delq_perm_restr = 1 THEN fbbid END) delq_perm_restr_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and disabled_credit_other_reason = 1 THEN fbbid END) disabled_credit_other_reason_with_balance
----
,count(CASE WHEN unable_to_draw_with_balance = 1 and (disabled_due_to_ba_disconnection = 1
OR disabled_due_to_as_disconnection=1) THEN fbbid END) disabled_other_reason_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and disabled_due_to_as_disconnection = 1 THEN fbbid END) disabled_as_disconnection_with_balance
,count(CASE WHEN unable_to_draw_with_balance = 1 and disabled_due_to_ba_disconnection = 1 THEN fbbid END) disabled_ba_disconnection_with_balance
----
----
, (early_delq_no_balance + early_delq_with_balance) as early_delq_all
, (delq_urp_eligible_no_balance + delq_urp_eligible_with_balance) as elq_urp_eligible_all
, (delq_perm_restr_no_balance + delq_perm_restr_with_balance) as delq_perm_restr_all
---
-----
, count(CASE WHEN customer_drew=1 THEN fbbid end) accounts_drew
-----
, (accounts_able_to_draw_no_balance + accounts_able_to_draw_with_balance) as percent_drew_den
-------
, div0(accounts_drew, (accounts_able_to_draw_no_balance + accounts_able_to_draw_with_balance)) percent_drew
-------
, count(CASE WHEN customer_drew_no_balance=1 THEN fbbid end) accounts_drew_no_balance
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw IS NULL THEN fbbid END) drew_no_balance_never_drawn
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw <= 30 THEN fbbid END) drew_no_balance_0_1_months
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw BETWEEN 31 and 90 THEN fbbid END) drew_no_balance_1_3_months
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw BETWEEN 91 and 180 THEN fbbid END) drew_no_balance_3_6_months
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw BETWEEN 181 and 365 THEN fbbid END) drew_no_balance_6_12_months
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw BETWEEN 366 and 730 THEN fbbid END) drew_no_balance_12_24_months
, count(CASE WHEN customer_drew_no_balance = 1 and days_since_last_draw >= 731 THEN fbbid END) drew_no_balance_24_months
------
, count(CASE WHEN customer_drew_no_balance=0 and customer_drew_with_balance = 1 THEN fbbid END) accounts_drew_with_balance
, count(CASE WHEN customer_drew_no_balance=0 and customer_drew_with_balance = 1
and utilization_before_draw < 0.25 THEN fbbid END) drew_with_balance_0_25_util
, count(CASE WHEN customer_drew_no_balance=0 and customer_drew_with_balance = 1
and utilization_before_draw >= 0.25 and utilization_before_draw < 0.5 THEN fbbid END) drew_with_balance_25_50_util
, count(CASE WHEN customer_drew_no_balance=0 and customer_drew_with_balance = 1
and utilization_before_draw >= 0.5 and utilization_before_draw < 0.75 THEN fbbid END) drew_with_balance_50_75_util
, count(CASE WHEN customer_drew_no_balance=0 and customer_drew_with_balance = 1
and utilization_before_draw >= 0.75 THEN fbbid END) drew_with_balance_75_100_util
-----
, count(CASE WHEN charged_off_this_week=1 THEN fbbid END ) accounts_charged_off_this_week
------
----
, count(CASE WHEN co_and_mob_less_than_180_days = 1 THEN fbbid END ) ACCOUNTS_CHARGED_OFF_less_than_180_days
, count(CASE WHEN co_and_mob_less_than_1_year = 1  THEN fbbid END) ACCOUNTS_CHARGED_OFF_less_than_1_year
, count(CASE WHEN co_and_mob_less_than_2_years = 1 THEN fbbid END) ACCOUNTS_CHARGED_OFF_less_than_2_years
, count(CASE WHEN co_and_mob_greater_than_2_years = 1 THEN fbbid END) ACCOUNTS_CHARGED_OFF_greater_than_2_years
------
, count(CASE WHEN  co_this_week_and_mob_less_than_180_days = 1 THEN fbbid END ) ACCOUNTS_CHARGED_OFF_THIS_WEEK_less_than_180_days
, count(CASE WHEN  co_this_week_and_mob_less_than_1_year = 1 THEN fbbid END) ACCOUNTS_CHARGED_OFF_THIS_WEEK_less_than_1_year
, count(CASE WHEN  co_this_week_and_mob_less_than_2_years = 1 THEN fbbid END) ACCOUNTS_CHARGED_OFF_THIS_WEEK_less_than_2_years
, count(CASE WHEN  co_this_week_and_mob_greater_than_2_years = 1 THEN fbbid END) ACCOUNTS_CHARGED_OFF_THIS_WEEK_greater_than_2_years
----
-----
, count(CASE WHEN closed_exposure_management_this_week=1 AND closed_exposure_management_last_week = 0 THEN fbbid end) accounts_closed_exposure_management_this_week
, count(CASE WHEN closed_exposure_management_this_week_inactive_flow=1 AND closed_exposure_management_last_week = 0 THEN fbbid end) accounts_closed_exposure_management_this_week_inactive_flow
, count(CASE WHEN closed_exposure_management_this_week_expedited_anc_flow=1 AND closed_exposure_management_last_week = 0 THEN fbbid end) accounts_closed_exposure_management_this_week_expedited_anc_flow
, count(CASE WHEN closed_exposure_management_this_week_expedited_inactive_flow=1 AND closed_exposure_management_last_week = 0 THEN fbbid end) accounts_closed_exposure_management_this_week_expedited_inactive_flow
, count(CASE WHEN closed_exposure_management_this_week_anc_flow=1 AND closed_exposure_management_last_week = 0 THEN fbbid end) accounts_closed_exposure_management_this_week_anc_flow
-------
--------
, SUM(CASE WHEN closed_exposure_management_this_week=1 AND closed_exposure_management_last_week = 0 THEN EXPOSURE_END_OF_LAST_WEEK end) EXP_closed_exposure_management_this_week
, SUM(CASE WHEN closed_exposure_management_this_week_inactive_flow=1 AND closed_exposure_management_last_week = 0 THEN EXPOSURE_END_OF_LAST_WEEK end) EXP_closed_exposure_management_this_week_inactive_flow
, SUM(CASE WHEN closed_exposure_management_this_week_expedited_anc_flow=1 AND closed_exposure_management_last_week = 0 THEN EXPOSURE_END_OF_LAST_WEEK end) EXP_closed_exposure_management_this_week_expedited_anc_flow
, SUM(CASE WHEN closed_exposure_management_this_week_expedited_inactive_flow=1 AND closed_exposure_management_last_week = 0 THEN EXPOSURE_END_OF_LAST_WEEK end) EXP_closed_exposure_management_this_week_expedited_inactive_flow
, SUM(CASE WHEN closed_exposure_management_this_week_anc_flow=1 AND closed_exposure_management_last_week = 0 THEN EXPOSURE_END_OF_LAST_WEEK end) EXP_closed_exposure_management_this_week_anc_flow
---------
, count(CASE WHEN closed_by_customer_this_week=1 THEN fbbid end) accounts_closed_cust_this_week
-----
,count(CASE WHEN closed_by_other_reason_this_week=1 THEN fbbid END) accounts_closed_other_this_week
,count(CASE WHEN closed_by_rmr_review_this_week=1 THEN fbbid end) accounts_closed_rmr_this_week
-----
------
,count(CASE WHEN (closed_exposure_management_this_week=1 OR closed_by_customer_this_week=1 OR closed_by_other_reason_this_week=1 OR closed_by_rmr_review_this_week = 1 ) THEN fbbid END) total_closed_this_week
------
-------
, count(CASE WHEN disabled_due_to_credit_reason=1 AND disabled_due_to_credit_reason_last_week=0 AND disabled_credit_other_reason_this_week = 0 THEN fbbid end)
accounts_disabled_credit_reason_this_week
--------
, count(CASE WHEN disabled_due_to_credit_reason=1 AND disabled_due_to_credit_reason_last_week=1 AND disabled_credit_other_reason_this_week = 0 THEN fbbid end)
accounts_disabled_credit_reason_last_week_this_week
, count(CASE WHEN disabled_credit_other_reason_this_week=1 AND disabled_due_to_credit_reason_last_week= 0 THEN fbbid END) accounts_disabled_credit_other_reason_this_week
-------
, count(CASE WHEN disabled_credit_other_reason_this_week=1 AND disabled_due_to_credit_reason_last_week= 1 THEN fbbid END) accounts_disabled_credit_other_reason_last_week_and_this_week
-------
-------
, count(CASE WHEN disabled_payoff_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_payoff_this_week
, count(CASE WHEN disabled_rmr_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_this_week
, sum(CASE WHEN disabled_rmr_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_LAST_week END ) exposure_rmr_this_week
, count(CASE WHEN t1.unrmr_this_week > 0 THEN fbbid END ) accounts_unrmr_this_week
, sum(t1.unrmr_this_week) exposure_unrmr_this_week
-------
, count(CASE WHEN disabled_rmr_og_11_fico_less_550_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_og_11_fico_less_550_this_week
, count(CASE WHEN disabled_rmr_mle_greater_96_this_week =1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_mle_greater_96_this_week
, count(CASE WHEN disabled_rmr_DQ_in_30_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_DQ_in_30_this_week
, count(CASE WHEN disabled_rmr_OG_9_10_partners=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_OG_9_10_partners
, count(CASE WHEN disabled_rmr_vantage_570_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_vantage_570_this_week
, count(CASE WHEN disabled_rmr_others_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_rmr_others_this_week
-------
, sum(CASE WHEN disabled_rmr_og_11_fico_less_550_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_og_11_fico_less_550_this_week
, sum(CASE WHEN disabled_rmr_mle_greater_96_this_week =1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_mle_greater_96_this_week
, sum(CASE WHEN disabled_rmr_DQ_in_30_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_DQ_in_30_this_week
, sum(CASE WHEN disabled_rmr_OG_9_10_partners=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_OG_9_10_partners
, sum(CASE WHEN disabled_rmr_vantage_570_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_vantage_570_this_week
, sum(CASE WHEN disabled_rmr_others_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN exposure_end_of_week end) exposure_disabled_rmr_others_this_week
-----
-----
, count(CASE WHEN disabled_payoff_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_payoff_this_week
------
, count(CASE WHEN disabled_rmr_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_this_week
-------
, count(CASE WHEN disabled_rmr_og_11_fico_less_550_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_og_11_fico_less_550_this_week
, count(CASE WHEN disabled_rmr_mle_greater_96_this_week =1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_mle_greater_96_this_week
, count(CASE WHEN disabled_rmr_DQ_in_30_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_DQ_in_30_this_week
, count(CASE WHEN disabled_rmr_OG_9_10_partners=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_OG_9_10_partners
, count(CASE WHEN disabled_rmr_vantage_570_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_vantage_570_this_week
, count(CASE WHEN disabled_rmr_others_this_week=1 AND disabled_due_to_credit_reason_last_week=1 THEN fbbid end) accounts_disabled_last_week_rmr_others_this_week
-------
------
--------
, count(CASE WHEN disabled_delq_this_week=1 AND disabled_due_to_credit_reason_last_week=0 THEN fbbid end) accounts_disabled_delq_this_week
------
, count(CASE WHEN disabled_delq_this_week=1 AND disabled_due_to_credit_reason_last_week=1 AND account_charged_off = 0 THEN fbbid end) accounts_disabled_last_week_delq_this_week
-------
--------
-------
, count(CASE WHEN disabled_due_to_other_reason=1 AND disabled_due_to_other_reason_last_week = 0 AND disabled_due_to_ba_as_reason_last_week = 0 THEN fbbid end) accounts_disabled_non_ba_as_reasons_this_week
, count(CASE WHEN disabled_due_to_other_reason = 1  AND disabled_due_to_other_reason_last_week = 0 THEN fbbid end) accounts_disabled_other_reasons_this_week
, count(CASE WHEN disabled_due_to_ba_disconnection=1 AND disabled_due_to_ba_as_reason_last_week = 0 AND disabled_due_to_other_reason_last_week = 0 THEN fbbid end) accounts_disabled_ba_disconnection_this_week
, count(CASE WHEN disabled_due_to_as_disconnection=1 AND disabled_due_to_ba_as_reason_last_week = 0 AND disabled_due_to_other_reason_last_week = 0 THEN fbbid end) accounts_disabled_as_disconnection_this_week
------
------
, (COALESCE(accounts_disabled_credit_other_reason_this_week,0) + COALESCE(accounts_disabled_payoff_this_week,0) + COALESCE(accounts_disabled_rmr_this_week,0) + COALESCE(accounts_disabled_delq_this_week,0) + COALESCE(accounts_disabled_non_ba_as_reasons_this_week,0) + COALESCE(accounts_disabled_ba_disconnection_this_week,0) + COALESCE(accounts_disabled_as_disconnection_this_week,0)) ACCOUNTS_DISABLED_THIS_WEEK_TOTAL
-----
,count(DISTINCT CASE WHEN open_accounts=1 THEN fbbid end) open_accts
,count(CASE WHEN new_approvals=1 THEN fbbid end) new_approvals_open
,count(CASE WHEN suspended_this_week=1 THEN fbbid end) suspended_this_week_open
,count(CASE WHEN co_not_suspended_this_Week=1 THEN fbbid end) co_not_suspended_this_Week_open
-------
,count(DISTINCT CASE WHEN active_accounts=1 THEN fbbid end) active_accts
,count(CASE WHEN new_ftd=1 THEN fbbid end) new_ftd
,count(CASE WHEN new_customers_with_balance=1 THEN fbbid end) new_customers_with_balance
,count(CASE WHEN active_last_suspended_this_week=1 THEN fbbid end) active_last_suspended_this_week
,count(CASE WHEN active_last_co_this_week=1 THEN fbbid end) active_last_co_this_week
,count(CASE WHEN paid_off_this_week=1 THEN fbbid end) paid_off_this_week
---------
,div0(active_accts,open_accts) pct_active_open
-------
,count(CASE WHEN paid_off_this_week=1 and active_accounts_last_week=1 THEN fbbid END) good_churn_num
,count(CASE WHEN active_last_suspended_this_week=1 OR active_last_co_this_week=1 THEN fbbid END) bad_churn_num
,count(CASE WHEN balance_end_of_week_last_week=0 and balance_end_of_week=1 THEN fbbid END) net_churn_exc
,good_churn_num + bad_churn_num - net_churn_exc AS net_churn_NUM
-------
,count(CASE WHEN active_accounts_last_week=1 or new_ftd=1 THEN fbbid END) churn_den
-------
,div0(good_churn_num, churn_den) good_churn_rate
,div0(bad_churn_num, churn_den) bad_churn_rate
,div0((good_churn_num + bad_churn_num - net_churn_exc), churn_den) net_churn_rate
------
,count(CASE WHEN active_accounts_last_week=1 THEN FBBID END) AS charge_off_perc_den
------
------
,div0(count(CASE WHEN active_last_co_this_week=1 THEN fbbid END), count(CASE WHEN active_accounts_last_week=1 THEN FBBID END)) charge_off_perc
------
-------
,count(DISTINCT CASE WHEN able_to_draw=1 THEN fbbid end) able_to_draw_accts
,count(CASE WHEN new_approvals_able_to_draw=1 THEN fbbid end) new_approvals_able_to_draw
,count(CASE WHEN credit_disabled_to_enabled_this_week=1 THEN fbbid end) credit_disabled_to_enabled_this_week
,count(CASE WHEN other_reasons_disabled_to_enabled_this_week=1 THEN fbbid end) other_reasons_disabled_to_enabled_this_week
,count(CASE WHEN ba_as_reasons_disabled_to_enabled_this_week=1 THEN fbbid end) ba_as_reasons_disabled_to_enabled_this_week
,-1 * count(CASE WHEN able_to_draw_suspended_this_week=1 THEN fbbid end) able_to_draw_suspended_this_week
,-1 * count(CASE WHEN credit_enabled_to_disabled_this_week=1 THEN fbbid end) credit_enabled_to_disabled_this_week
,-1 * count(CASE WHEN other_reasons_enabled_to_disabled_this_week=1 THEN fbbid end) other_reasons_enabled_to_disabled_this_week
,-1 * count(CASE WHEN ba_as_reasons_enabled_to_disabled_this_week=1 THEN fbbid end) ba_as_reasons_enabled_to_disabled_this_week
-------
,AVG(able_to_draw_available_balance) * able_to_draw_accts total_able_to_draw_available_balance 
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 THEN fbbid end) bank_account_disconnections
,count(DISTINCT CASE WHEN ba_reconnect_30_days=1 THEN fbbid end) ba_30_day_reconnect_num
,bank_account_disconnections ba_30_day_reconnect_den
--------
,div0(count(DISTINCT CASE WHEN ba_reconnect_30_days=1 THEN fbbid end),bank_account_disconnections) BA_30_day_reconnect_rate
-------
-------
, count(DISTINCT CASE WHEN ba_reconnect_30_days=1 and able_to_draw_no_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_draw_no_bal_num
, count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and able_to_draw_no_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_draw_no_bal_den
------
, count(DISTINCT CASE WHEN ba_reconnect_30_days=1 and able_to_draw_with_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_draw_bal_num
, count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and able_to_draw_with_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_draw_bal_den
------
, count(DISTINCT CASE WHEN ba_reconnect_30_days=1 and unable_to_draw_no_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_no_draw_no_bal_num
, count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and unable_to_draw_no_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_no_draw_no_bal_den
------
------
, count(DISTINCT CASE WHEN ba_reconnect_30_days=1 and unable_to_draw_with_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_no_draw_bal_num
, count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and unable_to_draw_with_balance=1 THEN fbbid end) BA_30_day_reconnect_rate_no_draw_bal_den
--------
---------
--------
--------
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and able_to_draw_with_balance=1 THEN fbbid END) bank_account_disconnections_bal_able_to_draw
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and able_to_draw_no_balance=1 THEN fbbid END) bank_account_disconnections_no_bal_able_to_draw
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and unable_to_draw_with_balance=1 THEN fbbid END) bank_account_disconnections_bal_unable_to_draw
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and unable_to_draw_no_balance=1 THEN fbbid END) bank_account_disconnections_no_bal_unable_to_draw
,count(DISTINCT CASE WHEN bank_not_connected_this_week=1 AND bank_not_connected_last_week=0 and (account_closed_by_customer=1 or account_closed_by_fundbox=1 or account_charged_off=1) THEN fbbid END) bank_account_disconnections_closed
--------
--------
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 THEN fbbid end) as_disconnections
,count(DISTINCT CASE WHEN as_reconnect_30_days=1 THEN fbbid end) as_30_day_reconnect_num
,as_disconnections as_30_day_reconnect_den
--------
,div0(count(DISTINCT CASE WHEN as_reconnect_30_days=1 THEN fbbid end),as_disconnections) as_30_day_reconnect_rate
--------
--------
, count(DISTINCT CASE WHEN as_reconnect_30_days=1 and able_to_draw_no_balance=1 THEN fbbid end) as_30_day_reconnect_rate_draw_no_bal_num
, count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and able_to_draw_no_balance=1 THEN fbbid end) as_30_day_reconnect_rate_draw_no_bal_den
--------
, count(DISTINCT CASE WHEN as_reconnect_30_days=1 and able_to_draw_with_balance=1 THEN fbbid end) as_30_day_reconnect_rate_draw_bal_num
, count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and able_to_draw_with_balance=1 THEN fbbid end) as_30_day_reconnect_rate_draw_bal_den
--------
, count(DISTINCT CASE WHEN as_reconnect_30_days=1 and unable_to_draw_no_balance=1 THEN fbbid end) as_30_day_reconnect_rate_no_draw_no_bal_num
, count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and unable_to_draw_no_balance=1 THEN fbbid end) as_30_day_reconnect_rate_no_draw_no_bal_den
--------
, count(DISTINCT CASE WHEN as_reconnect_30_days=1 and unable_to_draw_with_balance=1 THEN fbbid end) as_30_day_reconnect_rate_no_draw_bal_num
, count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and unable_to_draw_with_balance=1 THEN fbbid end) as_30_day_reconnect_rate_no_draw_bal_den
--------
--------
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and able_to_draw_with_balance=1 THEN fbbid END) as_disconnections_bal_able_to_draw
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and able_to_draw_no_balance=1 THEN fbbid END) as_disconnections_no_bal_able_to_draw
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and unable_to_draw_with_balance=1 THEN fbbid END) as_disconnections_bal_unable_to_draw
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and unable_to_draw_no_balance=1 THEN fbbid END) as_disconnections_no_bal_unable_to_draw
,count(DISTINCT CASE WHEN as_not_connected_this_week=1 AND as_not_connected_last_week=0 and (account_closed_by_customer=1 or account_closed_by_fundbox=1 or account_charged_off=1) THEN fbbid END) as_disconnections_closed
--------
,avg( CASE WHEN open_accounts=1 THEN util_end_of_week END) util_open_customers
--------
--------
,sum( CASE WHEN open_accounts=1 THEN util_end_of_week END) util_open_customers_numerator
,count( CASE WHEN open_accounts=1 THEN util_end_of_week  END) util_open_customers_denominator
--------
----------
--------
,sum( CASE WHEN open_accounts=1 THEN exposure_end_of_week END) exposure_open_customers
,sum( CASE WHEN open_accounts=1 THEN otb_end_of_week END) otb_open_customers
--------
--------
,avg( CASE WHEN active_accounts=1 THEN util_end_of_week END) util_active_customers
--------
--------
,sum( CASE WHEN active_accounts=1 THEN util_end_of_week END) util_active_customers_numerator
,count( CASE WHEN active_accounts=1 THEN util_end_of_week END) util_active_customers_denominator
--------
--------
,sum( CASE WHEN active_accounts=1 THEN exposure_end_of_week END) exposure_active_customers
,sum( CASE WHEN active_accounts=1 and able_to_draw=1 and is_rmr=0 THEN otb_end_of_week END) otb_active_customers
--------
,avg( CASE WHEN able_to_draw=1 THEN util_end_of_week END) util_able_to_draw_customers
--------
--------
,sum( CASE WHEN able_to_draw=1 THEN util_end_of_week END) util_able_to_draw_customers_numerator
,count( CASE WHEN able_to_draw=1 THEN util_end_of_week END) util_able_to_draw_customers_denominator
--------
--------
,sum( CASE WHEN able_to_draw=1 THEN exposure_end_of_week END) exposure_able_to_draw_customers
,sum( CASE WHEN able_to_draw=1 THEN otb_end_of_week END) otb_able_to_draw_customers
,sum( CASE WHEN able_to_draw_last_week=1 THEN otb_end_of_week_last_week END) otb_able_to_draw_customers_last_week
,sum( CASE WHEN able_to_draw_last_week=1 and active_accounts_last_week = 1 THEN otb_end_of_week_last_week END) otb_able_to_draw_active_customers_last_week
--------
, count(CASE WHEN clip_this_week=1 THEN fbbid end) accounts_clip_this_week
, ifnull(sum(clip_increased_line_this_week), 0) increased_line_clips_this_week
--------
--------
, count(CASE WHEN automated_cld_this_week=1 THEN fbbid end) accounts_cld_this_week
, ifnull(sum(automated_cld_this_week_exp), 0) decreased_line_cld_this_week
--------
, count(CASE WHEN inactive_offer_made=1 THEN fbbid end) offers_presented
, count(CASE WHEN inactive_offer_accepted=1 THEN fbbid end) offers_accepted
, ifnull(sum(offer_increased_line), 0) increased_line_offers_this_week
--------
, count(CASE WHEN boost_xl_this_week=1 THEN fbbid end) accounts_boost_xl_this_week
, ifnull(sum(boost_xl_increased_line_this_week), 0) increased_line_boost_xl_this_week
--------
, (increased_line_clips_this_week + increased_line_offers_this_week + increased_line_boost_xl_this_week) inc_line
,sum( CASE WHEN open_accounts_last_week=1 THEN exposure_end_of_last_week END) open_line_last_week
--------
--------
,sum(open_outstanding) open_outstanding
, div0(inc_line, open_line_last_week) exp_inc_from_offers
--------
, sum(og_auw_line_increase_amts) og_auw_amount_increased
, count(CASE WHEN og_auw_increases = 1 THEN fbbid ELSE NULL end) og_auw_accts_increased 
--------
--------
--ANALYTICS.credit.customer_experience_metric
FROM analytics.credit.customer_management_lagtable_pb T1
LEFT JOIN TEMP_TABLE T2
ON T1.week_start_DATE = T2.week_start_DATE
AND T1.week_end_DATE = T2.WEEK_END_DATE
--------
WHERE T1.week_start_DATE >= to_date('2020-12-30')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13);

create or replace table analytics.credit.customer_revenue_perc as (

  -- Open accounts
  select
    T1.WEEK_END_DATE,
    'open' as category,
    avg(customer_annual_revenue) as avg_revenue,
    percentile_cont(0.25) within group (order by customer_annual_revenue) as perc_25,
    percentile_cont(0.50) within group (order by customer_annual_revenue) as perc_50,
    percentile_cont(0.75) within group (order by customer_annual_revenue) as perc_75,
    --percentile_cont(1) within group (order by customer_annual_revenue) as perc_90
  from analytics.credit.customer_management_lagtable_pb T1
  where T1.open_accounts = 1
  AND T1.tenure_status = 'Tenure >=60'
  group by T1.WEEK_END_DATE

  union all

  -- Active accounts
  select
    T1.WEEK_END_DATE,
    'active' as category,
    avg(customer_annual_revenue) as avg_revenue,
    percentile_cont(0.25) within group (order by customer_annual_revenue) as perc_25,
    percentile_cont(0.50) within group (order by customer_annual_revenue) as perc_50,
    percentile_cont(0.75) within group (order by customer_annual_revenue) as perc_75,
    --percentile_cont(1) within group (order by customer_annual_revenue) as perc_90
  from analytics.credit.customer_management_lagtable_pb T1
  where T1.active_accounts = 1
  AND T1.tenure_status = 'Tenure >=60'
  group by T1.WEEK_END_DATE

  union all

  -- Able to draw accounts
  select
    T1.WEEK_END_DATE,
    'atd' as category,
    avg(customer_annual_revenue) as avg_revenue,
    percentile_cont(0.25) within group (order by customer_annual_revenue) as perc_25,
    percentile_cont(0.50) within group (order by customer_annual_revenue) as perc_50,
    percentile_cont(0.75) within group (order by customer_annual_revenue) as perc_75,
    --percentile_cont(1) within group (order by customer_annual_revenue) as perc_90
  from analytics.credit.customer_management_lagtable_pb T1
  where T1.able_to_draw = 1
  AND T1.tenure_status = 'Tenure >=60'
  group by T1.WEEK_END_DATE

);