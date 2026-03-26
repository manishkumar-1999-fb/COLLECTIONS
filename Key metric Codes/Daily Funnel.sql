create or replace view INDUS.PUBLIC.V_INDUS_KEY_METRICS_FUNNEL_DAILY_AGG(
	CURR_DATE,
	CHANNEL,
	TIER,
	SUB_PRODUCT,
	BUCKET_GROUP,
	PARTNER,
	INTUIT_FLOW,
	NAV_FLOW,
	LENDIO_FLOW,
	NATIONAL_FUNDING_FLOW,
	REGISTRATIONS,
	CIP_CONNECTIONS,
	CONNECTIONS,
	REG_FLOW_COMPLETED,
	RISK_REVIEWED,
	UNDERWRITTEN_OLD,
	UNDERWRITTEN,
	UNDERWRITTEN_PQ_MONTHS_IN_BUSINESS,
	UNDERWRITTEN_HAS_PQ_MONTHS_IN_BUSINESS,
	UNDERWRITTEN_CALC_REVENUE,
	UNDERWRITTEN_HAS_CALC_REVENUE,
	DECISIONS,
	APPROVALS,
	REJECTIONS,
	DATA_REJECTIONS,
	FICO_599_REJECTIONS,
	CREDIT_REJECTIONS,
	FRAUD_REJECTIONS,
	POLICY_MODEL_REJECTIONS,
	OTHER_REJECTIONS,
	FTDS,
	FTDS_0_7,
	FTDS_8_28,
	FTDS_29_60,
	FTDS_61_,
	FTDS_APP,
	FTDS_1,
	FTDS_3,
	SUM_FICO_UND,
	SUM_FICO_APP,
	APP_CREDIT_LIMIT,
	FTD_CREDIT_LIMIT,
	FIRST_DRAW_AMOUNT
) as 



	WITH funnel AS (


	SELECT DISTINCT cd.fbbid

	, f.channel
	, f.partner
	, f.intuit_flow
	, f.national_funding_flow
	, f.nav_flow
	, f.lendio_flow
	, f.tier
	, f.sub_product
	, CASE 
		WHEN cd.is_approved = 1 THEN f.ob_dal_bucket
		ELSE f.ob_risk_bucket_first
	END risk_bucket

	/*, CASE 
		WHEN cd.is_approved = 1 THEN f.ob_risk_bucket_approved
		ELSE f.ob_risk_bucket_first
	END risk_bucket
	*/
	/*
	, CASE 
		WHEN cd.is_approved = 1 THEN f.ob_bucket_group_approved
		ELSE f.ob_bucket_group_first
	END bucket_group
	*/

	-- 7 Change for OB DAL bucket_group
	, ob_bucket_group_dal as bucket_group

	, cd.registration_time
	, cd.registration_time::date reg_date


	, cd.is_cip_connected
	, cd.cip_connected_time::date cip_connected_date


	, cd.is_connected
	, cd.first_connected_time::date first_connected_date


	, cd.is_registration_flow_completed
	, cd.registration_flow_completed_time::date registration_flow_completed_date

	, CASE WHEN cd.FIRST_RISK_REVIEW_TIME IS NULL THEN 0 ELSE 1 END AS is_risk_review
	, cd.first_risk_review_time::date risk_review_date


	, cd.is_underwriting is_underwriting_old
	, CASE 
		WHEN cd.is_underwriting = 1 THEN 1 
		-- WHEN cd.current_credit_status_reason in ('Onboarding dynamic decision reject') then 1
		WHEN cd.first_rejected_reason in ('Onboarding dynamic decision reject') then 1
		ELSE 0
	END AS is_underwriting_new

	, CASE WHEN cd.first_decision_time IS NULL THEN 0 ELSE 1 END AS is_decision
	, cd.first_decision_time::date first_dec_date

	, MONTH(cd.registration_time::date) - pq.IN_BUSINESS_SINCE_MONTH + (YEAR(cd.registration_time::date) - pq.IN_BUSINESS_SINCE_YEAR)*12 pq_months_in_business
	, CASE WHEN pq.IN_BUSINESS_SINCE_MONTH IS NOT NULL THEN 1 ELSE 0 END has_pq_months_in_business
	, COALESCE(first_account_size_accounting_software,first_account_size_fi,0) * 12 calc_revenue
	, CASE WHEN first_account_size_accounting_software IS NOT NULL THEN 1 WHEN first_account_size_fi IS NOT NULL THEN 1 ELSE 0 END has_calc_revenue

	, cd.first_approved_time
	, cd.first_approved_time::date first_approved_date

	, cd.is_approved

	-----------29 feb addition for rejection reason------

	/*, CASE 
		WHEN cd.is_underwriting = 1 AND cd.is_approved = 0 THEN 1
		WHEN cd.is_underwriting = 1 AND cd.is_approved = 1 THEN 0
		WHEN cd.is_underwriting = 1 AND cd.is_approved IS NULL THEN 1
		WHEN cd.is_underwriting = 0 THEN NULL 
	END AS is_rejected*/

	, FEB.REJECTION_REASON 
	, FEB.Data_Rule
	, FEB.Credit_Rule
	, FEB.fraud_Rule

	, CASE 
		WHEN FEB.REJECTION_REASON = 'Fraud Rejection' and current_credit_status_reason in ('Rejected for fraud', 'Unsupported geolocation by ip') THEN 'Fraud/Compliance Rejection'
		WHEN FEB.REJECTION_REASON = 'Fraud Rejection' and current_Credit_Status_reason not in ('Rejected for fraud', 'Unsupported geolocation by ip','Reject but got a chance to manual review by fraud') then 'Policy Rejection/Model Rejection'
		WHEN FEB.REJECTION_REASON = 'Fraud Rejection' then 'Not Rejected'
		ELSE FEB.REJECTION_REASON
		END as REJECTION_REASON_FUNNEL

	, CASE 
		WHEN rejection_reason_funnel IS NOT NULL THEN rejection_reason_funnel
		WHEN rejection_reason_funnel IS NULL and cd.CURRENT_CREDIT_STATUS ='rejected' THEN 'Other Rejections'
		WHEN rejection_reason_funnel IS NULL and cd.CURRENT_CREDIT_STATUS !='rejected' THEN 'Not Rejected'
		else 'Check cases'
		end as REJECTION_REASON_2

	, CASE 
		WHEN REJECTION_REASON_2 = 'Not Rejected' THEN 0
		--WHEN FEB.REJECTION_REASON is Null and cd.CURRENT_CREDIT_STATUS !='rejected' THEN 0
		--WHEN FEB.REJECTION_REASON is Null and cd.CURRENT_CREDIT_STATUS ='rejected' THEN 1
		ELSE 1
		END AS is_rejected

	/*
	, CASE 
		WHEN is_rejected = 0 THEN NULL 
		WHEN is_rejected IS NULL THEN NULL 
		WHEN cd.state IN ('SD', 'NM') THEN 'State'
		WHEN cd.state = 'CO' and cd.first_aligned_bucket > 6 then 'State'
		WHEN cd.LAST_IS_APPROVED_BY_DATA_RULES = 0 THEN 'Data Rules'
		WHEN cd.fico_onboarding < 600 THEN 'FICO < 600'
		WHEN cd.LAST_IS_APPROVED_BY_CREDIT_RULES = 0 THEN 'Credit Rules'
		WHEN cd.LAST_IS_APPROVED_BY_FRAUD_RULES = 0 THEN 'Fraud Rules'
		ELSE 'Policy'
	END AS rejected_reason */
		
	, cd.first_rejected_reason

	, cd.is_ftu
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) <= 7 THEN 1 ELSE 0 END AS is_ftd_0_7
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) BETWEEN 8 AND 28 THEN 1 ELSE 0 END AS is_ftd_8_28
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) BETWEEN 29 AND 60 THEN 1 ELSE 0 END AS is_ftd_29_60
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) >= 61 THEN 1 ELSE 0 END AS is_ftd_61_


	, cd.first_draw_time
	, cd.first_draw_time::date first_draw_date

	, cd.fico_onboarding fico
	, cd.first_approved_credit_limit
	, cd.first_draw_amount fda
	, dacd.credit_limit as credit_limit_at_ftd

	--, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<8 THEN 1 END AS is_ftd7
	--, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<29 THEN 1 END AS is_ftd28

	-- 13 Feb 2023 Change: FTD definition change
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<2 THEN 1 END AS is_ftd1
	, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<4 THEN 1 END AS is_ftd3


	--FROM bi."PUBLIC".CUSTOMERS_DATA cd 
	FROM BI.PUBLIC.CUSTOMERS_DATA cd 

	--CHANGE MADE ON 2024-03-12 FOR CREDIT LIMIT AT THE TIME OF FIRST DRAW

	LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd 
	on cd.fbbid = dacd.fbbid 
	and cd.first_draw_time::date = dacd.edate

	--LEFT JOIN ANALYTICS.CREDIT.eg_key_metrics_filters f 
	LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f
	ON cd.fbbid = f.fbbid
	AND f.edate = f.min_edate

	--LEFT JOIN cdc.pre_qual.pre_qual_users pq
	--LEFT JOIN indus."PUBLIC".pre_qual_users_indus pq


	LEFT JOIN (
	select * from 
	(SELECT *, ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY last_modified_time DESC) AS row_num FROM indus."PUBLIC".pre_qual_users_indus a)
	where row_num = 1) pq
	ON cd.fbbid = pq.fbbid


	--LEFT JOIN bi."PUBLIC".APPROVED_CUSTOMERS_DATA acd 
	/*LEFT JOIN indus."PUBLIC".approved_customers_data_indus acd 
	ON cd.fbbid = acd.fbbid*/

	--------29feb add for rejection reasons-------


	LEFT JOIN indus.public.feb_report AS Feb
	ON cd.fbbid = feb.fbbid


	WHERE TRUE 
	--AND reg_date >= '2020-01-01'
	AND is_test = 0
	--AND is_rejected = 1
	--AND rejected_reason = 'Other'
	)

	SELECT a.*
	, reg.registrations
	, cip.cip_connections
	, con.connections
	, flo.reg_flow_completed
	, risk.risk_reviewed
	, und.underwritten_old
	, und.underwritten
	, und.underwritten_pq_months_in_business
	, und.underwritten_has_pq_months_in_business
	, und.underwritten_calc_revenue
	, und.underwritten_has_calc_revenue
	, deci.decisions
	, app.approvals
	, deci.rejections
	--, deci.state_rejections
	, deci.data_rejections
	, deci.fico_599_rejections
	, deci.credit_rejections
	, deci.fraud_rejections
	--, deci.policy_rejections
	, deci.policy_model_rejections
	, deci.other_rejections
	, ftd.ftds
	, ftd.ftds_0_7
	, ftd.ftds_8_28
	, ftd.ftds_29_60
	, ftd.ftds_61_
	, app.ftds_app
	, app.ftds_1 
	, app.ftds_3 
	, und.sum_fico_und
	, app.sum_fico_app
	, app.app_credit_limit
	, ftd.ftd_credit_limit
	, ftd.first_draw_amount

	--FROM ANALYTICS.CREDIT.eg_key_metrics_funnel_structure_daily a
	FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_daily a


	LEFT JOIN (
	SELECT reg_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) registrations
	FROM funnel
	GROUP BY 1,2,3,4,5,6,7,8,9,10) reg
	ON a.curr_date = reg.reg_date
	AND a.channel = reg.channel
	AND a.partner = reg.partner
	AND a.intuit_flow = reg.intuit_flow
	AND a.national_funding_flow = reg.national_funding_flow
	AND a.nav_flow = reg.nav_flow
	AND a.lendio_flow = reg.lendio_flow
	AND a.tier = reg.tier
	AND a.sub_product = reg.sub_product
	AND a.bucket_group = reg.bucket_group


	LEFT JOIN (
	SELECT cip_connected_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) cip_connections
	FROM funnel
	GROUP BY 1,2,3,4,5,6,7,8,9,10) cip
	ON a.curr_date = cip.cip_connected_date
	AND a.channel = cip.channel
	AND a.partner = cip.partner
	AND a.intuit_flow = cip.intuit_flow
	AND a.national_funding_flow = cip.national_funding_flow
	AND a.nav_flow = cip.nav_flow
	AND a.lendio_flow = cip.lendio_flow
	AND a.tier = cip.tier
	AND a.sub_product = cip.sub_product
	AND a.bucket_group = cip.bucket_group


	LEFT JOIN (
	SELECT first_connected_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) connections
	FROM funnel
	GROUP BY 1,2,3,4,5,6,7,8,9,10) con
	ON a.curr_date = con.first_connected_date
	AND a.channel = con.channel
	AND a.partner = con.partner
	AND a.intuit_flow = con.intuit_flow
	AND a.national_funding_flow = con.national_funding_flow
	AND a.nav_flow = con.nav_flow
	AND a.lendio_flow = con.lendio_flow
	AND a.tier = con.tier
	AND a.sub_product = con.sub_product
	AND a.bucket_group = con.bucket_group


	LEFT JOIN (
	SELECT registration_flow_completed_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) reg_flow_completed
	FROM funnel
	GROUP BY 1,2,3,4,5,6,7,8,9,10) flo
	ON a.curr_date = flo.registration_flow_completed_date
	AND a.channel = flo.channel
	AND a.partner = flo.partner
	AND a.intuit_flow = flo.intuit_flow
	AND a.national_funding_flow = flo.national_funding_flow
	AND a.nav_flow = flo.nav_flow
	AND a.lendio_flow = flo.lendio_flow
	AND a.tier = flo.tier
	AND a.sub_product = flo.sub_product
	AND a.bucket_group = flo.bucket_group


	LEFT JOIN (
	SELECT risk_review_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) risk_reviewed
	FROM funnel
	GROUP BY 1,2,3,4,5,6,7,8,9,10) risk
	ON a.curr_date = risk.risk_review_date
	AND a.channel = risk.channel
	AND a.partner = risk.partner
	AND a.intuit_flow = risk.intuit_flow
	AND a.national_funding_flow = risk.national_funding_flow
	AND a.nav_flow = risk.nav_flow
	AND a.lendio_flow = risk.lendio_flow
	AND a.tier = risk.tier
	AND a.sub_product = risk.sub_product
	AND a.bucket_group = risk.bucket_group


	LEFT JOIN (
	SELECT registration_flow_completed_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, sum(is_underwriting_old) underwritten_old
	, count(DISTINCT fbbid) underwritten
	, sum(pq_months_in_business) underwritten_pq_months_in_business
	, sum(has_pq_months_in_business) underwritten_has_pq_months_in_business
	, sum(calc_revenue) underwritten_calc_revenue
	, sum(has_calc_revenue) underwritten_has_calc_revenue
	, sum(fico) sum_fico_und
	FROM funnel
	WHERE is_underwriting_new = 1 -- Distinguishes flow completed FROM underwritten (new logic)
	GROUP BY 1,2,3,4,5,6,7,8,9,10) und
	ON a.curr_date = und.registration_flow_completed_date
	AND a.channel = und.channel
	AND a.partner = und.partner
	AND a.intuit_flow = und.intuit_flow
	AND a.national_funding_flow = und.national_funding_flow
	AND a.nav_flow = und.nav_flow
	AND a.lendio_flow = und.lendio_flow
	AND a.tier = und.tier
	AND a.sub_product = und.sub_product
	AND a.bucket_group = und.bucket_group


	LEFT JOIN (
	SELECT first_dec_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) decisions
	, sum(is_rejected) rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'FICO Sub 600' THEN 1 ELSE 0 END) fico_599_rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'Data Rule Reject' THEN 1 ELSE 0 END) data_rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'Credit Rule Reject' THEN 1 ELSE 0 END) credit_rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'Policy Rejection/Model Rejection' THEN 1 ELSE 0 END) policy_model_rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'Fraud/Compliance Rejection' THEN 1 ELSE 0 END) fraud_rejections
	, sum(CASE WHEN REJECTION_REASON_2 = 'Other Rejections' THEN 1 ELSE 0 END) other_rejections
	FROM funnel
	WHERE is_registration_flow_completed = 1
	AND first_dec_date IS NOT NULL 
	GROUP BY 1,2,3,4,5,6,7,8,9,10) deci
	ON a.curr_date = deci.first_dec_date
	AND a.channel = deci.channel
	AND a.partner = deci.partner
	AND a.intuit_flow = deci.intuit_flow
	AND a.national_funding_flow = deci.national_funding_flow
	AND a.nav_flow = deci.nav_flow
	AND a.lendio_flow = deci.lendio_flow
	AND a.tier = deci.tier
	AND a.sub_product = deci.sub_product
	AND a.bucket_group = deci.bucket_group


	LEFT JOIN (
	SELECT first_approved_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, count(DISTINCT fbbid) approvals
	, sum(is_ftu) ftds_app
	, sum(is_ftd1) ftds_1
	, sum(is_ftd3) ftds_3
	, sum(fico) sum_fico_app
	, sum(first_approved_credit_limit) app_credit_limit
	FROM funnel
	WHERE TRUE 
	AND is_approved = 1
	AND sub_product <> 'Credit Builder'
	GROUP BY 1,2,3,4,5,6,7,8,9,10) app
	ON a.curr_date = app.first_approved_date
	AND a.channel = app.channel
	AND a.partner = app.partner
	AND a.intuit_flow = app.intuit_flow
	AND a.national_funding_flow = app.national_funding_flow
	AND a.nav_flow = app.nav_flow
	AND a.lendio_flow = app.lendio_flow
	AND a.tier = app.tier
	AND a.sub_product = app.sub_product
	AND a.bucket_group = app.bucket_group
	AND app.sub_product <> 'Credit Builder'


	LEFT JOIN (
	SELECT first_draw_date
	, channel
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, sum(is_ftu) ftds
	, sum(is_ftd_0_7) ftds_0_7
	, sum(is_ftd_8_28) ftds_8_28
	, sum(is_ftd_29_60) ftds_29_60
	, sum(is_ftd_61_) ftds_61_
	--change on 12-03-2024 for avg utilization at first draw
	--, sum(first_approved_credit_limit) ftd_credit_limit
	, sum(credit_limit_at_ftd) ftd_credit_limit 
	, sum(fda) first_draw_amount
	FROM funnel
	WHERE TRUE
	AND is_ftu = 1
	AND sub_product <> 'Credit Builder'
	GROUP BY 1,2,3,4,5,6,7,8,9,10) ftd
	ON a.curr_date = ftd.first_draw_date
	AND a.channel = ftd.channel
	AND a.partner = ftd.partner
	AND a.intuit_flow = ftd.intuit_flow
	AND a.national_funding_flow = ftd.national_funding_flow
	AND a.nav_flow = ftd.nav_flow
	AND a.lendio_flow = ftd.lendio_flow
	AND a.tier = ftd.tier
	AND a.sub_product = ftd.sub_product
	AND a.bucket_group = ftd.bucket_group
	AND ftd.sub_product <> 'Credit Builder'


	WHERE TRUE 
	AND (reg.registrations IS NOT NULL 
	OR cip.cip_connections IS NOT NULL 
	OR con.connections IS NOT NULL 
	OR flo.reg_flow_completed IS NOT NULL 
	OR risk.risk_reviewed IS NOT NULL 
	OR und.underwritten_old IS NOT NULL 
	OR und.underwritten IS NOT NULL 
	OR und.underwritten_pq_months_in_business IS NOT NULL 
	OR und.underwritten_has_pq_months_in_business IS NOT NULL 
	OR und.underwritten_calc_revenue IS NOT NULL 
	OR und.underwritten_has_calc_revenue IS NOT NULL 
	OR deci.decisions IS NOT NULL 
	OR app.approvals IS NOT NULL 
	OR deci.rejections IS NOT NULL 
	--OR deci.state_rejections IS NOT NULL 
	OR deci.data_rejections IS NOT NULL 
	OR deci.fico_599_rejections IS NOT NULL 
	OR deci.credit_rejections IS NOT NULL 
	OR deci.fraud_rejections IS NOT NULL 
	--OR deci.policy_rejections IS NOT NULL 
	OR deci.policy_model_rejections
	OR deci.other_rejections
	OR ftd.ftds IS NOT NULL 
	OR ftd.ftds_0_7 IS NOT NULL 
	OR ftd.ftds_8_28 IS NOT NULL 
	OR ftd.ftds_29_60 IS NOT NULL 
	OR ftd.ftds_61_ IS NOT NULL 
	OR app.ftds_app IS NOT NULL 
	OR app.ftds_1  IS NOT NULL 
	OR app.ftds_3  IS NOT NULL 
	OR und.sum_fico_und IS NOT NULL 
	OR app.sum_fico_app IS NOT NULL 
	OR app.app_credit_limit IS NOT NULL 
	OR ftd.ftd_credit_limit IS NOT NULL 
	OR ftd.first_draw_amount IS NOT NULL)

	;