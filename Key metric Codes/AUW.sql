
--------------- MODIFYING HVC SCRIPT ATTEMPT 1 (18/07/2025)------------------------


-----------------------------------------------------------------------------AUW FUNNEL----------------------------------------------------------------------

	CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1_AUW_td AS

    WITH RECURSIVE a AS (
        SELECT DATEFROMPARTS(2020, 12, 30) AS week_end_date
        UNION ALL
        SELECT DATEADD(DAY, 7, week_end_date)
        FROM a
        WHERE week_end_date <= current_date()-7
    )

    , b AS (
    SELECT CASE WHEN dayofweek(current_date()) = 3 THEN current_date() ELSE current_date()-1 END week_end_date
    )

    , channel_table AS (
    SELECT 'Direct' channel UNION SELECT 'Partner' channel UNION SELECT 'Other' channel
    )

    , tier_table AS (
    SELECT 'A' tier UNION SELECT 'B' tier UNION SELECT 'C' tier UNION SELECT 'D' tier UNION SELECT 'F' tier
    )

    , sub_product_table AS (
    SELECT 'Line Of Credit' sub_product UNION SELECT 'Term Loan' sub_product UNION SELECT 'Pay' sub_product UNION SELECT 'Credit Builder' sub_product UNION SELECT 'No Selection' sub_product UNION SELECT 'mca'
    )

    , risk_bucket_table AS (
    SELECT 'No Bucket' risk_bucket UNION SELECT 'OB: 1-4' risk_bucket UNION SELECT 'OB: 5-7' risk_bucket UNION SELECT 'OB: 8-10' risk_bucket UNION SELECT 'OB: 11-12' risk_bucket UNION SELECT 'OB: 13+' risk_bucket
    )

    , termunits_table AS (
    SELECT 'Direct'  termunits UNION SELECT 'Intuit' termunits UNION SELECT 'Marketplaces' termunits UNION SELECT 'Platforms' termunits UNION SELECT 'Other Partners' termunits UNION SELECT 'Terminated Brokers and Partners' termunits
    )

    -- , partner_table AS (
    -- SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Sofi' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'BusinessLoans' partner UNION SELECT 'AtoB' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner UNION SELECT 'National Funding Super' partner UNION SELECT 'Bluevine' partner UNION SELECT 'ZenBusiness' partner UNION SELECT 'Anansii' partner UNION SELECT 'Guesty' partner
    -- )


    SELECT a.week_end_date
    , ct.channel
    , tt.tier
    , spt.sub_product
    , rbt.risk_bucket bucket_group
    , tut.termunits


    FROM a

    CROSS JOIN channel_table ct
    CROSS JOIN tier_table tt
    CROSS JOIN sub_product_table spt
    CROSS JOIN risk_bucket_table rbt
    CROSS JOIN termunits_table tut


    UNION

    SELECT b.week_end_date
    , ct.channel
    , tt.tier
    , spt.sub_product
    , rbt.risk_bucket bucket_group
    , tut.termunits


    FROM b

    CROSS JOIN channel_table ct
    CROSS JOIN tier_table tt
    CROSS JOIN sub_product_table spt
    CROSS JOIN risk_bucket_table rbt
    CROSS JOIN termunits_table tut


    ORDER BY 1 DESC,2,3,4
    ;


	CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_AUW_td AS (
		SELECT *, 1 AS ONE
		FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1_AUW_td);


-----------------------------------------------------------------------------AGG TABLE----------------------------------------------------------------------
	CREATE OR REPLACE TABLE analytics.credit.AUW_AGG AS (
		WITH AUW AS
		(
		SELECT FBBID
			, week_end_date
			, channel
			, termunits
			, industry_type
			, customer_annual_revenue_group
			, partner
			, tier
			, sub_product
			, ob_bucket_group 
			, registration_medium 
			, nav_flow
			, lendio_flow
			, intuit_flow
			, national_funding_flow
			, tenure_status
			
			, is_approved 
			, is_test
			, calc_revenue
			, has_calc_revenue
			, fico_onboarding
			, first_approved_credit_limit
			, case when is_charged_off_fmd=0 or is_charged_off_fmd is null then 0 else is_charged_off_fmd end as is_chargeoff_fmd
			, CASE WHEN account_status='active' and is_chargeoff_fmd = 0 and dpd_days_corrected < 98 THEN 1 ELSE 0 END AS open_accounts
			, CASE WHEN (DPD_DAYS_CORRECTED < 98) AND is_chargeoff_fmd=0 AND OUTSTANDING_PRINCIPAL_DUE > 0 THEN 1 ELSE 0 END active_accounts
			, credit_limit

			, augmented_uw_start_week_end

			, CASE 
					WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
					WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_approved_time::date, current_date()) <= 0 THEN NULL 
					WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
					ELSE DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
				END app_week_end_date

			, CASE 
					WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_rejected_time::date+4)::date+2
					WHEN datediff('day', first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_rejected_time::date, current_date()) <= 0 THEN NULL 
					WHEN datediff('day', first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
					ELSE DATE_TRUNC('WEEK', first_rejected_time::date+4)::date+2
				END rej_week_end_date

			, auw_pre_doc_review_start_time_weekend
			, auw_post_doc_review_start_time_weekend
			
			, auw_pre_doc_review_complete_time_weekend
			, auw_post_doc_review_complete_time_weekend

			, auw_cl_pre
			, auw_cl_post

			, auw_flow_type__c
			,FINAL_AUW_PRE_DOC_APPROVED_LIMIT
					
			, coalesce(FINAL_AUW_PRE_DOC_APPROVED_LIMIT, first_approved_credit_limit) AS PRE_DOC_CL_COALESCE
			, coalesce(AUW_POST_DOC_APPROVED_LIMIT__C, FINAL_AUW_PRE_DOC_APPROVED_LIMIT) AS POST_DOC_CL_COALESCE
			, coalesce(POST_DOC_CL_COALESCE, PRE_DOC_CL_COALESCE) AS TOTAL_EXP_AUW 

			, (FINAL_AUW_PRE_DOC_APPROVED_LIMIT - first_approved_Credit_limit) AS PRE_DOC_INC
			, (AUW_POST_DOC_APPROVED_LIMIT__C - PRE_DOC_CL_COALESCE) AS POST_DOC_INC

			, CASE WHEN AUW_PRE_DOC_REVIEW_STATUS__C ilike '%Complete - Increase%' THEN 1
					ELSE 0 END AS AUW_PRE_DOC_REVIEW_STATUS_FLAG
			, CASE WHEN AUW_POST_DOC_REVIEW_STATUS__C ilike '%Complete - Increase%' THEN 1
					ELSE 0 END AS AUW_POST_DOC_REVIEW_STATUS_FLAG
			, CASE WHEN (AUW_PRE_DOC_REVIEW_STATUS_FLAG=1 OR AUW_POST_DOC_REVIEW_STATUS_FLAG=1) THEN 1 END ob_auw_flag


			, (AUW_PRE_DOC_REVIEW_STATUS_FLAG*PRE_DOC_INC) AS pre_doc_inc1
			, (AUW_POST_DOC_REVIEW_STATUS_FLAG*POST_DOC_INC) AS post_doc_inc1 

			, CASE WHEN FEE_RATE_52 is not null THEN 1 ELSE 0 END AS AUW_52_WEEK_FLAG
			, CASE WHEN FUNDBOX_PLUS_STATUS = 'SUBSCRIBED' THEN 1 ELSE 0 END AS AUW_FBX_PLUS_FLAG
			, CASE WHEN FUNDBOX_PLUS_STATUS = 'SUBSCRIBED' THEN 0 ELSE 1 END AS AUW_FBX_OTHER_FLAG

			, CASE WHEN DATEDIFF(HOUR, first_approved_time, current_timestamp())/24 >=7 AND FIRST_DRAW_TIME IS NOT NULL 
					AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 <=7 AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 >=0 
					AND AUW_PRE_DOC_REVIEW_STATUS_FLAG = 1 then FBBID ELSE null
					END AS ftd_7_pre
			, CASE WHEN DATEDIFF(HOUR, first_approved_time, current_timestamp())/24 >=7 AND FIRST_DRAW_TIME IS NOT NULL 
					AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 <=7 AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 >=0 
					AND AUW_POST_DOC_REVIEW_STATUS_FLAG = 1 then FBBID ELSE null
					END AS ftd_7_post
			, CASE WHEN DATEDIFF(HOUR, first_approved_time, current_timestamp())/24 >=28 AND FIRST_DRAW_TIME IS NOT NULL 
					AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 <=28 AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 >=0 
					AND AUW_PRE_DOC_REVIEW_STATUS_FLAG = 1 then FBBID ELSE null
					END AS ftd_28_pre
			, CASE WHEN DATEDIFF(HOUR, first_approved_time, current_timestamp())/24 >=28 AND FIRST_DRAW_TIME IS NOT NULL 
					AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 <=28 AND DATEDIFF(HOUR, first_approved_time, FIRST_DRAW_TIME)/24 >=0 
					AND AUW_POST_DOC_REVIEW_STATUS_FLAG = 1 then FBBID ELSE null
					END AS ftd_28_post

			, CASE WHEN ftd_7_pre IS NOT NULL THEN (FIRST_DRAW_AMOUNT/AUW_PRE_DOC_APPROVED_LIMIT__C) ELSE NULL
					END AS FTD_UTIL_PRE_DOC_7DAY
			, CASE WHEN ftd_28_pre IS NOT NULL then (FIRST_DRAW_AMOUNT/AUW_PRE_DOC_APPROVED_LIMIT__C) ELSE NULL
					END AS FTD_UTIL_PRE_DOC_28DAY
			, CASE WHEN ftd_7_post IS NOT NULL then (FIRST_DRAW_AMOUNT/AUW_POST_DOC_APPROVED_LIMIT__C) ELSE NULL
					END AS FTD_UTIL_POST_DOC_7DAY
			, CASE WHEN ftd_28_post IS NOT NULL then (FIRST_DRAW_AMOUNT/AUW_POST_DOC_APPROVED_LIMIT__C) ELSE NULL
					END AS FTD_UTIL_POST_DOC_28DAY
			
			-------------------------- SL Metrics --------------------------
			, manual_review_start_week_end_date
			, review_complete_week_end_date
			, SL_review_complete_FLAG
			, SL_TOTAL_EXPOSURE_SL
			, SL_INC_FLAG
			, SL_DEC_FLAG
			, SL_NO_CHANGE_FLAG
			, SL_INC
			, SL_DEC
			, CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', SLA_REJECTED_TIME::date+4)::date+2
					WHEN datediff('day', SLA_REJECTED_TIME::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', SLA_REJECTED_TIME::date, current_date()) <= 0 THEN NULL 
					WHEN datediff('day', SLA_REJECTED_TIME::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
					ELSE DATE_TRUNC('WEEK', SLA_REJECTED_TIME::date+4)::date+2
				END SL_rejected_week_end
			, sl_automated_cl
			, SLA_first_approved_credit_limit
			, CASE WHEN SLA_CL_DELTA>0 THEN 1 END SL_INCREASE
			, CASE WHEN SLA_CL_DELTA<0 THEN 1 END SL_DECREASE 
			, CASE WHEN SLA_CL_DELTA=0 THEN 1 ELSE 0 END SL_NO_CHANGE
			, SLA_CL_DELTA
			, first_suggested_credit_limit_sl     

			-------------------------- OG AUW Metrics --------------------------    
			, og_auw_week_end
			, is_og_auw
			, og_auw_limit
			, og_auw_increase
			, CASE WHEN is_og_auw is not null THEN 1 END og_auw_flag
		
		FROM ANALYTICS.credit.customer_level_data_td
			WHERE true
			AND is_test=0
			AND SUB_PRODUCT <> 'Credit Builder'
		ORDER BY FBBID
		)

		SELECT A.*
			
		------- Decision Metrics
			, approvals
			, high_rev_fico_approvals
			, num_auw_started_v
			, num_auw_pre_started_v
			, num_auw_post_started_v
			, total_ob_exp_granted_v 
			, total_ob_exp_pre_granted_v
			, total_ob_exp_post_granted_v
			, COALESCE(pre_doc_inc_v,0) + COALESCE(post_doc_inc_v,0) as V_EXP_INC_AUW 
			, pre_line_increase_v
			, post_line_increase_v
			, COALESCE(pre_line_increase_v,0) + COALESCE(post_line_increase_v,0) as num_line_increase_v
			, COALESCE(pre_doc_inc_v,0) + COALESCE(post_doc_inc_v,0) as auw_inc_v
			, pre_doc_inc_v 
			, post_doc_inc_v
			, pre_doc_review_complete_v
			, post_doc_review_complete_v
			, num_auw_52w_v
			, num_auw_fbxplus_v
			, num_auw_other_v
			
			, og_auw_this_week_v
			, og_auw_inc_v
			, og_auw_exp_v

			, sl_review_initiated 
			, sl_review_completed_v
			, sl_inc_num_v
			, sl_dec_num_v
			, sl_no_change_num_v 
			, sl_rejections_num_v
			, sl_exp_increase_v 
			, -sl_exp_dec_v as sl_exp_decrease_v 
			, sl_exp_no_change_v 
			, COALESCE(sl_exp_increase_v,0) + COALESCE(sl_exp_decrease_v,0) as sl_net_exp_v
			, sl_lost_exp_v 

		----- Decision Tracker OB AUW [Horizontal]     
		
			, num_auw_started_h
			, num_auw_pre_started_h
			, num_auw_post_started_h
			, total_ob_exp_granted_h
			, total_ob_exp_52w
			, total_ob_exp_fbxplus
			, total_ob_exp_other
			, COALESCE(pre_doc_inc_h,0) + COALESCE(post_doc_inc_h,0) as H_EXP_INC_AUW
			, pre_doc_inc_h
			, post_doc_inc_h
			, COALESCE(num_pre_doc_inc_h,0) + COALESCE(num_post_doc_inc_h,0) as num_inc_h
			, num_pre_doc_inc_h
			, num_post_doc_inc_h
			, num_auw_52w_h
			, num_auw_fbxplus_h
			, num_auw_other_h
			, ftd_7_pre_h
			, ftd_28_pre_h
			, ftd_7_post_h
			, ftd_28_post_h
			, ftd_pre_7day_util_h
			, ftd_post_7day_util_h
			, ftd_pre_28day_util_h
			, ftd_post_28day_util_h

		----- Decision Tracker OG AUW [Horizontal]
			, og_auw_this_week_h
			, og_auw_inc_h
			, og_auw_exp_h

		--- Decision Tracker Second Look [Horizontal]
			, sl_review_initiated_h
			, sl_review_completed_h
			, sl_inc_num_h
			, sl_dec_num_h
			, sl_no_change_num_h
			, sl_exp_increase_h
			, sl_exp_no_change_h
			, -sl_exp_dec_h as sl_exp_decrease_h
			, sl_rejections_num_h
			, sl_exp_increase_h+sl_exp_decrease_h as sl_net_exp_h
			, sl_lost_exp_h
			, sl_total_exposure_sl
			, sl_automated_exposure

		--- Performance Metrics: Customer Level [Vertical]
			, og_auw_active
			, og_auw_exposure
			, ob_auw_active
			, sl_inc_active
			, sl_dec_active
			, sl_dec_exposure
			, sl_inc_exposure
			, sl_no_change_active
			, sl_no_change_exposure
			, ob_auw_exposure
			, og_auw_active_below60
			, og_auw_exposure_below60
			, ob_auw_active_below60
			, ob_auw_exposure_below60
			, sl_inc_active_below60
			, sl_inc_exposure_below60
			, sl_dec_active_below60
			, sl_dec_exposure_below60
			, og_auw_active_above60
			, og_auw_exposure_above60
			, ob_auw_active_above60
			, ob_auw_exposure_above60
			, sl_inc_active_above60
			, sl_inc_exposure_above60
			, sl_dec_active_above60
			, sl_dec_exposure_above60
			, sl_no_change_active_below60
			, sl_no_change_exposure_below60
			, sl_no_change_active_above60
			, sl_no_change_exposure_above60
			, sl_incremental
			, sl_decremental
			
		FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_AUW_td A
		---------------------------------------------HEADLINE METRICS [VERTICAL]---------------------------------------------

		LEFT JOIN -- High Revenue & High FICO Approvals & Approvals [Vertical]
			(SELECT app_week_end_date
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				, COUNT(DISTINCT CASE WHEN calc_revenue>300000 AND fico_onboarding>=650 THEN FBBID END) high_rev_fico_approvals
				, COUNT(DISTINCT FBBID) approvals
				FROM auw
				WHERE is_approved=1 
				and sub_product<>'Credit Builder'--- I have excluded credit builder from high revenue and high fico approvals, which is not in line with the current logic
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) RFA
				ON A.week_end_date = RFA.app_week_end_date
				AND A.channel = RFA.channel
				AND A.tier = RFA.tier
				AND A.sub_product = RFA.sub_product
				AND A.bucket_group = RFA.ob_bucket_group
				AND A.termunits = RFA.termunits

		------- OB AUW  
		LEFT JOIN -- AUW Started
			(SELECT augmented_uw_start_week_end
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				, COUNT(DISTINCT FBBID) num_auw_started_v
				, SUM(FIRST_APPROVED_CREDIT_LIMIT) AS total_ob_exp_granted_v
				FROM auw
				WHERE is_approved=1 
				AND augmented_uw_start_week_end=week_end_date 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW1
				ON A.week_end_date = AUW1.augmented_uw_start_week_end 
				AND A.channel = AUW1.channel
				AND A.tier = AUW1.tier
				AND A.sub_product = AUW1.sub_product
				AND A.bucket_group = AUW1.ob_bucket_group
				AND A.termunits = AUW1.termunits

		LEFT JOIN -- AUW [Pre-Doc] Started
			(SELECT auw_pre_doc_review_start_time_weekend
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				, COUNT(DISTINCT FBBID) num_auw_pre_started_v
				, SUM(FIRST_APPROVED_CREDIT_LIMIT) total_ob_exp_pre_granted_v
				FROM auw
				where auw_pre_doc_review_start_time_weekend=week_end_date
				and is_approved=1 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW1_pre
				ON A.week_end_date = AUW1_pre.auw_pre_doc_review_start_time_weekend 
				AND A.channel = AUW1_pre.channel
				AND A.tier = AUW1_pre.tier
				AND A.sub_product = AUW1_pre.sub_product
				AND A.bucket_group = AUW1_pre.ob_bucket_group
				AND A.termunits = AUW1_pre.termunits

		LEFT JOIN -- AUW [Post-Doc] Started
			(SELECT auw_post_doc_review_start_time_weekend
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				, COUNT(DISTINCT FBBID) num_auw_post_started_v
				, SUM(CASE WHEN auw_post_doc_review_start_time_weekend=week_end_date THEN FIRST_APPROVED_CREDIT_LIMIT END) total_ob_exp_post_granted_v
				
				FROM auw
				WHERE is_approved=1 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW1_post
				ON A.week_end_date = AUW1_post.auw_post_doc_review_start_time_weekend 
				AND A.channel = AUW1_pre.channel
				AND A.tier = AUW1_pre.tier
				AND A.sub_product = AUW1_pre.sub_product
				AND A.bucket_group = AUW1_pre.ob_bucket_group
				AND A.termunits=AUW1_pre.termunits

		LEFT JOIN -- AUW [Pre-Doc] Completed
			(SELECT auw_pre_doc_review_complete_time_weekend
						, channel
						, tier
						, sub_product
						, ob_bucket_group
						, termunits
						, sum(AUW_PRE_DOC_REVIEW_STATUS_FLAG*PRE_DOC_INC) AS pre_doc_inc_v
						, sum(AUW_PRE_DOC_REVIEW_STATUS_FLAG) pre_line_increase_v
						, count(distinct fbbid) pre_doc_review_complete_v
						, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG) num_auw_52w_v
						, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG * AUW_FBX_PLUS_FLAG) AS num_auw_fbxplus_v
						, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG * AUW_FBX_OTHER_FLAG) AS num_auw_other_v
				FROM auw
				WHERE auw_pre_doc_review_complete_time_weekend=week_end_date 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW_pre_comp
				ON A.week_end_date = AUW_pre_comp.auw_pre_doc_review_complete_time_weekend 
				AND A.channel = AUW_pre_comp.channel
				AND A.tier = AUW_pre_comp.tier
				AND A.sub_product = AUW_pre_comp.sub_product
				AND A.bucket_group = AUW_pre_comp.ob_bucket_group
				AND A.termunits=AUW_pre_comp.termunits

		LEFT JOIN -- AUW [Post-Doc] Completed
			(SELECT auw_post_doc_review_complete_time_weekend
						, channel
						, tier
						, sub_product
						, ob_bucket_group
						, termunits
						, SUM(AUW_POST_DOC_REVIEW_STATUS_FLAG*POST_DOC_INC) AS post_doc_inc_v
						, SUM(AUW_POST_DOC_REVIEW_STATUS_FLAG) post_line_increase_v
						, COUNT(distinct fbbid) post_doc_review_complete_v
				FROM auw
				WHERE auw_post_doc_review_complete_time_weekend=week_end_date
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW_post_comp
				ON A.week_end_date = AUW_post_comp.auw_post_doc_review_complete_time_weekend 
				AND A.channel = AUW_post_comp.channel
				AND A.tier = AUW_post_comp.tier
				AND A.sub_product = AUW_post_comp.sub_product
				AND A.bucket_group = AUW_post_comp.ob_bucket_group
				AND A.termunits=AUW_post_comp.termunits

		------- OG AUW 
		LEFT JOIN -- All headline metrics
			(SELECT og_auw_week_end
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits 
				, COUNT(distinct fbbid) og_auw_this_week_v
				, SUM(og_auw_increase) og_auw_inc_v
				, SUM(og_auw_limit) og_auw_exp_v
				FROM auw
				WHERE og_auw_week_end=week_end_date
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) OG_V
				ON A.week_end_date = OG_V.og_auw_week_end 
				AND A.channel = OG_V.channel
				AND A.tier = OG_V.tier
				AND A.sub_product = OG_V.sub_product
				AND A.bucket_group = OG_V.ob_bucket_group
				AND A.termunits=OG_V.termunits

		------- SL 
		LEFT JOIN -- SL decisions
			(SELECT review_complete_week_end_date
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits 
				, COUNT(DISTINCT FBBID) sl_review_completed_v
				, COUNT(DISTINCT CASE WHEN SLA_CL_DELTA>0 THEN FBBID END) sl_inc_num_v
				, COUNT(DISTINCT CASE WHEN SLA_CL_DELTA<0 THEN FBBID END) sl_dec_num_v
				, COUNT(DISTINCT CASE WHEN SLA_CL_DELTA=0 THEN FBBID END) sl_no_change_num_v
				, COUNT(DISTINCT CASE WHEN SLA_first_approved_credit_limit=0 THEN FBBID END) sl_rejections_num_v

				, SUM(CASE WHEN review_complete_week_end_date=week_end_date THEN SL_INC_FLAG*SL_INC END) sl_exp_increase_v
				, SUM(CASE WHEN review_complete_week_end_date=week_end_date THEN SL_DEC_FLAG*SL_DEC END) sl_exp_dec_v
				, SUM(CASE WHEN review_complete_week_end_date=week_end_date THEN SL_NO_CHANGE_FLAG*SL_DEC END) sl_exp_no_change_v
				, SUM(CASE WHEN SLA_first_approved_credit_limit = 0 THEN SL_AUTOMATED_CL ELSE NULL END) sl_lost_exp_v

				FROM auw
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) SL_v1
				ON A.week_end_date = SL_v1.review_complete_week_end_date 
				AND A.channel = SL_v1.channel
				AND A.tier = SL_v1.tier
				AND A.sub_product = SL_v1.sub_product
				AND A.bucket_group = SL_v1.ob_bucket_group
				AND A.termunits=SL_v1.termunits

		LEFT JOIN -- SL review initiated
			(SELECT manual_review_start_week_end_date
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits 
				, COUNT(distinct fbbid) sl_review_initiated
				FROM auw
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) SL_v2
				ON A.week_end_date = SL_v2.manual_review_start_week_end_date 
				AND A.channel = SL_v2.channel
				AND A.tier = SL_v2.tier
				AND A.sub_product = SL_v2.sub_product
				AND A.bucket_group = SL_v2.ob_bucket_group
				AND A.termunits=SL_v2.termunits

		--------------------------------------------- DECISION TRACKER OB AUW [HORIZONTAL] ---------------------------------------------
		LEFT JOIN 
			(SELECT augmented_uw_start_week_end
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				
				, COUNT(DISTINCT FBBID) num_auw_started_h 
				, COUNT(DISTINCT CASE WHEN auw_pre_doc_review_start_time_weekend IS NOT NULL THEN fbbid END) num_auw_pre_started_h
				, COUNT(DISTINCT CASE WHEN auw_post_doc_review_start_time_weekend IS NOT NULL THEN fbbid END) num_auw_post_started_h

				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG) num_auw_52w_h
				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG * AUW_FBX_PLUS_FLAG) AS num_auw_fbxplus_h
				, sum(AUW_PRE_DOC_REVIEW_STATUS_FLAG * AUW_52_WEEK_FLAG * AUW_FBX_OTHER_FLAG) AS num_auw_other_h

				, SUM(FIRST_APPROVED_CREDIT_LIMIT) AS total_ob_exp_granted_h

				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*PRE_DOC_INC) AS pre_doc_inc_h
				, SUM(AUW_POST_DOC_REVIEW_STATUS_FLAG*POST_DOC_INC) AS post_doc_inc_h
				, 
				SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG) AS num_pre_doc_inc_h
				, SUM(AUW_POST_DOC_REVIEW_STATUS_FLAG) AS num_post_doc_inc_h
				
				, COUNT(DISTINCT ftd_7_pre) ftd_7_pre_h
				, COUNT(DISTINCT ftd_28_pre) ftd_28_pre_h
				, COUNT(DISTINCT ftd_7_post) ftd_7_post_h
				, COUNT(DISTINCT ftd_28_post) ftd_28_post_h
				, AVG(FTD_UTIL_PRE_DOC_7DAY) ftd_pre_7day_util_h
				, AVG(FTD_UTIL_POST_DOC_7DAY) ftd_post_7day_util_h
				, AVG(FTD_UTIL_PRE_DOC_28DAY) ftd_pre_28day_util_h
				, AVG(FTD_UTIL_POST_DOC_28DAY) ftd_post_28day_util_h

				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*PRE_DOC_INC) total_ob_exp_52w
				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*PRE_DOC_INC * AUW_FBX_PLUS_FLAG) total_ob_exp_fbxplus
				, SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*PRE_DOC_INC * AUW_FBX_OTHER_FLAG) total_ob_exp_other

				-- , SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*FIRST_APPROVED_CREDIT_LIMIT) total_ob_exp_52w
				-- , SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*FIRST_APPROVED_CREDIT_LIMIT * AUW_FBX_PLUS_FLAG) total_ob_exp_fbxplus
				-- , SUM(AUW_PRE_DOC_REVIEW_STATUS_FLAG*AUW_52_WEEK_FLAG*FIRST_APPROVED_CREDIT_LIMIT * AUW_FBX_OTHER_FLAG) total_ob_exp_other
				
				FROM auw
				WHERE augmented_uw_start_week_end=week_end_date 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) AUW_H
				ON A.week_end_date = AUW_H.augmented_uw_start_week_end 
				AND A.channel = AUW_H.channel
				AND A.tier = AUW_H.tier
				AND A.sub_product = AUW_H.sub_product
				AND A.bucket_group = AUW_H.ob_bucket_group
				AND A.termunits=AUW_H.termunits

		--------------------------------------------- DECISION TRACKER OG AUW [HORIZONTAL] ---------------------------------------------
		LEFT JOIN 
			(SELECT og_auw_week_end
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits
				, COUNT(DISTINCT fbbid) og_auw_this_week_h
				, SUM(og_auw_increase) og_auw_inc_h
				, SUM(og_auw_limit) og_auw_exp_h
			FROM auw
			WHERE og_auw_week_end=week_end_date
				GROUP BY 1,2,3,4,5,6
				ORDER BY 1,2,3,5,5,6) OG_H
			ON A.week_end_date = OG_H.og_auw_week_end 
			AND A.channel = OG_H.channel
			AND A.tier = OG_H.tier
			AND A.sub_product = OG_H.sub_product
			AND A.bucket_group = OG_H.ob_bucket_group
			AND A.termunits=OG_H.termunits
			
		--------------------------------------------- DECISION TRACKER SL [HORIZONTAL] ---------------------------------------------
		LEFT JOIN 
		(SELECT manual_review_start_week_end_date
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits 
				, COUNT(DISTINCT fbbid) sl_review_initiated_h
				, SUM(sl_review_complete_flag)  sl_review_completed_h
				, COUNT(DISTINCT CASE WHEN sla_cl_delta>0 THEN FBBID END) sl_inc_num_h
				, COUNT(DISTINCT CASE WHEN sla_cl_delta<0 THEN FBBID END) sl_dec_num_h
				, COUNT(DISTINCT CASE WHEN sla_cl_delta=0 THEN FBBID END) sl_no_change_num_h
				, SUM(SL_INC_FLAG*SL_INC) sl_exp_increase_h
				, SUM(SL_DEC_FLAG*SL_DEC) sl_exp_dec_h
				, SUM(SL_NO_CHANGE_FLAG*SL_DEC) sl_exp_no_change_h
				, COUNT(DISTINCT CASE WHEN SLA_first_approved_credit_limit=0 THEN FBBID END) sl_rejections_num_h 
				, SUM(CASE WHEN SLA_first_approved_credit_limit=0 THEN SL_AUTOMATED_CL ELSE NULL END) sl_lost_exp_h
				, SUM(SL_TOTAL_EXPOSURE_SL) sl_total_exposure_sl
				, SUM(first_suggested_credit_limit_sl) sl_automated_exposure
				FROM
				(SELECT DISTINCT fbbid
					, channel, tier, sub_product, ob_bucket_group, termunits
					, sl_review_complete_flag, sl_rejected_week_end
					, SL_INC_FLAG, SL_DEC_FLAG, SL_NO_CHANGE_FLAG, sla_cl_delta
					, manual_review_start_week_end_date
					, sl_inc, sl_dec, first_approved_credit_limit, sl_automated_cl
					, SLA_first_approved_credit_limit, sl_total_exposure_sl, first_suggested_credit_limit_sl
					FROM auw) 
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) SL_h
				ON A.week_end_date = SL_h.manual_review_start_week_end_date 
				AND A.channel = SL_h.channel
				AND A.tier = SL_h.tier
				AND A.sub_product = SL_h.sub_product
				AND A.bucket_group = SL_h.ob_bucket_group
				AND A.termunits=SL_h.termunits

		--------------------------------------------- PERFORMANCE METRICS ---------------------------------------------
		LEFT JOIN 
			(SELECT week_end_date
				, channel
				, tier
				, sub_product
				, ob_bucket_group
				, termunits 

				--- tenure below 60 
				, COUNT(DISTINCT CASE WHEN og_auw_flag=1 AND active_accounts=1 AND tenure_status='Tenure <60' THEN fbbid END) og_auw_active_below60
				, SUM(CASE WHEN og_auw_flag=1 AND open_accounts=1 AND tenure_status='Tenure <60' THEN credit_limit END) og_auw_exposure_below60
				
				, COUNT(DISTINCT CASE WHEN ob_auw_flag=1 AND active_accounts=1 AND tenure_status='Tenure <60' THEN fbbid END) ob_auw_active_below60
				, SUM(CASE WHEN ob_auw_flag=1 AND open_accounts=1 AND tenure_status='Tenure <60' THEN credit_limit END) ob_auw_exposure_below60
				
				, COUNT(DISTINCT CASE WHEN SL_INCREASE=1 AND active_accounts=1  AND tenure_status='Tenure <60' THEN fbbid END) sl_inc_active_below60
				, SUM(CASE WHEN SL_INCREASE=1 AND open_accounts=1  AND tenure_status='Tenure <60' THEN credit_limit END) sl_inc_exposure_below60
				
				, COUNT(DISTINCT CASE WHEN SL_DECREASE=1 AND active_accounts=1  AND tenure_status='Tenure <60' THEN fbbid END) sl_dec_active_below60
				, SUM(CASE WHEN SL_DECREASE=1 AND open_accounts=1 AND tenure_status='Tenure <60' THEN credit_limit END) sl_dec_exposure_below60

				, COUNT(DISTINCT CASE WHEN SL_NO_CHANGE=1 AND active_accounts=1  AND tenure_status='Tenure <60' THEN fbbid END) sl_no_change_active_below60 
				, SUM(CASE WHEN SL_NO_CHANGE=1 AND open_accounts=1 AND tenure_status='Tenure <60' THEN credit_limit END) sl_no_change_exposure_below60 

				--- tenure above 60 
				, COUNT(DISTINCT CASE WHEN og_auw_flag=1 AND active_accounts=1 AND tenure_status='Tenure >=60' THEN fbbid END) og_auw_active_above60
				, SUM(CASE WHEN og_auw_flag=1 AND open_accounts=1 AND tenure_status='Tenure >=60' THEN credit_limit END) og_auw_exposure_above60
				
				, COUNT(DISTINCT CASE WHEN ob_auw_flag=1 AND active_accounts=1 AND tenure_status='Tenure >=60' THEN fbbid END) ob_auw_active_above60
				, SUM(CASE WHEN ob_auw_flag=1 AND open_accounts=1 AND tenure_status='Tenure >=60' THEN credit_limit END) ob_auw_exposure_above60
				
				, COUNT(DISTINCT CASE WHEN SL_INCREASE=1 AND active_accounts=1  AND tenure_status='Tenure >=60' THEN fbbid END) sl_inc_active_above60
				, SUM(CASE WHEN SL_INCREASE=1 AND open_accounts=1  AND tenure_status='Tenure >=60' THEN credit_limit END) sl_inc_exposure_above60
				
				, COUNT(DISTINCT CASE WHEN SL_DECREASE=1 AND active_accounts=1  AND tenure_status='Tenure >=60' THEN fbbid END) sl_dec_active_above60
				, SUM(CASE WHEN SL_DECREASE=1 AND open_accounts=1 AND tenure_status='Tenure >=60' THEN credit_limit END) sl_dec_exposure_above60

				, COUNT(DISTINCT CASE WHEN SL_NO_CHANGE=1 AND active_accounts=1  AND tenure_status='Tenure >=60' THEN fbbid END) sl_no_change_active_above60
				, SUM(CASE WHEN SL_NO_CHANGE=1 AND open_accounts=1 AND tenure_status='Tenure >=60' THEN credit_limit END) sl_no_change_exposure_above60
				
				--- overall
				, COUNT(DISTINCT CASE WHEN og_auw_flag=1 AND active_accounts=1 THEN fbbid END) og_auw_active
				, SUM(CASE WHEN og_auw_flag=1 AND open_accounts=1 THEN credit_limit END) og_auw_exposure
				
				, COUNT(DISTINCT CASE WHEN ob_auw_flag=1 AND active_accounts=1 THEN fbbid END) ob_auw_active
				, SUM(CASE WHEN ob_auw_flag=1 AND open_accounts=1 THEN credit_limit END) ob_auw_exposure
				
				, COUNT(DISTINCT CASE WHEN SL_INCREASE=1 AND active_accounts=1 THEN fbbid END) sl_inc_active
				, SUM(CASE WHEN SL_INCREASE=1 AND open_accounts=1 THEN credit_limit END) sl_inc_exposure
				
				, COUNT(DISTINCT CASE WHEN SL_DECREASE=1 AND active_accounts=1 THEN fbbid END) sl_dec_active
				, SUM(CASE WHEN SL_DECREASE=1 AND open_accounts=1 THEN credit_limit END) sl_dec_exposure

				, COUNT(DISTINCT CASE WHEN SL_NO_CHANGE=1 AND active_accounts=1 THEN fbbid END) sl_no_change_active
				, SUM(CASE WHEN SL_NO_CHANGE=1 AND open_accounts=1 THEN credit_limit END) sl_no_change_exposure

				--- SL incremental and decremental exposure
				, SUM(CASE WHEN SL_INCREASE=1 THEN SLA_CL_DELTA END) sl_incremental
				, SUM(CASE WHEN SL_DECREASE=1 THEN SLA_CL_DELTA END) sl_decremental
		
				FROM auw
					GROUP BY 1,2,3,4,5,6
					ORDER BY 1,2,3,5,5,6) PERF
			ON A.week_end_date = PERF.week_end_date 
			AND A.channel = PERF.channel
			AND A.tier = PERF.tier
			AND A.sub_product = PERF.sub_product
			AND A.bucket_group = PERF.ob_bucket_group
			AND A.termunits=PERF.termunits
			)
			;


-----------------------------------------------------------------------------LOAN LEVEL METRICS---------------------------------------------------------------------

CREATE OR REPLACE TABLE ANALYTICS.CREDIT.auw_performance_metrics_agg AS (
SELECT L.WEEK_END_DATE
     , new_cust_filter
     
     --- Total Originations
     , SUM(CASE WHEN og_auw_flag=1 and loan_created_date between week_start_date and L.week_end_date then originated_amount ELSE 0 END) og_auw_orig 
     , SUM(CASE WHEN sl_inc_flag=1 and loan_created_date between week_start_date and L.week_end_date THEN originated_amount ELSE 0 END) sl_inc_orig 
     , SUM(CASE WHEN sl_dec_flag=1 and loan_created_date between week_start_date and L.week_end_date THEN originated_amount ELSE 0 END) sl_dec_orig 
     , SUM(CASE WHEN sl_no_change_flag=1 and loan_created_date between week_start_date and L.week_end_date THEN originated_amount ELSE 0 END) sl_no_change_orig 
     , SUM(CASE WHEN ob_auw_flag=1 and loan_created_date between week_start_date and L.week_end_date THEN originated_amount ELSE 0 END) ob_auw_orig 
     
     --- Total Outstanding
     , SUM(CASE WHEN og_auw_flag=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) og_auw_os
     , SUM(CASE WHEN sl_inc_flag=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) sl_inc_os 
     , SUM(CASE WHEN sl_dec_flag=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) sl_dec_os
     , SUM(CASE WHEN sl_no_change_flag=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) sl_no_change_os
     , SUM(CASE WHEN ob_auw_flag=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) ob_auw_os
     
     --- Number of Delinquent Draws
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq
     --- Number of draws in DPD 1-7
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq_1_7
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq_1_7
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq_1_7
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq_1_7
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq_1_7
     --- Number of draws in DPD 8-14
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq_8_14
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq_8_14
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq_8_14
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq_8_14
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq_8_14
     --- Number of draws in DPD 15-35
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq_15_35
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq_15_35
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq_15_35
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq_15_35
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq_15_35
     --- Number of draws in DPD 36-63
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq_36_63
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq_36_63
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq_36_63
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq_36_63
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq_36_63
     --- Number of draws in DPD 64-90
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_num_delq_64_90
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_num_delq_64_90
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_num_delq_64_90
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_num_delq_64_90
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_auw_num_delq_64_90
     
     --- $OS in DPD
     , SUM(CASE WHEN og_auw_flag=1 THEN os_1_90 ELSE 0 END) og_delq
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_1_90 ELSE 0 END) sl_inc_delq
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_1_90 ELSE 0 END) sl_dec_delq
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_1_90 ELSE 0 END) sl_no_change_delq
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_1_90 ELSE 0 END) ob_auw_delq
     --- $OS in DPD 1-7
     , SUM(CASE WHEN og_auw_flag=1 THEN os_1_7 ELSE 0 END) og_delq_1_7
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_1_7 ELSE 0 END) sl_inc_delq_1_7
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_1_7 ELSE 0 END) sl_dec_delq_1_7
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_1_7 ELSE 0 END) sl_no_change_delq_1_7
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_1_7 ELSE 0 END) ob_auw_delq_1_7
     --- $OS in DPD 8-14
     , SUM(CASE WHEN og_auw_flag=1 THEN os_8_14 ELSE 0 END) og_delq_8_14
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_8_14 ELSE 0 END) sl_inc_delq_8_14
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_8_14 ELSE 0 END) sl_dec_delq_8_14
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_8_14 ELSE 0 END) sl_no_change_delq_8_14
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_8_14 ELSE 0 END) ob_auw_delq_8_14
     --- $OS in 15-35
     , SUM(CASE WHEN og_auw_flag=1 THEN os_15_35 ELSE 0 END) og_delq_15_35
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_15_35 ELSE 0 END) sl_inc_delq_15_35
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_15_35 ELSE 0 END) sl_dec_delq_15_35
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_15_35 ELSE 0 END) sl_no_change_delq_15_35
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_15_35 ELSE 0 END) ob_auw_delq_15_35
     --- $OS in 36-63
     , SUM(CASE WHEN og_auw_flag=1 THEN os_36_63 ELSE 0 END) og_delq_36_63
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_36_63 ELSE 0 END) sl_inc_delq_36_63
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_36_63 ELSE 0 END) sl_dec_delq_36_63
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_36_63 ELSE 0 END) sl_no_change_delq_36_63
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_36_63 ELSE 0 END) ob_auw_delq_36_63
     --- $OS in DPD 64-90
     , SUM(CASE WHEN og_auw_flag=1 THEN os_64_90 ELSE 0 END) og_delq_64_90
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_64_90 ELSE 0 END) sl_inc_delq_64_90
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_64_90 ELSE 0 END) sl_dec_delq_64_90
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_64_90 ELSE 0 END) sl_no_change_delq_64_90
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_64_90 ELSE 0 END) ob_auw_delq_64_90

     --- New DPD 1-7 ($)
     , SUM(CASE WHEN os_p_0>0 AND og_auw_flag=1 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new_og
     , SUM(CASE WHEN os_p_0>0 AND sl_inc_flag=1 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new_sl_inc
     , SUM(CASE WHEN os_p_0>0 AND sl_dec_flag=1 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new_sl_dec
     , SUM(CASE WHEN os_p_0>0 AND sl_no_change_flag=1 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new_sl_no_change
     , SUM(CASE WHEN os_p_0>0 AND ob_auw_flag=1 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new_ob
     --- Old DPD 0 ($)
     , SUM(CASE WHEN og_auw_flag=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_og
     , SUM(CASE WHEN sl_inc_flag=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_sl_inc
     , SUM(CASE WHEN sl_dec_flag=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_sl_dec
     , SUM(CASE WHEN sl_no_change_flag=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_sl_no_change
     , SUM(CASE WHEN ob_auw_flag=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_ob

     --- New DPD 1-7 (#)
     , COUNT(DISTINCT CASE WHEN os_p_0>0  AND os_1_7>0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_og
     , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_sl_inc
     , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_sl_dec
     , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_sl_no_change
     , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 AND ob_auw_flag=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_ob
     --- Old DPD 0 (#)
     , COUNT(DISTINCT CASE WHEN og_auw_flag=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_og
     , COUNT(DISTINCT CASE WHEN sl_inc_flag=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_sl_inc
     , COUNT(DISTINCT CASE WHEN sl_dec_flag=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_sl_dec
     , COUNT(DISTINCT CASE WHEN sl_no_change_flag=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_sl_no_change
     , COUNT(DISTINCT CASE WHEN ob_auw_flag=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_ob

     --- $ Revenue
     , SUM(CASE WHEN og_auw_flag=1 THEN fees_paid ELSE 0 END) og_revenue
     , SUM(CASE WHEN sl_inc_flag=1 THEN fees_paid ELSE 0 END) sl_inc_revenue
     , SUM(CASE WHEN sl_dec_flag=1 THEN fees_paid ELSE 0 END) sl_dec_revenue
     , SUM(CASE WHEN sl_no_change_flag=1 THEN fees_paid ELSE 0 END) sl_no_change_revenue
     , SUM(CASE WHEN ob_auw_flag=1 THEN fees_paid ELSE 0 END) ob_revenue

     --- $CO
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and og_auw_flag=1 THEN outstanding_principal_due ELSE 0 END) og_co
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and sl_inc_flag=1 THEN outstanding_principal_due ELSE 0 END) sl_inc_co
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and sl_dec_flag=1 THEN outstanding_principal_due ELSE 0 END) sl_dec_co
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and sl_no_change_flag=1 THEN outstanding_principal_due ELSE 0 END) sl_no_change_co
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and ob_auw_flag=1 THEN outstanding_principal_due ELSE 0 END) ob_co

     --- $Recovery
     , SUM(CASE WHEN is_after_co=1 and og_auw_flag=1 THEN principal_paid ELSE 0 END) og_recovery
     , SUM(CASE WHEN is_after_co=1 and sl_inc_flag=1 THEN principal_paid ELSE 0 END) sl_inc_recovery
     , SUM(CASE WHEN is_after_co=1 and sl_dec_flag=1 THEN principal_paid ELSE 0 END) sl_dec_recovery
     , SUM(CASE WHEN is_after_co=1 and sl_no_change_flag=1 THEN principal_paid ELSE 0 END) sl_no_change_recovery
     , SUM(CASE WHEN is_after_co=1 and ob_auw_flag=1 THEN principal_paid ELSE 0 END) ob_recovery

     --- Draws Cured
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND og_auw_flag=1 THEN loan_key ELSE NULL END) og_cured
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND sl_inc_flag=1 THEN loan_key ELSE NULL END) sl_inc_cured
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND sl_dec_flag=1 THEN loan_key ELSE NULL END) sl_dec_cured
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND sl_no_change_flag=1 THEN loan_key ELSE NULL END) sl_no_change_cured
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 and ob_auw_flag=1 THEN loan_key ELSE NULL END) ob_cured

     --------------- 100K+ Performance
     , SUM(CASE WHEN cl_100k=1 and loan_created_date between week_start_date and L.week_end_date then originated_amount ELSE 0 END) orig_100k
     , SUM(CASE WHEN cl_100k=1 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) os_100k
        -- # Draws Delinquent
     , COUNT(DISTINCT CASE WHEN os_1_90>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_100k
     , COUNT(DISTINCT CASE WHEN os_1_7>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_1_7_100k
     , COUNT(DISTINCT CASE WHEN os_8_14>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_8_14_100k
     , COUNT(DISTINCT CASE WHEN os_15_35>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_15_35_100k
     , COUNT(DISTINCT CASE WHEN os_36_63>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_36_63_100k
     , COUNT(DISTINCT CASE WHEN os_64_90>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_delq_64_90_100k
        -- $OS Delinquent
     , SUM(CASE WHEN cl_100k=1 THEN os_1_90 ELSE 0 END) os_delq_100k
     , SUM(CASE WHEN cl_100k=1 THEN os_1_7 ELSE 0 END) os_delq_1_7_100k
     , SUM(CASE WHEN cl_100k=1 THEN os_8_14 ELSE 0 END) os_delq_8_14_100k
     , SUM(CASE WHEN cl_100k=1 THEN os_15_35 ELSE 0 END) os_delq_15_35_100k
     , SUM(CASE WHEN cl_100k=1 THEN os_36_63 ELSE 0 END) os_delq_36_63_100k
     , SUM(CASE WHEN cl_100k=1 THEN os_64_90 ELSE 0 END) os_delq_64_90_100k
        -- $DPD 1-7 RR
     , SUM(CASE WHEN os_p_0>0 AND cl_100k=1 THEN os_1_7 ELSE 0 END) CU
     , SUM(CASE WHEN cl_100k=1 THEN os_p_0 ELSE 0 END) os_dpd_0_old_100k
        --#DPD 1-7 RR
     , COUNT(DISTINCT CASE WHEN os_p_0>0 AND os_1_7>0 AND cl_100k=1 THEN loan_key ELSE NULL END) num_dpd_1_7_new_100k
     , COUNT(DISTINCT CASE WHEN cl_100k=1 AND os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old_100k

     , SUM(CASE WHEN cl_100k=1 THEN fees_paid ELSE 0 END) revenue_100k -- revenue
     , SUM(CASE WHEN (charge_off_date BETWEEN L.week_start_date AND L.week_end_date) and cl_100k=1 THEN outstanding_principal_due ELSE 0 END) co_100k -- chargeoffs
     , SUM(CASE WHEN is_after_co=1 and cl_100k=1 THEN principal_paid ELSE 0 END) recovery_100k
     , COUNT(DISTINCT CASE WHEN os_p_1_90>0 AND os_1_90=0 AND is_charged_off=0 AND cl_100k=1 THEN loan_key ELSE NULL END) cured_100k
     
     FROM analytics.credit.loan_level_data_pb L

LEFT JOIN 
    (SELECT FBBID
          , week_end_date
          , is_og_auw
          , MIN(is_og_auw) OVER (PARTITION BY fbbid ORDER BY week_end_date) AS min_og_auw_date
          , CASE WHEN is_og_auw is not null THEN 1 ELSE 0 END og_auw_flag
          , CASE WHEN sla_cl_delta>0 THEN 1 ELSE 0 END sl_inc_flag
          , CASE WHEN sla_cl_delta<0 THEN 1 ELSE 0 END sl_dec_flag
          , CASE WHEN sla_cl_delta=0 THEN 1 ELSE 0 END sl_no_change_flag
          , CASE WHEN auw_pre_doc_review_status__c ilike '%Complete - Increase%' THEN 1
            ELSE 0 END AS auw_pre_doc_review_status_flag
          , CASE WHEN auw_post_doc_review_status__c ilike '%Complete - Increase%' THEN 1
            ELSE 0 END AS auw_post_doc_review_status_flag
          , CASE WHEN (AUW_PRE_DOC_REVIEW_STATUS_FLAG=1 OR AUW_POST_DOC_REVIEW_STATUS_FLAG=1) THEN 1  ELSE 0 END ob_auw_flag
          , CASE WHEN ((FINAL_AUW_PRE_DOC_APPROVED_LIMIT >=100000 or AUW_POST_DOC_APPROVED_LIMIT__C >=100000) AND ob_auw_flag=1) OR (sla_first_approved_credit_limit>=100000 AND (sl_dec_flag=1 or sl_inc_flag=1))  
                 THEN 1 ELSE 0 END cl_100k
          , first_approved_credit_limit
          
    FROM analytics.credit.customer_level_data_td) C
    ON L.fbbid=C.fbbid
    AND L.week_end_date=C.week_end_date
    WHERE ((og_auw_flag=1 and min_og_auw_date::date<loan_created_date::date)
        OR sl_inc_flag=1
        OR sl_dec_flag=1
        OR ob_auw_flag=1
        OR sl_no_change_flag=1)
    GROUP BY 1,2
    ORDER BY 1 DESC, 2)
    ;


---------------------------------------------------------AUW MONITORING--------------------------------------

CREATE OR REPLACE TABLE INDUS.PUBLIC.AUW_MONITOR AS ( 
    with datas as (
    select a.fbbid, a.last_modified_time as lmt, a.risk_review_id, a.total_limit as current_limit, a.id as curr_id, a.reason as curr_reason, a.comment, 
           a.PREV_CREDIT_LIMIT_ID as pcli,
           b.id,
           b.total_limit as prev_limit,
           b.LAST_MODIFIED_TIME as prev_lmt,
           current_limit - prev_limit as delta
          
    from cdc_v2.credit.CREDIT_LIMITS a
    left join cdc_v2.credit.credit_limits b 
    on true
    and b.id = a.PREV_CREDIT_LIMIT_ID
    
    where true 
    -- The automated CLD program tag
--    and a.reason ilike '%Automated CLD%'
    and delta < 0
    --and lmt >= '2025-02-01'
)
, cte as (select a.*, 
date_trunc('week', lmt::date+4)::date+2 AS week_end_date,
f.termunits
from datas a
left join INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 F
on a.fbbid = f.fbbid
and a.lmt::date = f.edate

where true

-- Reviews created for manual underwriting team to review. Not all reviews result in an action.
and a.fbbid in (
    SELECT fundbox_id__c as fbbid
    FROM External_Data_Sources.Salesforce_nova.Loan__c
    WHERE recordtypeid ='012Rd000002Dp5CIAS'
)
and comment ilike '%auw Monitoring%')
select week_end_date,termunits,  sum(delta) as credit_change, count(distinct fbbid) as num_accounts
from cte group by 1,2 
);

-- CREATE OR REPLACE TABLE INDUS.PUBLIC.AUW_HVC AS ( 
-- WITH DATAS AS (
-- 	SELECT 
-- 	FUNDBOX_ID__C AS FBBID,
-- 	F.TERMUNITS,
-- 	status__c,
-- 	review_complete_time__c::DATE AS LMT,
-- 	APPROVED_UW_CHANGE_AMOUNT__C AS EXP_INC

-- 	from external_data_sources.salesforce_nova.loan__c A
-- 	left join INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 F
-- 	ON A.FUNDBOX_ID__C = F.FBBID
-- 	AND F.EDATE = F.min_edate
-- where
--     Recordtypeid = '012Rd000002EppVIAS'
--     and status__c is not NULL)

-- 	SELECT  date_trunc('week', lmt::date+4)::date+2 AS week_end_date, 
-- 			TERMUNITS,
-- 			SUM(EXP_INC) AS EXP_INC_HVC,
-- 			COUNT(DISTINCT CASE WHEN status__c IN ILIKE 'complete - increase%' THEN FBBID END) AS NUM_HVC
-- 			FROM DATAS GROUP BY 1,2 );


CREATE OR REPLACE TABLE INDUS.PUBLIC.AUW_HVC AS

WITH OG_AUW_FBBIDS_TO_EXCLUDE AS (
    SELECT DISTINCT
        a.FBBID
    FROM 
        CDC_V2.credit.CREDIT_LIMITS AS a
    WHERE
        a.comment ILIKE '%AUW OG Review%'
),
HVCData AS (
    SELECT 
        FUNDBOX_ID__C AS FBBID,
        review_complete_time__c::DATE AS loan_review_date,
        status__c,
        DATE_TRUNC('week', review_complete_time__c::date + 4)::date + 2 AS week_end_date,
        APPROVED_UW_CHANGE_AMOUNT__C AS EXP_INC,
        ROW_NUMBER() OVER (
            PARTITION BY FUNDBOX_ID__C, DATE_TRUNC('week', review_complete_time__c::date + 4)::date + 2
            ORDER BY review_complete_time__c DESC, createddate DESC
        ) AS WEEK_END_STATUS
    FROM external_data_sources.salesforce_nova.loan__c 
    WHERE 
        Recordtypeid = '012Rd000002EppVIAS'
        AND status__c IS NOT NULL
        AND status__c IN (
            'Complete - Increase',
            'Complete - Temp Increase',
            'Complete - Temp Increase + Terms',
            'Complete - Counteroffer',
            'Complete - Increase + Better Terms'
        )
),
HVC_No_Overlap AS (
    SELECT 
        HVC.FBBID,
        HVC.week_end_date,
        HVC.status__c,
        HVC.EXP_INC
    FROM 
        HVCData HVC
    WHERE
        HVC.week_end_status = 1
        AND NOT EXISTS (
            SELECT 1
            FROM OG_AUW_FBBIDS_TO_EXCLUDE OGE
            WHERE HVC.FBBID = OGE.FBBID
        )
), 
HVC_No_Overlap_termunits as (
select A.*, 
      F.TERMUNITS,
	from HVC_No_Overlap A
	left join INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 F
	ON A.FBBID = F.FBBID
	AND F.EDATE = F.min_edate
)
SELECT
    week_end_date,
	TERMUNITS,
    COUNT(DISTINCT FBBID) AS NUM_HVC,
    SUM(EXP_INC) AS EXP_INC_HVC
FROM
    HVC_NO_OVERLAP_TERMUNITS
GROUP BY
    week_end_date, TERMUNITS
ORDER BY
    week_end_date, TERMUNITS 
	;




--------------------------- ADDITIONAL AUW METRICS (HEADLINE, AS OF 13TH AUGUST 2025)

CREATE OR REPLACE TABLE indus.public.final_auw_metrics AS
WITH

og_auw_updated_tag AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      review_complete_time__c::DATE AS review_complete_time,
      t2.credit_limit AS post_cl,
      t1.credit_limit AS pre_cl,
      recordtypeid
    FROM (
      SELECT *
      FROM external_data_sources.salesforce_nova.loan__c
      WHERE recordtypeid IN ('012Rd000001B2txIAC')
        AND review_complete_time__c IS NOT NULL
    ) a
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) AS t1
      ON a.fundbox_id__c = t1.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(DAY, 1, t1.edate)
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) AS t2
      ON a.fundbox_id__c = t2.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(DAY, -1, t2.edate)
  )
  SELECT fbbid, MIN(review_complete_time) AS auw_og_inc_time
  FROM tab1
  WHERE post_cl > pre_cl
  GROUP BY 1
),


HVCData AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    MIN(review_complete_time__c::DATE) AS loan_review_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000002EppVIAS'
    AND status__c IS NOT NULL
    AND status__c IN (
      'Complete - Increase',
      'Complete - Temp Increase',
      'Complete - Temp Increase + Terms',
      'Complete - Counteroffer',
      'Complete - Increase + Better Terms'
    )
  GROUP BY 1
),
HVC_list_ns AS (
  SELECT
    hvc.fbbid,
    hvc.loan_review_date
  FROM HVCData hvc
  WHERE hvc.loan_review_date IS NOT NULL
    -- AND NOT EXISTS (
    --   SELECT 1
    --   FROM og_auw_updated_tag oge
    --   WHERE oge.fbbid = hvc.fbbid
    --     AND oge.auw_og_inc_time = hvc.loan_review_date
    -- )
),


pre_approval_ns AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    MIN(review_complete_time__c::DATE) AS pre_approval_date
  FROM external_data_sources.salesforce_nova.loan__c
  WHERE recordtypeid = '012Rd000000jbbJIAQ'
    AND fbbid IN (
      SELECT DISTINCT fbbid
      FROM bi.public.customers_data
      WHERE first_approved_time IS NOT NULL
    )
    AND review_complete_time__c IS NOT NULL
  GROUP BY 1
),


tab1_pre_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_pre_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      CASE
        WHEN YEAR(auw_pre_doc_review_start_time__c::DATE) >= 2025
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        WHEN auw_pre_doc_review_start_time__c IS NULL
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        ELSE auw_pre_doc_approved_limit__c
      END AS final_auw_pre_doc_approved_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_pre_doc_review_complete_time__c IS NOT NULL
      AND auw_pre_doc_review_status__c ILIKE '%Complete - Increase%'
  )
  SELECT fbbid, MIN(ob_increase_auw_date) AS ob_increase_auw_date_min
  FROM tab1
  WHERE final_auw_pre_doc_approved_limit > first_approved_credit_limit
  GROUP BY 1
),


tab1_post_doc_inc AS (
  WITH tab1 AS (
    SELECT
      FUNDBOX_ID__C AS fbbid,
      auw_post_doc_review_complete_time__c::DATE AS ob_increase_auw_date,
      b.first_approved_credit_limit,
      auw_post_doc_approved_limit__c,
      CASE
        WHEN YEAR(auw_pre_doc_review_start_time__c::DATE) >= 2025
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        WHEN auw_pre_doc_review_start_time__c IS NULL
          THEN COALESCE(approved_uw_credit_limit__c, auw_pre_doc_approved_limit__c)
        ELSE auw_pre_doc_approved_limit__c
      END AS final_auw_pre_doc_approved_limit
    FROM external_data_sources.salesforce_nova.loan__c a
    LEFT JOIN bi.public.customers_data b
      ON a.fundbox_id__c = b.fbbid
    WHERE recordtypeid = '012Rd000000AcjxIAC'
      AND auw_post_doc_review_complete_time__c IS NOT NULL
      AND auw_post_doc_review_status__c ILIKE '%Complete - Increase%'
  )
  SELECT fbbid, MIN(ob_increase_auw_date) AS ob_increase_auw_date_min
  FROM tab1
  WHERE auw_post_doc_approved_limit__c > first_approved_credit_limit
  GROUP BY 1
),


tab1_SL AS (
  SELECT
    FUNDBOX_ID__C AS fbbid,
    review_complete_time__c::DATE AS sl_auw_date,
    b.first_approved_credit_limit,
    approved_uw_credit_limit__c,
    first_suggested_credit_limit__c,
    (approved_uw_credit_limit__c - COALESCE(b.first_approved_credit_limit, first_suggested_credit_limit__c)) AS delta_n
  FROM external_data_sources.salesforce_nova.loan__c a
  LEFT JOIN bi.public.customers_data b
    ON a.fundbox_id__c = b.fbbid
  WHERE recordtypeid = '0124T000000DSMTQA4'
    AND review_complete_time__c::DATE IS NOT NULL
    AND FUNDBOX_ID__C IN (
      SELECT DISTINCT fbbid FROM bi.public.customers_data WHERE first_approved_time IS NOT NULL
    )
),
tab2_SL AS (
  SELECT
    a.*,
    b.automated_cl,
    b.cl_delta,
    CASE
      WHEN b.cl_delta > 0 THEN 'Increase'
      WHEN b.cl_delta < 0 THEN 'Decrease'
      WHEN b.cl_delta = 0 THEN 'No Change'
      ELSE 'NA'
    END AS tag1_chk
  FROM tab1_SL a
  LEFT JOIN analytics.credit.second_look_accounts b
    ON a.fbbid = b.fbbid
),


aw_list AS (
  SELECT fbbid, loan_review_date AS eff_dt, 'HVC'          AS tag1 FROM HVC_list_ns
  UNION ALL
  SELECT fbbid, pre_approval_date,                     'Pre-Approval' AS tag1 FROM pre_approval_ns
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min,              'OB AUW'       AS tag1 FROM tab1_pre_doc_inc
  UNION ALL
  SELECT fbbid, ob_increase_auw_date_min,              'OB AUW'       AS tag1 FROM tab1_post_doc_inc
  UNION ALL
  SELECT fbbid, sl_auw_date,                           'Second Look'   AS tag1 FROM tab2_SL WHERE tag1_chk <> 'NA'
  UNION ALL
  SELECT fbbid, auw_og_inc_time,                       'OG AUW'       AS tag1 FROM og_auw_updated_tag
),


auw_base AS (
  SELECT
    loan_key, fbbid, week_end_date, week_start_date, loan_created_date, first_planned_transmission_date,
    is_charged_off, charge_off_date, outstanding_principal_due, originated_amount, dpd_days_corrected,
    new_cust_filter, bucket_group, risk_bucket, termunits, partner, intuit_flow, nav_flow,
    national_funding_flow, lendio_flow, payment_plan, industry_type, fico, vantage4,
    total_paid, fees_paid, principal_paid, is_after_co, customer_annual_revenue_group
  FROM analytics.credit.loan_level_data_pb
  WHERE sub_product <> 'mca'
    AND fbbid IN (SELECT DISTINCT fbbid FROM aw_list)
),


auw_base_join AS (
  SELECT
    a.*,
    b.fbbid AS fbbid_aw_list,
    b.eff_dt,
    b.tag1
  FROM auw_base a
  LEFT JOIN aw_list b
    ON a.fbbid = b.fbbid
   AND a.week_end_date >= b.eff_dt
),
auw_base_join_deduped AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbbid, loan_key, week_end_date ORDER BY eff_dt DESC) AS rnk
    FROM auw_base_join
  ) q
  WHERE rnk = 1
),


auw_perf_metrics_cte AS (
  SELECT
    week_start_date,
    week_end_date,

  
    SUM(CASE WHEN tag1 = 'OB AUW' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS ob_auw_total_os,
    SUM(CASE WHEN tag1 = 'OB AUW' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS ob_auw_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'OB AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS ob_auw_gross_co,
    SUM(CASE WHEN tag1 = 'OB AUW' THEN fees_paid * 52 ELSE 0 END) AS ob_auw_revenue,
    SUM(CASE WHEN tag1 = 'OB AUW' THEN fees_paid ELSE 0 END) AS ob_auw_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'OB AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OB AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS ob_auw_gross_yield,
      SUM(CASE WHEN tag1 = 'OB AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OB AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OB AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS ob_auw_net_yield,
    SUM(CASE WHEN tag1 = 'OB AUW' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS ob_auw_originations,

    
    SUM(CASE WHEN tag1 = 'OG AUW' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS og_auw_total_os,
    SUM(CASE WHEN tag1 = 'OG AUW' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS og_auw_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS og_auw_gross_co,
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END) AS og_auw_revenue,
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid ELSE 0 END) AS og_auw_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS og_auw_gross_yield,
     SUM(CASE WHEN tag1 = 'OG AUW' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS og_auw_net_yield,
    SUM(CASE WHEN tag1 = 'OG AUW' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS og_auw_originations,

   
    SUM(CASE WHEN tag1 = 'Second Look' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS sl_total_os,
    SUM(CASE WHEN tag1 = 'Second Look' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS sl_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'Second Look' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS sl_gross_co,
    SUM(CASE WHEN tag1 = 'Second Look' THEN fees_paid * 52 ELSE 0 END) AS sl_revenue,
    SUM(CASE WHEN tag1 = 'Second Look' THEN fees_paid ELSE 0 END) AS sl_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'Second Look' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Second Look' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS sl_gross_yield,
     SUM(CASE WHEN tag1 = 'Second Look' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Second Look' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Second Look' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS sl_net_yield,
    SUM(CASE WHEN tag1 = 'Second Look' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS sl_originations,

    
    SUM(CASE WHEN tag1 = 'HVC' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS hvc_total_os,
    SUM(CASE WHEN tag1 = 'HVC' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS hvc_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS hvc_gross_co,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END) AS hvc_revenue,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid ELSE 0 END) AS hvc_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS hvc_gross_yield,
    SUM(CASE WHEN tag1 = 'HVC' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS hvc_net_yield,
    SUM(CASE WHEN tag1 = 'HVC' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS hvc_originations,

    
    SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS pre_approvals_total_os,
    SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS pre_approvals_os_1_90_dpd,
    SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS pre_approvals_gross_co,
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END) AS pre_approvals_revenue,
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid ELSE 0 END) AS pre_approvals_revenue_NOT_ANNUAL,
    SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS pre_approvals_gross_yield,
     SUM(CASE WHEN tag1 = 'Pre-Approval' THEN fees_paid * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
      - SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS pre_approvals_net_yield,
    SUM(CASE WHEN tag1 = 'Pre-Approval' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS pre_approvals_originations,


    SUM(CASE WHEN is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS total_os_all,
    SUM(CASE WHEN is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS os_1_90_dpd_all,
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_co_all,
    SUM(fees_paid * 52) AS revenue_all,
    SUM(fees_paid) AS revenue_all_NOT_ANNUAL,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_yield_all,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
    - SUM(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS net_yield_all,
    SUM(case when loan_created_date between week_start_date and week_end_date then originated_amount else null end) AS originations_all
  FROM auw_base_join_deduped
  WHERE tag1 IS NOT NULL
  GROUP BY 1, 2
),


auw_op_metrics_cte AS (
  SELECT
    CASE
      WHEN dayofweek(current_date()) = 3
        THEN DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0
           AND DATEDIFF('day', review_complete_date_coalesce, current_date()) <= 0
        THEN NULL
      WHEN DATEDIFF('day', review_complete_date_coalesce, DATE_TRUNC('WEEK', current_date() + 4)::DATE - 5) < 0
        THEN current_date() - 1
      ELSE DATE_TRUNC('WEEK', review_complete_date_coalesce::DATE + 4)::DATE + 2
    END AS week_end_date,
    SUM(sla_hrs) AS sum_sla_hrs,
    COUNT(*) AS files_reviewed,
    SUM(CASE WHEN program_type = 'OB AUW'         THEN sla_hrs END) AS ob_sla_hrs,
    SUM(CASE WHEN program_type = 'OG AUW'         THEN sla_hrs END) AS og_sla_hrs, 
    SUM(CASE WHEN program_type = 'SL'             THEN sla_hrs END) AS sl_sla_hrs,
    SUM(CASE WHEN program_type = 'Pre-Approvals'  THEN sla_hrs END) AS pre_app_sla_hrs,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_sla_hrs,
    SUM(CASE WHEN program_type = 'OB AUW'         THEN 1 ELSE 0 END) AS ob_count,
    SUM(CASE WHEN program_type = 'OG AUW'         THEN 1 ELSE 0 END) AS og_count,
    SUM(CASE WHEN program_type = 'SL'             THEN 1 ELSE 0 END) AS sl_count,
    SUM(CASE WHEN program_type = 'Pre-Approvals'  THEN 1 ELSE 0 END) AS pre_app_count,
    SUM(CASE WHEN program_type = 'AUW Monitoring' THEN 1 ELSE 0 END) AS auw_monitoring_count,

 MEDIAN(sla_hrs) AS median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'OB AUW'         THEN sla_hrs END) AS ob_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'OG AUW'         THEN sla_hrs END) AS og_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'SL'             THEN sla_hrs END) AS sl_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'Pre-Approvals'  THEN sla_hrs END) AS pre_app_median_sla_hrs,
    MEDIAN(CASE WHEN program_type = 'AUW Monitoring' THEN sla_hrs END) AS auw_monitoring_median_sla_hrs
    
  FROM (
    SELECT
      fundbox_id__c,
      recordtypeid,
      status__c,
      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC'                            THEN 'OB AUW'
        WHEN recordtypeid = '0124T000000DSMTQA4'                            THEN 'SL'
        WHEN recordtypeid = '012Rd000000jbbJIAQ'                            THEN 'Pre-Approvals'
        WHEN recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS')    THEN 'OG AUW'
        WHEN recordtypeid = '012Rd000002Dp5CIAS'                            THEN 'AUW Monitoring'
        ELSE 'Other'
      END AS program_type,

      CASE
        WHEN recordtypeid = '012Rd000000AcjxIAC' THEN b.first_approved_credit_limit
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.automated_cl
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.automated_cl_pa
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t1.credit_limit
        ELSE c.credit_limit
      END AS credit_limit_filled,

      CASE
        WHEN recordtypeid = '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
        WHEN recordtypeid = '012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa
        WHEN (recordtypeid = '012Rd000002Dp5CIAS' AND status__c IN ('Close Account', 'RMR/Disable')) THEN 0
        WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t2.credit_limit
        ELSE COALESCE(approved_uw_credit_limit__c, 0)
      END AS approved_uw_credit_limit,

      CASE
        WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
          CASE
            WHEN status__c = 'Complete - Current CL'              THEN 'No change'
            WHEN status__c = 'Reduce CL'                          THEN 'Decrease'
            WHEN status__c IN ('Close Account','RMR/Disable')     THEN 'Close/RMR/Disable'
            ELSE status__c
          END
        ELSE
          CASE
            WHEN approved_uw_credit_limit > credit_limit_filled                         THEN 'Increase'
            WHEN approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit <> 0 THEN 'Decrease'
            WHEN approved_uw_credit_limit = credit_limit_filled                         THEN 'No change'
            WHEN approved_uw_credit_limit = 0                                          THEN 'Rejected'
            ELSE 'Other'
          END
      END AS decision_type,

      review_start_time__c::DATE AS review_start_date,
      review_complete_time__c::DATE AS review_complete_date,
      auw_pre_doc_review_complete_time__c::DATE AS auw_pre_doc_review_complete_date,

      COALESCE(review_complete_time__c, auw_pre_doc_review_complete_time__c)            AS review_complete_time_coalesce,
      COALESCE(review_start_time__c, auw_pre_doc_review_start_time__c)                  AS review_start_time_coalesce,
      COALESCE(review_complete_time__c::DATE, auw_pre_doc_review_complete_time__c::DATE) AS review_complete_date_coalesce,

      TIMESTAMPDIFF(MINUTE, review_start_time_coalesce, review_complete_time_coalesce) AS sla_minutes,
      sla_minutes / 60 AS sla_hrs
    FROM external_data_sources.salesforce_nova.loan__c A
    LEFT JOIN bi.public.customers_data b
      ON b.fbbid = a.fundbox_id__c
    LEFT JOIN bi.public.daily_approved_customers_data c
      ON c.fbbid = a.fundbox_id__c
     AND a.createddate::DATE = c.edate
    LEFT JOIN analytics.credit.second_look_accounts sl
      ON a.fundbox_id__c = sl.fbbid
    LEFT JOIN (
      SELECT fbbid, partner_name, calculated_annual_revenue AS calculated_annual_revenue_pa,
             pre_approval_amount AS automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
      FROM bi.customers.leads_data
    ) l
      ON l.fbbid = a.fundbox_id__c
     AND a.recordtypeid = '012Rd000000jbbJIAQ'
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) t1
      ON a.fundbox_id__c = t1.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(day, 1, t1.edate)
     AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')
    LEFT JOIN (
      SELECT fbbid, credit_limit, edate
      FROM bi.public.daily_approved_customers_data
    ) t2
      ON a.fundbox_id__c = t2.fbbid
     AND a.review_complete_time__c::DATE = DATEADD(day, -1, t2.edate)
     AND a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS', '012Rd000002Dp5CIAS')
    WHERE (review_complete_time__c IS NOT NULL OR auw_pre_doc_review_complete_time__c IS NOT NULL)
      AND recordtypeid IN (
        '012Rd000000AcjxIAC','0124T000000DSMTQA4','012Rd000000jbbJIAQ',
        '012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS'
      )
      AND fundbox_id__c <> 9987800100888
  ) c
  GROUP BY 1
),


high_rev_atd_metrics AS (
  WITH atd_population AS (
    SELECT week_end_date, fbbid
    FROM analytics.credit.customer_management_helper_pb
    WHERE able_to_draw = 1
      AND customer_annual_revenue_group = '> $1.5M'
    GROUP BY 1,2
  )
  SELECT
    p.week_end_date,
    COUNT(DISTINCT p.fbbid)                          AS total_atd_customers_high_rev,
    SUM(COALESCE(l.fees_paid * 52, 0))               AS total_revenue_high_rev_atd
  FROM atd_population p
  LEFT JOIN analytics.credit.loan_level_data_pb l
    ON p.week_end_date = l.week_end_date
   AND p.fbbid = l.fbbid
  GROUP BY 1
)


SELECT
  a.week_end_date,
  a.total_os_all, a.os_1_90_dpd_all, a.gross_co_all, a.revenue_all,a.revenue_all_NOT_ANNUAL, a.gross_yield_all, a.net_yield_all, a.originations_all,

  a.ob_auw_total_os, a.ob_auw_os_1_90_dpd, a.ob_auw_gross_co, a.ob_auw_revenue, a.ob_auw_revenue_NOT_ANNUAL, a.ob_auw_gross_yield,a.ob_auw_net_yield, a.ob_auw_originations,
  a.og_auw_total_os, a.og_auw_os_1_90_dpd, a.og_auw_gross_co, a.og_auw_revenue,a.og_auw_revenue_NOT_ANNUAL, a.og_auw_gross_yield,a.og_auw_net_yield, a.og_auw_originations,
  a.sl_total_os, a.sl_os_1_90_dpd, a.sl_gross_co, a.sl_revenue,a.sl_revenue_NOT_ANNUAL, a.sl_gross_yield,a.sl_net_yield, a.sl_originations,
  a.hvc_total_os, a.hvc_os_1_90_dpd, a.hvc_gross_co, a.hvc_revenue,a.hvc_revenue_NOT_ANNUAL, a.hvc_gross_yield,a.hvc_net_yield, a.hvc_originations,
  a.pre_approvals_total_os, a.pre_approvals_os_1_90_dpd, a.pre_approvals_gross_co, a.pre_approvals_revenue, a.pre_approvals_revenue_NOT_ANNUAL, a.pre_approvals_gross_yield,a.pre_approvals_net_yield, a.pre_approvals_originations,

  b.sum_sla_hrs, b.files_reviewed,
  b.ob_sla_hrs, b.og_sla_hrs, b.sl_sla_hrs, b.pre_app_sla_hrs, b.auw_monitoring_sla_hrs,
  b.ob_count, b.og_count, b.sl_count, b.pre_app_count, b.auw_monitoring_count,

  
 (COALESCE(b.pre_app_sla_hrs, 0) + COALESCE(b.ob_sla_hrs, 0) + COALESCE(b.sl_sla_hrs, 0)) AS reactive_sla_hrs,
(COALESCE(b.og_sla_hrs, 0) + COALESCE(b.auw_monitoring_sla_hrs, 0)) AS proactive_sla_hrs,
(COALESCE(b.pre_app_count, 0) + COALESCE(b.ob_count, 0) + COALESCE(b.sl_count, 0)) AS reactive_count,
(COALESCE(b.og_count, 0) + COALESCE(b.auw_monitoring_count, 0)) AS proactive_count,

--adding median proactive/reactive split
(COALESCE(b.pre_app_median_sla_hrs, 0) + COALESCE(b.ob_median_sla_hrs, 0) + COALESCE(b.sl_median_sla_hrs, 0)) AS reactive_median_sla_hrs,
(COALESCE(b.og_median_sla_hrs, 0) + COALESCE(b.auw_monitoring_median_sla_hrs, 0)) AS proactive_median_sla_hrs,


b.median_sla_hrs, b.ob_median_sla_hrs, b.og_median_sla_hrs, b.sl_median_sla_hrs, b.pre_app_median_sla_hrs, b.auw_monitoring_median_sla_hrs,

  c.total_atd_customers_high_rev,
  c.total_revenue_high_rev_atd
  
FROM auw_perf_metrics_cte a
LEFT JOIN auw_op_metrics_cte b
  ON a.week_end_date = b.week_end_date
LEFT JOIN high_rev_atd_metrics c
  ON a.week_end_date = c.week_end_date
ORDER BY a.week_end_date;
