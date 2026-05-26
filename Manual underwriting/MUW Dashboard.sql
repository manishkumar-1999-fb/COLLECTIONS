------------------------------------------------ MANUAL UNDERWRITING AGENT PERFORMANCE -------------------------------
    /*This table is a review level table which includes all relevant metrics pertaining to a review, such as 
        1. What program the review pertains to (recordtypeid)
        2. Who is the underwriter and secondary underwriter
        3. The SLA time (review complete time - review start time) -- the total time taken from when the review record is created to decisioning
        4. The TAT time (secondary review start time - full underwriting start time) -- time taken by primary underwriter to review the file
        5. Customer attributes
        6. Manual v automated CLs
        7. Decision of the review
*/

 select * from ANALYTICS.CREDIT.MANUAL_UW_AGENT_V1 where approver_name='Christopher Dykes';


CREATE OR REPLACE VIEW ANALYTICS.CREDIT.MANUAL_UW_AGENT_V1 as (
SELECT 
        a.underwriter__c as underwriter
        , A.approver__c -- Secondary review underwriter
        , CASE WHEN a.underwriter__c='0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN a.underwriter__c='0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN a.underwriter__c='005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN a.underwriter__c='0054T0000010694QAA' THEN 'Neil Patel'
            WHEN a.underwriter__c='005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN a.underwriter__c='0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN a.underwriter__c='0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN a.underwriter__c='005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN a.underwriter__c='005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN a.underwriter__c='005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN a.underwriter__c='005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN a.underwriter__c='005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN a.underwriter__c='005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN a.underwriter__c='005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN a.underwriter__c='005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            WHEN a.underwriter__c='005Rd000006hZqIIAU' THEN 'Venkatesh Kaipu Thyagaraj'
            WHEN a.underwriter__c='005Rd000006oZC1IAM' THEN 'Ramkumar Dendukuru'
            WHEN a.underwriter__c='005Rd000006FZV3IAO' THEN 'Kaleema Shaik'
            ELSE a.underwriter__c END underwriter__c 
        
        , CASE WHEN a.approver__c=underwriter THEN NULL 
            WHEN a.approver__c='0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN a.approver__c='0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN a.approver__c='005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN a.approver__c='0054T0000010694QAA' THEN 'Neil Patel'
            WHEN a.approver__c='005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN a.approver__c='0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN a.approver__c='0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN a.approver__c='005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN a.approver__c='005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN a.approver__c='005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN a.approver__c='005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN a.approver__c='005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN a.approver__c='005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN a.approver__c='005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN a.approver__c='005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            WHEN a.approver__c='005Rd000006hZqIIAU' THEN 'Venkatesh Kaipu Thyagaraj'
            WHEN a.approver__c='005Rd000006oZC1IAM' THEN 'Ramkumar Dendukuru'
            WHEN a.approver__c='005Rd000006FZV3IAO' THEN 'Kaleema Shaik'
            ELSE a.approver__c END approver_name     
    
    , a.fundbox_id__c AS fbbid
    , A.recordtypeid -- Program 
    , A.RISK_LEVEL__C
    , A.createddate::date as createddate -- When the record is created in MU pipeline
    , A.REVIEW_START_TIME__C -- When the record enters a given underwriter's queue
    , A.review_complete_time__c  -- Final decisioning completed
    , A.REVIEW_START_TIME__C::date AS review_start_date
    , A.review_complete_time__c::date review_complete_date   
    , A.auw_pre_doc_review_start_time__c::date AS auw_pre_doc_review_start_date -- When the record enters a given underwriter's queue [OB AUW]
    , A.auw_pre_doc_review_complete_time__c:: date AS auw_pre_doc_review_complete_date -- Final decisioning completed [OB AUW]

    
    , COALESCE(review_start_date, auw_pre_doc_review_start_date) AS review_start_date_coalesce -- All reviews use review start and complete time but AUW uses auw_pre_doc_review start/complete time
    , COALESCE(review_complete_date, auw_pre_doc_review_complete_date) AS review_complete_date_coalesce
    , COALESCE(REVIEW_START_TIME__C, auw_pre_doc_review_start_time__c) AS review_start_time_coalesce
    , COALESCE(REVIEW_COMPLETE_TIME__C, auw_pre_doc_review_complete_time__c) AS review_complete_time_coalesce
    
    , full_underwriting_start_time__c -- when primary underwriter starts actively reviewing the file
    -- , secondary_review_start_time__c -- when secondary underwriter starts reviewing the file - only available if a file underwent secondary review
    , CASE 
        WHEN underwriter = approver__c THEN NULL 
        ELSE secondary_review_start_time__c 
    END AS secondary_review_start_time__c
    
    , hold_start_time__c
    , hold_end_time__c
    , CASE WHEN timestampdiff(minute,hold_start_time__c,hold_end_time__c) IS NULL THEN 0 
            ELSE timestampdiff(minute,hold_start_time__c,hold_end_time__c) END hold_duration -- duration for which the record was in hold [currently we dont use these since there is issues with how the data flows in for hold timestamps]
            
    -- Dont really use these 3 fields
    , timestampdiff(minute, full_underwriting_start_time__c,review_complete_time_coalesce) review_duration -- time taken for review 
    , review_duration-hold_duration review_time_min_net -- time taken for review excluding time in hold (min)
    , review_time_min_net/60 review_time_hr_net -- time taken for review excluding time in hold (hr)

    -- TAT Times : time taken for primary review
    -- , case WHEN secondary_review_start_time__c>full_underwriting_start_time__c 
    --         THEN (TIMESTAMPDIFF(HOUR, A.full_underwriting_start_time__c, secondary_review_start_time__c)) 
    --         ELSE NULL END AS tat_hrs_num 
    -- , case WHEN secondary_review_start_time__c>full_underwriting_start_time__c 
    --         THEN (TIMESTAMPDIFF(MINUTE, A.full_underwriting_start_time__c, secondary_review_start_time__c)) 
    --         ELSE NULL END AS tat_minutes_num

    -- new TAT metric calculation as requested by Neil
    , CASE 
        WHEN COALESCE(secondary_review_start_time__c, review_complete_time_coalesce) > A.full_underwriting_start_time__c
        THEN (TIMESTAMPDIFF(HOUR, A.full_underwriting_start_time__c, COALESCE(secondary_review_start_time__c, review_complete_time_coalesce)))
        ELSE NULL 
      END AS tat_hrs_num
    
    , CASE 
        WHEN COALESCE(secondary_review_start_time__c, review_complete_time_coalesce) > A.full_underwriting_start_time__c
        THEN (TIMESTAMPDIFF(MINUTE, A.full_underwriting_start_time__c, COALESCE(secondary_review_start_time__c, review_complete_time_coalesce)))
        ELSE NULL 
      END AS tat_minutes_num
    
    -- Secondary review time
    , CASE WHEN (underwriter=approver__c OR approver_name IS NULL) THEN NULL
    ELSE TIMESTAMPDIFF(HOUR, A.SECONDARY_REVIEW_START_TIME__C, review_complete_time_coalesce) END AS sec_review_hrs
    
    -- SLA time : complete turnaround time from a file from when it enters queue to decisioning
    , DATEDIFF(MINUTE,REVIEW_START_TIME__C,review_complete_time__c) sla
    , TIMESTAMPDIFF(MINUTE,REVIEW_START_TIME__C,review_complete_time__c) sla1
    , TIMESTAMPDIFF(MINUTE,review_start_time_coalesce,review_complete_time_coalesce) sla1_complete
    , sla1_complete/60 sla1_complete_hrs
    , CASE WHEN CAST(review_start_time_coalesce AS TIME) > '16:00:00' THEN TO_TIMESTAMP_NTZ(DATEADD(DAY, 1, CAST(REVIEW_START_TIME__C AS DATE)) || ' 08:00:00')
           ELSE review_start_time_coalesce END AS sla_review_time

    -- Profiling customer     
    , f.channel
    , CASE WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.partner_name ELSE f.partner END AS partner
    , f.ob_bucket_group_retro
    
    , CASE WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.calculated_annual_revenue_pa
           ELSE COALESCE(b.first_account_size_accounting_software,b.first_account_size_fi,0) * 12 
           END customer_annual_revenue     
    ,CASE WHEN customer_annual_revenue >= 0 AND customer_annual_revenue < 150000 THEN '0K-150K'
          WHEN customer_annual_revenue >= 150000 AND customer_annual_revenue < 500000 THEN '150K-500K'
          WHEN customer_annual_revenue >= 500000 and customer_annual_revenue<1000000 THEN '500K-1M'
          WHEN customer_annual_revenue >= 1000000 and customer_annual_revenue<2000000 THEN '1M-2M'
          WHEN customer_annual_revenue >= 2000000 THEN '2M+'
          ELSE NULL
          END rev_band
          
    -- CLs and decisions      
    , CASE WHEN recordtypeid='012Rd000000AcjxIAC' THEN b.first_approved_credit_limit -- OB AUW
           WHEN recordtypeid='0124T000000DSMTQA4' THEN sl.automated_cl -- SL 
           WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.automated_cl_pa -- Pre-approval
           WHEN recordtypeid in ('012Rd000001B2txIAC','012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t1.credit_limit -- OG AUW, AUW Monitoring, HVC
           ELSE c.credit_limit END credit_limit_filled ------------ Automated CL
           
    , CASE WHEN recordtypeid='0124T000000DSMTQA4' THEN sl.first_approved_credit_limit -- SL 
           WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa -- Pre-approval
           WHEN (recordtypeid='012Rd000002Dp5CIAS' AND status__c IN ('Close Account', 'RMR/Disable')) THEN 0 -- AUW Monitoring
           WHEN recordtypeid in ('012Rd000001B2txIAC','012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t2.credit_limit -- OG AUW, HVC
           ELSE COALESCE(approved_uw_credit_limit__c,0) 
           END approved_uw_credit_limit ------------ Manually approved CL
           
    , CASE
            WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
                CASE 
                    WHEN status__c = 'Complete - Current CL' THEN 'No change'
                    WHEN status__c = 'Reduce CL' THEN 'Decrease'
                    WHEN status__c IN ('Close Account', 'RMR/Disable') THEN 'Close/RMR/Disable'
                    ELSE status__c 
                END
            ELSE
                CASE 
                    WHEN approved_uw_credit_limit > credit_limit_filled THEN 'Increase'
                    WHEN (approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit != 0) THEN 'Decrease'
                    WHEN approved_uw_credit_limit = credit_limit_filled THEN 'No change'
                    WHEN approved_uw_credit_limit = 0 THEN 'Rejected'
                    ELSE 'Other' 
                END
        END AS decision_type
        
    , status__c
        
    , CASE WHEN leads_flag_fbbid IS NOT NULL THEN 1 ELSE 0 END leads_flag
    , b.first_approved_time::date first_approved_date
    , b.registration_time::date registration_date
    , b.first_draw_time
    , c.credit_limit
    , b.first_approved_credit_limit

FROM 
-- Base table from which review data is pulled
(
    SELECT *
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C 
    WHERE UNDERWRITER__C IS NOT NULL
    AND fundbox_id__c IS NOT NULL
    AND (review_complete_time__c is not null
        OR auw_pre_doc_review_complete_time__c is not null)
    AND createddate >= '2024-8-1'
    AND fundbox_id__c NOT IN ('9987800100888', '999999999999999910', '99999999999987')
)a

    LEFT JOIN bi.public.customers_data b
    ON b.fbbid = a.fundbox_id__c

    LEFT JOIN bi.public.daily_approved_customers_data c
    ON c.fbbid = a.fundbox_id__c
    AND a.createddate::date=c.edate

    LEFT JOIN (SELECT DISTINCT fbbid, partner, channel, ob_bucket_group_retro FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2) f
    ON f.fbbid = a.fundbox_id__c

    -- SL data
    LEFT JOIN analytics.credit.second_look_accounts sl
    ON a.fundbox_id__c=sl.fbbid

    -- PA AUW data
    LEFT JOIN (SELECT fbbid, partner_name, calculated_annual_revenue AS calculated_annual_revenue_pa, pre_approval_amount as automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
    FROM bi.customers.leads_data) l
    ON l.fbbid=a.fundbox_id__c
    AND a.recordtypeid ='012Rd000000jbbJIAQ'

LEFT JOIN (select bff.fbbid AS leads_flag_fbbid
FROM cdc_v2.feature_flags.business_feature_flag_preferences bff
    JOIN cdc_v2.feature_flags.feature_flags ff
        ON ff.id = bff.feature_flag_id
            AND ff.id = 375
AND bff.value = TRUE) ld
ON a.fundbox_id__c=ld.leads_flag_fbbid -- leads flag indicates whether the customer was assigned to the Lead policy

LEFT JOIN (select fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t1 -- Day before review completed, for CL prior to decision
    ON a.fundbox_id__c = t1.fbbid
    AND a.review_complete_time__c::date = DATEADD(day, 1, t1.edate)
    AND A.recordtypeid in ('012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS') -- OG AUW, HVC, AUW monitoring
    
LEFT JOIN (select fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t2 -- Day after review completed, for CL after decision
    ON a.fundbox_id__c = t2.fbbid
    AND a.review_complete_time__c::date = DATEADD(day, -1, t2.edate)
    AND A.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS') -- OG AUW, HVC, AUW monitoring

WHERE recordtypeid IN ('012Rd000000AcjxIAC','012Rd000000jbbJIAQ', '0124T000000DSMTQA4', '012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS')
AND fundbox_id__c NOT IN ('9987800100888', '999999999999999910', '99999999999987')
);




--------------------------------------------------- MANUAL UNDERWRITING CUSTOMER RISK AND PROFITABILITY METRICS --------------------------------------------------
 /*This script tracks customer risk and revenue , such as 
        1. Loss rates
        2. Delinquency profiling
        3. Revenue
        4. Chargeoff rates

    Please note the following logic in tracking risk metrics:
        - A loan is tagged to the LAST program prior to its origination
        - For example, a customer got OB AUW in January 2025, and OG AUW in July 2025. They originated in April 2025. This loan and its subsequent performance will be tagged to the OB AUW review.
            - If this same loan goes delinquent in September 2025, it will STILL be tagged to the OB AUW review since it was originated post that review and prior to OG AUW review
        
    The way this has been implemented in the script is as follows:
        - We created a column review_complete_date_next which gives us the timestamp for the next review that a customer underwent 
            - EG: if a customer got OB AUW in Jan 2025, OG AUW in July 2025, HVC in September 2025
                - Review complete time next for OB AUW record (Jan 2025) would be July 2025
                - Review complete time next for OG AUW record (Jul 2025) would be Sept 2025
                - Review complete time next for HVC record would be null since there is no review after that
        - When joining FMD to the base review level table, we therefore add the following condition:
            - (fmd.loan_created_date>=a.review_complete_date_coalesce and (fmd.loan_created_date<a.review_complete_date_next or a.review_complete_date_next is null))
*/

--- Loan Level Table: Uses agent level table as base and joins relevant metrics at loan key X edate level



CREATE OR REPLACE VIEW analytics.credit.manual_uw_risk_metrics_v0 AS
SELECT a.underwriter__c  
     , a.approver__c
     , a.approver_name
     , a.recordtypeid
     , a.fbbid
     , fmd.loan_key
     , fmd.loan_created_date
     , A.RISK_LEVEL__C
     , a.decision_type
     , a.createddate
     , a.review_complete_date_coalesce
     , a.review_complete_date_next -- Timestamp of subsequent review from review complete date.
     , CASE WHEN a.recordtypeid IN ('012Rd000000AcjxIAC','012Rd000000jbbJIAQ', '0124T000000DSMTQA4') THEN cd.first_approved_time::date
           WHEN a.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS') THEN review_complete_date_coalesce 
           END start_date -- date from which we will track performance; if this is OB underwriting then first approved time, if OG then review complete date
     , dacd.edate
     , datediff(day,start_date,dacd.edate) dob
     , dacd.credit_limit
     , a.rev_band
     , a.credit_limit_filled
     , a.approved_uw_credit_limit
     , a.status__c
     , a.leads_flag
     , cd.first_approved_time::date first_approved_date
     , cd.registration_time::date registration_date
     , f.partner
     , f.ob_bucket_group_retro
     , F.INDUSTRY_TYPE
     , CASE WHEN fmd.dpd_days IS NULL and fmd.is_charged_off = 0 THEN 0
           WHEN fmd.dpd_days IS NULL and fmd.is_charged_off = 1 THEN 98
           ELSE fmd.dpd_days
           END dpd_days_corrected 
     , fmd.is_charged_off
     , cast(fmd.outstanding_principal_due as float) outstanding_principal_due
     , fmd.originated_amount
     , CASE WHEN fmd.loan_created_date=dacd.edate THEN fmd.originated_amount ELSE null END origination_at_date
     , fmd.charge_off_date
     , rev.revenue
     , case when u.min_udr_date=dacd.edate THEN 1 ELSE 0 END UDR35  
     , dpd.delinq_date,
    DATEDIFF(day, start_date, dpd.delinq_date) AS delinq_days_after_draw,
    f2.vantage4,
    CASE
        WHEN f2.vantage4 IS NULL THEN NULL
        WHEN f2.vantage4 < 550 THEN NULL
        WHEN f2.vantage4 < 600 THEN '550-600'
        WHEN f2.vantage4 < 650 THEN '600-650'
        WHEN f2.vantage4 < 700 THEN '650-700'
        WHEN f2.vantage4 < 750 THEN '700-750'
        WHEN f2.vantage4 < 800 THEN '750-800'
        WHEN f2.vantage4 <= 850 THEN '800-850'
        ELSE NULL
    END AS Vantage_buckets
FROM
(
SELECT *,
      LEAD(review_complete_date_coalesce) OVER (PARTITION BY fbbid ORDER BY review_complete_date_coalesce) as review_complete_date_next,
      LEAD(recordtypeid) OVER (PARTITION BY fbbid ORDER BY review_complete_date_coalesce) as recordtypeid_next
FROM ANALYTICS.CREDIT.MANUAL_UW_AGENT_V1
WHERE ((recordtypeid in ('012Rd000000AcjxIAC','012Rd000000jbbJIAQ','0124T000000DSMTQA4') and decision_type<>'Rejected')
    OR (recordtypeid in ('012Rd000002EppVIAS','012Rd000001B2txIAC') and decision_type not in ('Close/RMR/Disable','No change')))
    AND decision_type<>'Rejected') a

LEFT JOIN bi.public.customers_data cd
    ON a.fbbid=cd.fbbid

LEFT JOIN bi.public.daily_approved_customers_data dacd
    ON a.fbbid=dacd.fbbid

LEFT JOIN (SELECT DISTINCT fbbid, partner, channel, ob_bucket_group_retro,INDUSTRY_TYPE FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2) f
    ON a.fbbid = f.fbbid

LEFT JOIN -- All loan level info like DPD days, OS, originations
(SELECT *
FROM bi.finance.finance_metrics_daily
WHERE product_type <> 'Flexpay'
AND loan_operational_status <> 'CNCL'
AND loan_created_date>='2022-01-01') fmd
    ON a.fbbid = fmd.fbbid
    AND dacd.edate = fmd.edate
    -- AND (a.start_date<=fmd.loan_created_date and review_complete_date_next<=fmd.loan_created_date)
    AND (fmd.loan_created_date>=a.review_complete_date_coalesce and (fmd.loan_created_date<a.review_complete_date_next or a.review_complete_date_next is null)) -- This is the condition that ensures no double counting of loan performances

LEFT JOIN -- Revenue metrics
(SELECT fbbid , 
		edate,
		loan_key,
		sum(STATUS_VALUE) revenue
		FROM  bi.FINANCE.LOAN_STATUSES
		WHERE STATUS_NAME = 'REVENUE'
		GROUP BY 1, 2, 3
) rev
    ON a.fbbid = rev.fbbid
    AND fmd.loan_key=rev.loan_key
    AND dacd.edate=rev.edate

LEFT JOIN -- UDR/loss information for DPD 35
    (SELECT
        fbbid,
        MIN(edate) AS min_udr_date
    FROM (
        SELECT
            fbbid,
            edate,
            CASE
                WHEN dpd_days IS NULL AND is_charged_off = 0 THEN 0
                WHEN dpd_days IS NULL AND is_charged_off = 1 THEN 98
                ELSE dpd_days
            END AS dpd_days_corrected
        FROM
            BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE
            PRODUCT_TYPE <> 'Flexpay'
    )
    WHERE
        dpd_days_corrected >= 35
    GROUP BY
        fbbid) u
    ON a.fbbid=u.fbbid
    AND dacd.edate=u.min_udr_date 
/*added by mk*/
    LEFT JOIN (
    -- Single join subquery to get the first DPD1 date
    SELECT 
        fbbid, 
        loan_key, 
        loan_created_date, 
        MIN(edate) AS delinq_date
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate > '2022-01-01' AND dpd_bucket = 1
    GROUP BY 1, 2, 3
) dpd ON a.fbbid = dpd.fbbid 
    AND fmd.loan_created_date = dpd.loan_created_date 
    AND fmd.loan_key = dpd.loan_key
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2 f2
    ON a.fbbid = f2.fbbid 
    AND start_date = f2.edate
WHERE (dob IS NOT NULL and dob>=0)
AND dacd.fbbid NOT IN ('9987800100888', '999999999999999910', '99999999999987')
ORDER BY a.fbbid, dacd.edate
;


-- Select * from analytics.credit.manual_uw_risk_metrics_v1
-- where DPD_days>0 and is_charged_off = 0 and edate = current_date;


--  Customer level table: Aggregates above table up to the customer level
create or replace view analytics.credit.manual_uw_risk_metrics_v1 as (

with fmd_agg AS (
  SELECT
      fmd.fbbid
    , fmd.edate
    , MAX(fmd.dpd_days)     AS dpd_days        
    , MAX(fmd.dpd_bucket)   AS dpd_bucket
    , SUM(fmd.outstanding_principal_due) AS outstanding_balance_due_all_loans
    , MAX(fmd.is_charged_off) AS is_charged_off_any
  FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
 -- where edate >= '2025-10-01'
  GROUP BY 1,2
),
Latest_DPD_1 as
(
select *,
lag(dpd_bucket,1,null) over(partition by fbbid order by edate) as previous_bucket
from fmd_agg 
),
latest_dpd_2 as
(
Select * from latest_dpd_1 
where (previous_bucket = 0 or previous_bucket is null) and dpd_days >0
qualify row_number() over(partition by fbbid order by edate desc) = 1
)
--select * from latest_dpd_2 where fbbid = 887824 order by edate desc;

select review_complete_date_coalesce
     , dacd.first_approved_time::date first_approved_time
     , start_date
     , createddate
     , underwriter__c
     , approver__c
     , approver_name
     , recordtypeid
     , decision_type
     , dob
     , r.edate
     , r.fbbid
     , r.partner
     , ob_bucket_group_retro
     , c.edate as latest_dpd_1
     -- , og_bucket_group
     , rev_band
     , leads_flag
     , Vantage_buckets
     , industry_type
     , risk_level__c
     , min(delinq_days_after_draw) as delinq_days_after_draw
     , MAX(TRY_TO_NUMBER(dpd_days_corrected::STRING)) as dpd_days     
     , count(distinct case when dpd_days_corrected>0 then loan_key else null end) dq_draws
     , max(approved_uw_credit_limit) as mu_approved_limit
     , max(r.credit_limit) credit_limit
     , sum(r.outstanding_principal_due) outstanding_principal
     , sum(r.revenue) revenue
     , sum(origination_at_date) originated_amount
     , max(udr35) udr35
     , sum(case when is_charged_off>0 then 1 else 0 end) is_charged_off
     , min(charge_off_date) co_date
     -- , max(ftd_flag) ftd
     -- , max(r.first_draw_amount) fda
from analytics.credit.manual_uw_risk_metrics_v0 r
left join bi.public.daily_approved_customers_data dacd
    on r.fbbid = dacd.fbbid
    and r.edate = dacd.edate
left join latest_dpd_2 c
on r.fbbid = c.fbbid 
WHERE dacd.fbbid NOT IN ('9987800100888', '999999999999999910', '99999999999987')
group by ALL
order by fbbid, edate, dob)
;






-- Risk snapshots:
CREATE OR REPLACE VIEW analytics.credit.manual_uw_agent_risk_snapshots_2 AS
(
select UNDERWRITER__C
    , rev_band
    , ob_bucket_group_retro
    , partner
    , decision_type
    , recordtypeid
    , review_complete_date_coalesce
    , start_date
    , leads_flag
    , approver__c
    , approver_name
    , createddate
    , fbbid

    , count(distinct fbbid) num_files // total number of customers
    , sum(case when (is_charged_off = 1) and edate=charge_off_date then outstanding_principal_due else null end) os_chof // $OS CO
    , count(distinct case when (is_charged_off = 1) and edate=charge_off_date then fbbid else null end) files_chof //#CO overall

--------- 90 DOB ---------
    , count(distinct case when (dob = 90) then fbbid else null end) files_90dob // total number of customers at DOB 90
    , count(distinct case when ((dob = 90) and (dpd_days_corrected >0) and (is_charged_off = 0)) then fbbid else null end) files_dpd1_90dob // #DPD 1+ at DOB 90
    , count(distinct case when (dob = 90 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then fbbid else null end) files_dpd17_90dob //#DPD 1-7 at DOB 90
    , count(distinct case when (dob = 90 and dpd_days_corrected >= 35 and is_charged_off = 0) then fbbid else null end) files_dpd35_90dob // #DPD 35+ at DOB 90

    , sum(case when (dob = 90) then outstanding_principal_due else null end) os_90dob // $OS at 90 DOB
    , sum(case when (dob = 90 and dpd_days_corrected > 0 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd1_90dob // $OS DPD 1+ at 90 DOB
    , sum(case when (dob = 90 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd17_90dob//$OS DPD 1-7 at 90 DOB

    , sum(case when (dob = 90 and dpd_days_corrected >= 35 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd35_90dob // $OS DPD 35+ at 90 DOB
    
    , sum(case when dob<=90 then revenue else null end) revenue_90dob
    -- , sum(case when dob=90 then originated_amount else null end) orig_90dob

--------- 180 DOB ---------
    , count(distinct case when (dob = 180) then fbbid else null end) files_180dob // total number of customers at DOB 180
    , count(distinct case when ((dob = 180) and (dpd_days_corrected >0) and (is_charged_off = 0)) then fbbid else null end) files_dpd1_180dob // #DPD 1+ at DOB 180
    , count(distinct case when (dob = 180 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then fbbid else null end) files_dpd17_180dob//#DPD 1-7 @DOB 180
    , count(distinct case when (dob = 180 and dpd_days_corrected >= 35 and is_charged_off = 0) then fbbid else null end) files_dpd35_180dob // #DPD 35+ at DOB 180
    
    , sum(case when (dob = 180) then outstanding_principal_due else null end) os_180dob // $OS at 180 DOB
    , sum(case when (dob = 180 and dpd_days_corrected > 0 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd1_180dob // $OS DPD 1+ at 180 DOB
    , sum(case when (dob = 180 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd17_180dob//$OS DPD 1-7 at 180 DOB
    
    , sum(case when (dob = 180 and dpd_days_corrected >= 35 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd35_180dob //$OS DPD 35+ at 180 DOB
    , sum(case when dob<=180 then revenue else null end) revenue_180dob
    -- , sum(case when dob=180 then originated_amount else null end) orig_180dob

--------- 270 DOB ---------
    , count(distinct case when (dob = 270) then fbbid else null end) files_270dob // total number of customers at DOB 270
    , count(distinct case when ((dob = 270) and (dpd_days_corrected >0) and (is_charged_off = 0)) then fbbid else null end) files_dpd1_270dob // #DPD 1+ at DOB 270
    , count(distinct case when (dob = 270 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then fbbid else null end) files_dpd17_270dob//#DPD 1-7 @DOB 270
    , count(distinct case when (dob = 270 and dpd_days_corrected >= 35 and is_charged_off = 0) then fbbid else null end) files_dpd35_270dob // #DPD 35+ at DOB 270
    
    , sum(case when (dob = 270) then outstanding_principal_due else null end) os_270dob // $OS at 270 DOB
    , sum(case when (dob = 270 and dpd_days_corrected > 0 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd1_270dob // $OS DPD 1+ at 270 DOB
    , sum(case when (dob = 270 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd17_270dob//$OS DPD 1-7 at 270 DOB
    
    , sum(case when (dob = 270 and dpd_days_corrected >= 35 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd35_270dob //$OS DPD 35+ at 270 DOB
    , sum(case when dob<=270 then revenue else null end) revenue_270dob
    -- , sum(case when dob=270 then originated_amount else null end) orig_270dob

--------- 360 DOB ---------
    , count(distinct case when (dob = 360) then fbbid else null end) files_360dob // total number of customers at DOB 360
    , count(distinct case when ((dob = 360) and (dpd_days_corrected >0) and (is_charged_off = 0)) then fbbid else null end) files_dpd1_360dob // #DPD 1+ at DOB 360
    , count(distinct case when (dob = 360 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then fbbid else null end) files_dpd17_360dob//#DPD 1-7 @DOB 360
    , count(distinct case when (dob = 360 and dpd_days_corrected >= 35 and is_charged_off = 0) then fbbid else null end) files_dpd35_360dob // #DPD 35+ at DOB 360
    
    , sum(case when (dob = 360) then outstanding_principal_due else null end) os_360dob // $OS at 360 DOB
    , sum(case when (dob = 360 and dpd_days_corrected > 0 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd1_360dob // $OS DPD 1+ at 360 DOB
    , sum(case when (dob = 360 and (dpd_days_corrected between 1 and 7) and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd17_360dob//$OS DPD 1-7 at 360 DOB
    
    , sum(case when (dob = 360 and dpd_days_corrected >= 35 and is_charged_off = 0) then outstanding_principal_due else null end) os_dpd35_360dob //$OS DPD 35+ at 360 DOB
    , sum(case when dob<=360 then revenue else null end) revenue_360dob

    , sum(case when dob=360 and (is_charged_off = 1) then outstanding_principal_due else null end) os_chof360 // $OS CO at 360 DOB
    , count(distinct case when dob=360 and (is_charged_off = 1) then fbbid else null end) files_chof360 //#CO at 360 DOB
    -- , sum(case when dob=360 then originated_amount else null end) orig_360dob

from analytics.credit.manual_uw_risk_metrics_v0
group by all
order by 1,2
)
;

---------------------------------------------------------- MANUAL UNDERWRITING CUSTOMER ACTIVITY METRICS ----------------------------------------------------------
 /*This script tracks customer activity , such as 
        1. Originations
        2. Activation (essentially the same as FTD but looks at activation post-review for a given program)
        3. Utilization
    Please note that performance metrics are tracked ONLY for the MOST RECENT program for which a customer has been manually underwritten. 
        EG:: If a customer got OB AUW increase in Jan 2025 and subsequently got an OG AUW increase in July 2025, we are only tracking activity post that second (OG AUW) review in July
*/

--- Loan level table: Pulls the same things as agent level (underwriter, program, manual v auto CL, decision, review times etc) table and then adds on activity metrics at an edate and loan level 
CREATE OR REPLACE view analytics.credit.manual_uw_activity_metrics_v0 AS(
SELECT CASE WHEN a.underwriter__c='0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN a.underwriter__c='0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN a.underwriter__c='005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN a.underwriter__c='0054T0000010694QAA' THEN 'Neil Patel'
            WHEN a.underwriter__c='005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN a.underwriter__c='0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN a.underwriter__c='0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN a.underwriter__c='005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN a.underwriter__c='005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN a.underwriter__c='005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN a.underwriter__c='005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN a.underwriter__c='005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN a.underwriter__c='005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN a.underwriter__c='005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN a.underwriter__c='005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            ELSE a.underwriter__c END underwriter__c 
    , a.approver__c
    , CASE WHEN a.approver__c='0054T00000103TVQAY' THEN 'Monica Rios'
            WHEN a.approver__c='0054T0000010699QAA' THEN 'Christopher Dykes'
            WHEN a.approver__c='005Rd000003Lu2HIAS' THEN 'Greg Maitles'
            WHEN a.approver__c='0054T0000010694QAA' THEN 'Neil Patel'
            WHEN a.approver__c='005Rd000005K1LFIA0' THEN 'Dmitry Altshuler'
            WHEN a.approver__c='0054T000002Oc2DQAS' THEN 'Kia Fowler'
            WHEN a.approver__c='0054T000002NfylQAC' THEN 'Lorena Albright'
            WHEN a.approver__c='005Rd000004bZSjIAM' THEN 'Jerry Christian'
            WHEN a.approver__c='005Rd000005M10zIAC' THEN 'Nagur Vali Shaik'
            WHEN a.approver__c='005Rd000005M12bIAC' THEN 'Karthik Sirigiri'
            WHEN a.approver__c='005Rd000005W5cnIAC' THEN 'Sreekanth Anumula'
            WHEN a.approver__c='005Rd000005gikvIAA' THEN 'Rhythm Rai'
            WHEN a.approver__c='005Rd000005t5LxIAI' THEN 'Salman Khan'
            WHEN a.approver__c='005Rd000005t5NZIAY' THEN 'Banadita Rachel'
            WHEN a.approver__c='005Rd000005t5QnIAI' THEN 'Srihari Amudala'
            ELSE a.approver__c END approver_name 
    , a.recordtypeid
    , a.fbbid
    , fmd.loan_key
    , dacd.edate
    , CASE WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.partner_name ELSE f.partner END AS partner
    , f.ob_bucket_group_retro
    , f.og_bucket_group_retro
    , f.og_bucket_group
    , createddate::date as createddate
    , COALESCE(review_complete_time__c::date, auw_pre_doc_review_complete_time__c::date) AS review_complete_date_coalesce
    , CASE WHEN recordtypeid IN ('012Rd000000AcjxIAC','012Rd000000jbbJIAQ', '0124T000000DSMTQA4') THEN cd.first_approved_time::date -- For OB programs we take first approved time
           WHEN recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS') THEN review_complete_date_coalesce -- For OG programs this is review complete time
           END start_date -- Date from which we will track performance. 
    
    , datediff(day,start_date,dacd.edate) dob

    , CASE WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.calculated_annual_revenue_pa
           ELSE COALESCE(cd.first_account_size_accounting_software,cd.first_account_size_fi,0) * 12 END customer_annual_revenue_raw
    , CASE WHEN customer_annual_revenue_raw >= 0 AND customer_annual_revenue_raw < 150000 THEN '0K-150K'
          WHEN customer_annual_revenue_raw >= 150000 AND customer_annual_revenue_raw < 500000 THEN '150K-500K'
          WHEN customer_annual_revenue_raw >= 500000 and customer_annual_revenue_raw<1000000 THEN '500K-1M'
          WHEN customer_annual_revenue_raw >= 1000000 and customer_annual_revenue_raw<2000000 THEN '1M-2M'
          WHEN customer_annual_revenue_raw >= 2000000 THEN '2M+'
          ELSE NULL
          END rev_band
    
    , fmd.loan_created_date
    , cd.first_approved_time::date first_approved_time
    , cd.first_draw_amount

    -- CLs and decisions      
    , CASE WHEN recordtypeid='012Rd000000AcjxIAC' THEN cd.first_approved_credit_limit -- OB AUW
           WHEN recordtypeid='0124T000000DSMTQA4' THEN sl.automated_cl -- SL 
           WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.automated_cl_pa -- Pre-approval
           WHEN recordtypeid in ('012Rd000001B2txIAC','012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t1.credit_limit -- OG AUW, AUW Monitoring, HVC
           END credit_limit_filled // Automated CL
           
    , CASE WHEN recordtypeid='0124T000000DSMTQA4' THEN sl.first_approved_credit_limit -- SL 
           WHEN recordtypeid='012Rd000000jbbJIAQ' THEN l.auw_approved_limit_pa -- Pre-approval
           WHEN (recordtypeid='012Rd000002Dp5CIAS' AND status__c IN ('Close Account', 'RMR/Disable')) THEN 0 -- HVC
           WHEN recordtypeid in ('012Rd000001B2txIAC','012Rd000002EppVIAS', '012Rd000002Dp5CIAS') THEN t2.credit_limit -- OG AUW, AUW Monitoring
           ELSE COALESCE(approved_uw_credit_limit__c,0) 
           END approved_uw_credit_limit // Manually approved CL
           
    , CASE
            WHEN recordtypeid = '012Rd000002Dp5CIAS' THEN
                CASE 
                    WHEN status__c = 'Complete - Current CL' THEN 'No change'
                    WHEN status__c = 'Reduce CL' THEN 'Decrease'
                    WHEN status__c IN ('Close Account', 'RMR/Disable') THEN 'Close/RMR/Disable'
                    ELSE status__c 
                END
            ELSE
                CASE 
                    WHEN approved_uw_credit_limit > credit_limit_filled THEN 'Increase'
                    WHEN (approved_uw_credit_limit < credit_limit_filled AND approved_uw_credit_limit != 0) THEN 'Decrease'
                    WHEN approved_uw_credit_limit = credit_limit_filled THEN 'No change'
                    WHEN approved_uw_credit_limit = 0 THEN 'Rejected'
                    ELSE 'Other' 
                END
        END AS decision_type
    , status__c

    , cast(fmd.outstanding_principal_due as float) outstanding_principal_due
    , fmd.originated_amount
    , CASE WHEN fmd.loan_created_date=f.edate THEN fmd.originated_amount ELSE null END origination_at_date

    , CASE WHEN leads_flag_fbbid IS NOT NULL THEN 1 ELSE 0 END leads_flag
    , MIN(fmd.loan_key) OVER (PARTITION BY A.FBBID order by fmd.loan_key) activation_origination -- this is the date of first origination post review
    , CASE WHEN fmd.loan_key=activation_origination and fmd.loan_created_date=dacd.edate THEN 1 ELSE 0 end activation_flag -- same as FTD flag but also encompasses OG programs
    , CASE WHEN activation_flag=1 then origination_at_date else null end activation_amount -- amount originated at first origination
   
    
FROM
-- Review info
(
SELECT DISTINCT UNDERWRITER__C, fundbox_id__c fbbid, recordtypeid, approver__c, 
                approved_uw_credit_limit__c,status__c, review_complete_time__c, 
                auw_pre_doc_review_complete_time__c, createddate,
                coalesce(review_complete_time__c,auw_pre_doc_review_complete_time__c) as review_complete_time_filled,
                row_number() over (partition by fundbox_id__c order by review_complete_time_filled desc) rnk -- This field ranks review times in descending order so that the latest review has rank 1
FROM (
SELECT *
FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C 
WHERE UNDERWRITER__C IS NOT NULL
AND fundbox_id__c IS NOT NULL
AND fundbox_id__c NOT IN ('9987800100888', '999999999999999910', '99999999999987')
AND (review_complete_time__c is not null
    OR auw_pre_doc_review_complete_time__c is not null)
AND createddate >= '2024-8-1')
) a

LEFT JOIN bi.public.daily_approved_customers_data dacd
ON a.fbbid=dacd.fbbid 

LEFT JOIN bi.public.customers_data cd
ON a.fbbid = cd.fbbid

LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 f
ON a.fbbid = f.fbbid
AND dacd.edate=f.edate

-- Loan level information
LEFT JOIN
(SELECT *
FROM bi.finance.finance_metrics_daily
WHERE product_type <> 'Flexpay'
AND loan_operational_status <> 'CNCL'
AND loan_created_date>'2022-01-01') fmd
    ON a.fbbid = fmd.fbbid
    AND dacd.edate = fmd.edate
    AND (a.review_complete_time__c::date<=fmd.loan_created_date or a.auw_pre_doc_review_complete_time__c::date<=fmd.loan_created_date)

-- Next few joins are same as agent performance table
LEFT JOIN analytics.credit.second_look_accounts sl
ON a.fbbid=sl.fbbid


LEFT JOIN (SELECT fbbid, partner_name, calculated_annual_revenue AS calculated_annual_revenue_pa, pre_approval_amount AS automated_cl_pa, auw_approved_limit AS auw_approved_limit_pa
FROM bi.customers.leads_data) l
ON l.fbbid=a.fbbid
AND a.recordtypeid ='012Rd000000jbbJIAQ'

LEFT JOIN (select bff.fbbid as leads_flag_fbbid
from cdc_v2.feature_flags.business_feature_flag_preferences bff
    join cdc_v2.feature_flags.feature_flags ff
        on ff.id = bff.feature_flag_id
            and ff.id = 375
and bff.value = TRUE) ld
ON a.fbbid=ld.leads_flag_fbbid

LEFT JOIN (select fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t1 -- Day before review completed, for CL prior to decision
    ON a.fbbid = t1.fbbid
    AND a.review_complete_time__c::date = DATEADD(day, 1, t1.edate)
    AND A.recordtypeid in ('012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS') -- OG AUW, HVC, AUW monitoring
    
LEFT JOIN (select fbbid, credit_limit, edate FROM bi.public.daily_approved_customers_data) t2 -- Day after review completed, for CL after decision
    ON a.fbbid = t2.fbbid
    AND a.review_complete_time__c::date = DATEADD(day, -1, t2.edate)
    AND A.recordtypeid IN ('012Rd000001B2txIAC', '012Rd000002EppVIAS','012Rd000002Dp5CIAS') -- OG AUW, HVC, AUW monitoring

WHERE (recordtypeid in ('012Rd000000AcjxIAC','012Rd000000jbbJIAQ', '0124T000000DSMTQA4') 
        OR (recordtypeid in ('012Rd000001B2txIAC', '012Rd000002EppVIAS') and decision_type<>'No change')) 
AND a.fbbid NOT IN ('9987800100888', '999999999999999910')
AND (dob IS NOT NULL and dob>=0)
AND decision_type not in ('Rejected', 'Close/RMR/Disable')
AND rnk=1 -- we filter out all entries where rank is not 1 so that the table only includes information for the latest review 
)



;



--- Customer level table: Aggregates above table up to the customer level

create or replace table analytics.credit.manual_uw_activity_metrics_v1 as 
select review_complete_date_coalesce
     , r.first_approved_time::date first_approved_time
     , start_date
     , createddate
     , underwriter__c
     , approver__c
     , approver_name
     , recordtypeid
     , decision_type
     , dob
     , r.edate
     , r.fbbid
     , r.partner
     , ob_bucket_group_retro
     , og_bucket_group
     , rev_band
     , leads_flag
     -- , ()max(dpd_days_corrected) dpd_days
     -- , ()count(distinct case when dpd_days_corrected>0 then loan_key else null end) dq_draws
     , max(approved_uw_credit_limit) as mu_approved_limit
     , max(credit_limit) credit_limit
     , sum(r.outstanding_principal_due) outstanding_principal
     -- , ()sum(r.revenue) revenue
     , sum(origination_at_date) originated_amount
     -- , ()max(udr35) udr35
     -- , ()sum(case when is_charged_off>0 then 1 else 0 end) is_charged_off
     , max(activation_flag) ftd
     , max(r.activation_amount) fda
from analytics.credit.manual_uw_activity_metrics_v0 r
left join bi.public.daily_approved_customers_data dacd
    on r.fbbid = dacd.fbbid
    and r.edate = dacd.edate 
WHERE dacd.fbbid NOT IN ('9987800100888', '999999999999999910', '99999999999987')
group by ALL
order by fbbid, edate, dob
;

Select * from analytics.credit.manual_uw_activity_metrics_v1

---------------------------------------------------------------- ROLL RATES ---------------------------------------------------------------- 
-- select * from analytics.credit.manual_uw_rr limit 100;
-- select count(distinct fbbid) FROM analytics.credit.manual_uw_agent_v1 ;

CREATE OR REPLACE VIEW analytics.credit.manual_uw_rr AS
SELECT a.FBBID
     , l.loan_key
     , week_end_date
     , a.UNDERWRITER__C
     , a.recordtypeid
     , review_complete_date_coalesce
     , a.partner
     , a.rev_band
     , a.ob_bucket_group_retro
     , a.leads_flag
     , a.createddate
     , a.decision_type
    ------------
     , CAST(os_0 AS INTEGER) AS os_0
     , CAST(os_1_7 AS INTEGER) AS os_1_7
     , CAST(os_p_0 AS INTEGER) AS os_p_0
     , CAST(os_p_1_7 AS INTEGER) AS os_p_1_7
     , CAST(os_8_14 AS INTEGER) AS os_8_14
     , CAST(os_p_8_14 AS INTEGER) AS os_p_8_14
     , CAST(os_15_21 AS INTEGER) AS os_15_21
     , CAST(os_p_15_21 AS INTEGER) AS os_p_15_21
     , CAST(os_22_28 AS INTEGER) AS os_22_28
     , CAST(os_p_22_28 AS INTEGER) AS os_p_22_28
     , CAST(os_29_35 AS INTEGER) AS os_29_35
     , CAST(os_p_29_35 AS INTEGER) AS os_p_29_35
     , CAST(os_36_42 AS INTEGER) AS os_36_42
     , CAST(os_p_36_42 AS INTEGER) AS os_p_36_42
     , CAST(os_43_49 AS INTEGER) AS os_43_49
     , CAST(os_p_43_49 AS INTEGER) AS os_p_43_49
     , CAST(os_50_56 AS INTEGER) AS os_50_56
     , CAST(os_p_50_56 AS INTEGER) AS os_p_50_56
     , CAST(os_57_63 AS INTEGER) AS os_57_63
     , CAST(os_64_70 AS INTEGER) AS os_64_70
     , CAST(os_p_57_63 AS INTEGER) AS os_p_57_63
     , CAST(os_71_77 AS INTEGER) AS os_71_77
     , CAST(os_p_64_70 AS INTEGER) AS os_p_64_70
     , CAST(os_78_84 AS INTEGER) AS os_78_84
     , CAST(os_p_71_77 AS INTEGER) AS os_p_71_77
     , CAST(os_85_90 AS INTEGER) AS os_85_90
     , CAST(os_85_91 AS INTEGER) AS os_85_91
     , CAST(os_p_78_84 AS INTEGER) AS os_p_78_84
     , CAST(os_15_35 AS INTEGER) AS os_15_35
     , CAST(os_36_63 AS INTEGER) AS os_36_63
     , CAST(os_64_90 AS INTEGER) AS os_64_90
     , CAST(os_p_84_90 AS INTEGER) AS os_p_84_90
     , CAST(os_p_85_90 AS INTEGER) AS os_p_85_90
     , CAST(os_p_64_90 AS INTEGER) AS os_p_64_90
     , CAST(os_91_from_64_90 AS INTEGER) AS os_91_from_64_90
     , CAST(os_91_from_85_90 AS INTEGER) AS os_91_from_85_90
     , CAST(os_91 AS INTEGER) AS os_91
     , CAST(os_91_new AS INTEGER) AS os_91_new
     , CAST(os_0_90 AS INTEGER) AS os_0_90
     , CAST(os_1_90 AS INTEGER) AS os_1_90
     , CAST(os_p_0_90 AS INTEGER) AS os_p_0_90
    --
    --
    , f_0
    , f_p_0
    , f_1_7
    , f_p_1_7
    , f_8_14
    , f_p_8_14
    , f_15_21
    , f_p_15_21
    , f_22_28
    , f_p_22_28
    , f_29_35
    , f_p_29_35
    , f_36_42
    , f_p_36_42
    , f_43_49
    , f_p_43_49
    , f_50_56
    , f_p_50_56
    , f_57_63
    , f_p_57_63
    , f_64_70
    , f_p_64_70
    , f_71_77
    , f_p_71_77
    , f_78_84
    , f_p_78_84
    , f_85_90
    , f_85_91
    , f_15_35
    , f_36_63
    , f_64_90
    , f_p_85_90
    , f_P_64_90
    , f_91_from_64_90
    , f_91_from_85_90
    , f_p_84_90
    , f_91
    , f_91_new
    , f_0_90
    , f_1_90
FROM analytics.credit.manual_uw_agent_v1 a
LEFT JOIN analytics.credit.loan_level_data_pb l
    ON a.fbbid = l.fbbid
    AND a.review_complete_date_coalesce<l.week_end_date
WHERE decision_type not in ('Rejected', 'Close/RMR/Disable') AND a.fbbid NOT IN ('9987800100888', '999999999999999910')
-- GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 1,2,3;


CREATE OR REPLACE TABLE analytics.credit.final_auw_metrics_vm AS
WITH og_auw_updated_tag AS (
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
        AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
    AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
    AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
      AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
      AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
    AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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
AND FBBID NOT IN ('9987800100888', '999999999999999910')
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
      + SUM(CASE WHEN tag1 = 'OB AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
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
      + SUM(CASE WHEN tag1 = 'OG AUW' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
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
      + SUM(CASE WHEN tag1 = 'Second Look' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
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
      + SUM(CASE WHEN tag1 = 'HVC' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
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
      + SUM(CASE WHEN tag1 = 'Pre-Approval' AND is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
      AS pre_approvals_net_yield,
    SUM(CASE WHEN tag1 = 'Pre-Approval' and loan_created_date between week_start_date and week_end_date THEN originated_amount ELSE 0 END) AS pre_approvals_originations,


    SUM(CASE WHEN is_charged_off = 0 THEN outstanding_principal_due ELSE 0 END) AS total_os_all,
    SUM(CASE WHEN is_charged_off = 0 AND dpd_days_corrected BETWEEN 1 AND 90 THEN outstanding_principal_due ELSE 0 END) AS os_1_90_dpd_all,
    SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_co_all,
    SUM(fees_paid * 52) AS revenue_all,
    SUM(fees_paid) AS revenue_all_NOT_ANNUAL,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END) AS gross_yield_all,
    SUM(fees_paid * 52) - SUM(CASE WHEN charge_off_date BETWEEN week_start_date AND week_end_date THEN outstanding_principal_due * 52 ELSE 0 END)
    + SUM(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END)
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
  AND FUNDBOX_ID__C NOT IN ('9987800100888', '999999999999999910')
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



---------------- Custom SQL query for Tableau ------------------------------------------------

SELECT
    WEEK_END_DATE,
    'OB AUW' AS Program,
    OB_AUW_TOTAL_OS AS TOTAL_OS,
    OB_AUW_OS_1_90_DPD AS OS_1_90_DPD,
    OB_AUW_GROSS_CO AS GROSS_CO,
    OB_AUW_REVENUE AS REVENUE,
    OB_AUW_NET_YIELD AS NET_YIELD,
    OB_AUW_ORIGINATIONS AS ORIGINATIONS,
    OB_SLA_HRS AS SLA_HRS,
    OB_COUNT AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm

UNION ALL

-- Process data for the 'OG AUW' program
SELECT
    WEEK_END_DATE,
    'OG AUW' AS Program,
    OG_AUW_TOTAL_OS AS TOTAL_OS,
    OG_AUW_OS_1_90_DPD AS OS_1_90_DPD,
    OG_AUW_GROSS_CO AS GROSS_CO,
    OG_AUW_REVENUE AS REVENUE,
    OG_AUW_NET_YIELD AS NET_YIELD,
    OG_AUW_ORIGINATIONS AS ORIGINATIONS,
    OG_SLA_HRS AS SLA_HRS,
    OG_COUNT AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm

UNION ALL

-- Process data for the 'Pre approval' program
SELECT
    WEEK_END_DATE,
    'Pre approval' AS Program,
    PRE_APPROVALS_TOTAL_OS AS TOTAL_OS,
    PRE_APPROVALS_OS_1_90_DPD AS OS_1_90_DPD,
    PRE_APPROVALS_GROSS_CO AS GROSS_CO,
    PRE_APPROVALS_REVENUE AS REVENUE,
    PRE_APPROVALS_NET_YIELD AS NET_YIELD,
    PRE_APPROVALS_ORIGINATIONS AS ORIGINATIONS,
    PRE_APP_SLA_HRS AS SLA_HRS,
    PRE_APP_COUNT AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm

UNION ALL

-- Process data for the 'HVC' program
SELECT
    WEEK_END_DATE,
    'HVC' AS Program,
    HVC_TOTAL_OS AS TOTAL_OS,
    HVC_OS_1_90_DPD AS OS_1_90_DPD,
    HVC_GROSS_CO AS GROSS_CO,
    HVC_REVENUE AS REVENUE,
    HVC_NET_YIELD AS NET_YIELD,
    HVC_ORIGINATIONS AS ORIGINATIONS,
    NULL AS SLA_HRS,
    NULL AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm

UNION ALL

-- Process data for the 'SL' program
SELECT
    WEEK_END_DATE,
    'SL' AS Program,
    SL_TOTAL_OS AS TOTAL_OS,
    SL_OS_1_90_DPD AS OS_1_90_DPD,
    SL_GROSS_CO AS GROSS_CO,
    SL_REVENUE AS REVENUE,
    SL_NET_YIELD AS NET_YIELD,
    SL_ORIGINATIONS AS ORIGINATIONS,
    SL_SLA_HRS AS SLA_HRS,
    SL_COUNT AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm

UNION ALL

-- Process data for ALL programs
SELECT
    WEEK_END_DATE,
    'All' AS Program,
    TOTAL_OS_ALL AS TOTAL_OS,
    OS_1_90_DPD_ALL AS OS_1_90_DPD,
    GROSS_CO_ALL AS GROSS_CO,
    REVENUE_ALL AS REVENUE,
    NET_YIELD_ALL AS NET_YIELD,
    ORIGINATIONS_ALL AS ORIGINATIONS,
    SUM_SLA_HRS AS SLA_HRS,
    FILES_REVIEWED AS FILES_REVIEWED
FROM
    analytics.credit.final_auw_metrics_vm
    
ORDER BY
    WEEK_END_DATE, Program




    Select top 10* from analytics.credit.KM_HEADLINE_OKR