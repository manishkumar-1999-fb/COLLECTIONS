CREATE OR REPLACE VIEW INDUS.PUBLIC.Autobooks_funnel_structure AS
(
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

        , risk_bucket_table AS (
        SELECT 'No Bucket' risk_bucket UNION SELECT '1-4' risk_bucket UNION SELECT '5-7' risk_bucket UNION SELECT '8-10' risk_bucket UNION SELECT '11-12' risk_bucket UNION SELECT '13+' risk_bucket
        )

        , partner_name_table AS (
        SELECT 'Autobooks' partner_name
        )

        , industry_type_table AS (
        SELECT 'Construction' industry_type UNION select 'ASWR' industry_type UNION select 'Real Estate, Rental & Hospitality' industry_type UNION select 'Professional Services' industry_type UNION select 'Retail & Wholesale Trade' industry_type UNION select 'Transportation & Warehousing' industry_type UNION select 'Others/No Data' industry_type
        )

        , customer_revenue_table AS (
        SELECT '$0 - $500K' customer_revenue UNION SELECT '$500K - $1.5M' customer_revenue UNION SELECT '> $1.5M' customer_revenue UNION SELECT 'Other/No Data' customer_revenue
        )


        SELECT a.week_end_date
        , rbt.risk_bucket bucket_group
        , pt.PARTNER_NAME
        , it.industry_type
        , crt.customer_revenue

        FROM a

        CROSS JOIN risk_bucket_table rbt
        CROSS JOIN PARTNER_NAME_TABLE pt
        CROSS JOIN INDUSTRY_TYPE_TABLE it
        CROSS JOIN CUSTOMER_REVENUE_TABLE crt

        UNION

        SELECT b.week_end_date
        , rbt.risk_bucket bucket_group
        , pt.PARTNER_NAME
        , it.industry_type
        , crt.customer_revenue

        FROM b

        CROSS JOIN risk_bucket_table rbt
        CROSS JOIN PARTNER_NAME_TABLE pt
        CROSS JOIN INDUSTRY_TYPE_TABLE it
        CROSS JOIN CUSTOMER_REVENUE_TABLE crt

        ORDER BY 1 DESC,2)
        ;

        --select * from INDUS.PUBLIC.onboarding_autobooks_customers where registration_date>='2025-06-4' and registration_date<='2025-06-11';
------ 1. Leads Data on Autobooks Customers
CREATE OR REPLACE TABLE INDUS.PUBLIC.leads_autobooks_customers_AR AS (
SELECT  fbbid
      , partner_name
      , annual_revenue
      , calculated_annual_revenue
      , initial_lead_submission_timestamp
      , prequal_timestamp
      , lead_sunset_timestamp
      , in_prequal_api
      , prequal_decision
      , pre_approval_amount
      , fico_score
      , channel_type
       , /*FINAL_DECISION, FIRST_DRAW_AMOUNT,*/
FROM bi.CUSTOMERS.LEADS_DATA
WHERE partner_name = 'Autobooks');

     --   drop table INDUS.PUBLIC.onboarding_autobooks_customers;

------ 2. Registration Data on Autobooks Customers
CREATE OR REPLACE TABLE INDUS.PUBLIC.onboarding_autobooks_customers_AR AS
(
SELECT cd.fbbid
     , first_approved_time::date first_approved_date
     , registration_time::date registration_date
     , cip_connected_time::date cip_connected_date
     , first_connected_time::date first_connected_date
     , registration_flow_completed_time::date underwritten_date
     , fico_onboarding
     , first_draw_time::date first_draw_date
     , first_approved_credit_limit
     , first_draw_amount
     , is_approved
     , is_registration_flow_completed
     , cd.first_rejected_time
     , CASE WHEN is_underwriting = 1 THEN 1
            WHEN first_rejected_reason in ('Onboarding dynamic decision reject') then 1
            ELSE 0
            END AS is_underwriting_new
     , f.fico
     , f.customer_annual_revenue_group
     , f.industry_type
     , f.partner
     , f.ob_bucket_group_retro
     , f.termunits
     , FRD.fraud_review
     , FRD.fraud_time::DATE AS FRAUD_DATE
     , FEB.REJECTION_REASON
FROM bi.PUBLIC.CUSTOMERS_DATA cd

LEFT JOIN (
    select distinct FUNDBOX_ID__C AS FBBID, 1 as fraud_review,
    case when fraud_review_completed_date_time__c is null then lastmodifieddate else fraud_review_completed_date_time__c end as fraud_time
from EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.FRAUD_REVIEW__C
where recordtypeid='0124T000000HN0QQAW'
) FRD
ON CD.FBBID = FRD.FBBID

LEFT JOIN
(SELECT fbbid, rejection_reason
FROM
indus.public.feb_report) FEB
ON CD.fbbid = FEB.fbbid

LEFT JOIN (SELECT fbbid
                , termunits
                , ob_bucket_group_retro
                , partner
                , industry_type
                , customer_annual_revenue_group
                , fico
                , ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate) rnk
    FROM indus.PUBLIC.indus_key_metrics_filters_v2
        QUALIFY rnk=1) f
    ON cd.fbbid=f.fbbid
    WHERE
     registration_campaign_source = 'autobooks'
    -- AND registration_time >= DATE '2025-06-03'
    and is_test=0
)
;

------ 3. Leads and Acquisitions Agg
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.autobooks_leads_acq_agg_AR AS
(
WITH autobooks_data AS
(SELECT a.fbbid
     , a.partner_name
     , a.annual_revenue
     , a.calculated_annual_revenue
     ,  CASE WHEN calculated_annual_revenue>0 and calculated_annual_revenue<=500000 THEN '$0 - $500K'
             WHEN calculated_annual_revenue>500000 and calculated_annual_revenue<=1500000 THEN '$500K - $1.5M'
             WHEN calculated_annual_revenue>1500000 THEN '> $1.5M'
             --WHEN calculated_annual_revenue>1000000 THEN '> $1M'
             ELSE 'Other/No Data'
           END AS calculated_annual_revenue_group
     , a.fico_score
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', initial_lead_submission_timestamp::date+4)::date+2
                    WHEN datediff('day', initial_lead_submission_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    initial_lead_submission_timestamp::date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', initial_lead_submission_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', initial_lead_submission_timestamp::date+4)::date+2
                    END initial_lead_submission_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', prequal_timestamp::date+4)::date+2
                    WHEN datediff('day', prequal_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    prequal_timestamp::date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', prequal_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', prequal_timestamp::date+4)::date+2
                    END prequal_week_end_date
     , a.in_prequal_api
     , a.prequal_decision
     , a.pre_approval_amount
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', lead_sunset_timestamp::date+4)::date+2
                    WHEN datediff('day', lead_sunset_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    lead_sunset_timestamp::date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', lead_sunset_timestamp::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', lead_sunset_timestamp::date+4)::date+2
                    END lead_sunset_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_date+4)::date+2
                    WHEN datediff('day', registration_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    registration_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', registration_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', registration_date+4)::date+2
                    END registration_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', cip_connected_date+4)::date+2
                    WHEN datediff('day', cip_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    cip_connected_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', cip_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', cip_connected_date+4)::date+2
                    END cip_connected_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_connected_date+4)::date+2
                    WHEN datediff('day', first_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    first_connected_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', first_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', first_connected_date+4)::date+2
                    END first_connected_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', underwritten_date+4)::date+2
                    WHEN datediff('day', underwritten_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    underwritten_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', underwritten_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', underwritten_date+4)::date+2
                    END underwritten_week_end_date // Reconfirm logic on this
     , is_underwriting_new
     , is_registration_flow_completed
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_date+4)::date+2
                    WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    first_approved_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', first_approved_date+4)::date+2
                    END first_approved_week_end_date
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', FRAUD_DATE::date+4)::date+2
                            WHEN datediff('day', FRAUD_DATE::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', FRAUD_DATE::date,
                              current_date()) <= 0 THEN NULL
                            WHEN datediff('day', FRAUD_DATE::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                            ELSE DATE_TRUNC('WEEK', FRAUD_DATE::date+4)::date+2
                          END fraud_week_end_date
     , is_approved
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_date+4)::date+2
                    WHEN datediff('day', first_draw_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day',
                    first_draw_date, current_date()) <= 0 THEN NULL
                    WHEN datediff('day', first_draw_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', first_draw_date+4)::date+2
                    END first_draw_week_end_date
     , FRAUD_REVIEW
     , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', b.first_rejected_time::date+4)::date+2
                WHEN datediff('day', b.first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', b.first_rejected_time::date,
                current_date()) <= 0 THEN NULL
                WHEN datediff('day', b.first_rejected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                ELSE DATE_TRUNC('WEEK', b.first_rejected_time::date+4)::date+2
                END dec_weekend_date
     , CASE WHEN rejection_reason='FICO Sub 600' THEN B.FBBID ELSE NULL END as fico_599_rejections1
     , b.first_draw_amount
     , b.first_approved_credit_limit
     , b.fico_onboarding
     , fico
     , CASE WHEN fico_score < 600 THEN '<600'
            WHEN fico_score BETWEEN 600 and 650 THEN '600-650'
            WHEN fico_score BETWEEN 650 and 700 THEN '650-700'
            WHEN fico_score BETWEEN 700 and 750 THEN '700-750'
            WHEN fico_score BETWEEN 750 and 800 THEN '750-800'
            WHEN fico_score > 800 THEN '>800'
            ELSE null END fico_band_leads
     , customer_annual_revenue_group
     , CASE WHEN industry_type IS NULL THEN 'Others' ELSE industry_type end industry_type
     , CASE WHEN ob_bucket_group_retro='OB: 1-4' THEN '1-4'
            WHEN ob_bucket_group_retro='OB: 5-7' THEN '5-7'
            WHEN ob_bucket_group_retro='OB: 8-10' THEN '8-10'
            WHEN ob_bucket_group_retro='OB: 11-12' THEN '11-12'
            WHEN ob_bucket_group_retro='OB: 13+' THEN '13+'
            ELSE 'No Bucket'
            END ob_bucket_group
     , 'Platforms' as termunits // since Yosi said Autobooks would be a Platforms partner
     , dacd.credit_limit as first_draw_credit_limit
     , CASE WHEN industry_type IS NULL OR industry_type='Others' then 'Others/No Data' ELSE industry_type END industry_type_filled
     , CASE WHEN ob_bucket_group IS NULL then 'No Bucket' ELSE ob_bucket_group END ob_bucket_group_filled

     , CASE WHEN datediff('day',first_approved_date,first_draw_date) <= 7 THEN 1 ELSE 0 END AS is_ftd_0_7
     , CASE WHEN datediff('day',first_approved_date,first_draw_date) = 0 THEN 1 ELSE 0 END AS is_ftd_same_day

     , CASE WHEN datediff('day',prequal_timestamp::date,registration_date) <= 7 THEN 1 ELSE 0 END AS is_reg_0_7
     , CASE WHEN datediff('day',prequal_timestamp::date,registration_date) = 0 THEN 1 ELSE 0 END AS is_reg_same_day
     , datediff('day',prequal_timestamp::date,registration_date) AS prequal_reg_time


FROM INDUS.PUBLIC.leads_autobooks_customers_AR a
    LEFT JOIN INDUS.PUBLIC.onboarding_autobooks_customers_AR b
        ON a.fbbid=b.fbbid
    LEFT JOIN BI.public.daily_approved_customers_data dacd
        ON a.fbbid=dacd.fbbid
        AND b.first_draw_date=dacd.edate::date
    )

, overall_underwritten_revenue AS (
    SELECT
        underwritten_week_end_date AS week_end_date,
        AVG(calculated_annual_revenue) AS overall_avg_underwritten_revenue,
        MEDIAN(calculated_annual_revenue) AS overall_median_underwritten_revenue
    FROM autobooks_data
    WHERE is_underwriting_new = 1
    GROUP BY 1
)

, overall_approved_revenue AS (
    SELECT
        first_approved_week_end_date AS week_end_date,
        AVG(calculated_annual_revenue) AS overall_avg_approved_revenue,
        MEDIAN(calculated_annual_revenue) AS overall_median_approved_revenue
    FROM autobooks_data
    WHERE is_approved = 1
    GROUP BY 1
)

SELECT A.week_end_date
    , 1 as ONE 
     , A.bucket_group
     , A.customer_revenue
     , A.industry_type
     , A.partner_name
     , HOR.leads_hor
     , HOR.prequal_hor
     , HOR.registered_hor
     , HOR.same_reg_hor
     , HOR.reg_0_7_hor
     , VERT.regs
     , VERT.prequal_reg_time
     , CIP.cip
     , FLOW.flow_completed
     , UND.underwritten
     , UND.und_fico
     , UND.underwritten_not_null
     , UND.avg_underwritten_revenue -- Average underwritten revenue per split
     , UND.median_underwritten_revenue -- Median underwritten revenue per split
     , OUND.overall_avg_underwritten_revenue -- **Overall average underwritten revenue**
     , OUND.overall_median_underwritten_revenue -- **Overall median underwritten revenue**
     , APP.approved
     , APP.app_fico
     , APP.first_approved_advance_amt
     , APP.ftd_0_7
     , APP.avg_approved_revenue -- Average approved revenue per split
     , APP.approved_not_null 
     , APP.median_approved_revenue -- Median approved revenue per split
     , OAPP.overall_avg_approved_revenue -- **Overall average approved revenue**
     , OAPP.overall_median_approved_revenue -- **Overall median approved revenue**
     , FTD.num_ftd
     , FTD.ftd_amt
     , FTD.ftd_cl
     , fico_599_rejections
     , fraud_cases

FROM INDUS.PUBLIC.Autobooks_funnel_structure A

---------------- PA Horizontal metrics
LEFT JOIN
    (SELECT initial_lead_submission_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) leads_hor
          , COUNT(DISTINCT CASE WHEN prequal_decision='approved' THEN fbbid ELSE NULL END) prequal_hor
          , COUNT(DISTINCT CASE WHEN registration_week_end_date IS NOT NULL THEN fbbid ELSE NULL END) registered_hor
          , COUNT(DISTINCT CASE WHEN is_reg_same_day=1 THEN fbbid ELSE NULL END) same_reg_hor
          , COUNT(DISTINCT CASE WHEN is_reg_0_7=1 THEN fbbid ELSE NULL END) reg_0_7_hor
    FROM autobooks_data
    GROUP BY all) HOR
ON A.week_end_date=HOR.initial_lead_submission_week_end_date
AND A.bucket_group=HOR.ob_bucket_group_filled
AND A.partner_name=HOR.partner_name
AND A.industry_type=HOR.industry_type_filled
AND A.customer_revenue=HOR.calculated_annual_revenue_group

---------------- PA Vertical metrics
LEFT JOIN
(SELECT registration_week_end_date //confirm
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) regs
          , SUM(prequal_reg_time) prequal_reg_time
    FROM autobooks_data
    -- WHERE prequal_week_end_date IS NOT NULL
    -- WHERE prequal_decision='approved'
    -- AND registration_week_end_date IS NOT NULL
    GROUP BY all) VERT
ON A.week_end_date=VERT.registration_week_end_date
AND A.bucket_group=VERT.ob_bucket_group_filled
AND A.partner_name=VERT.partner_name
AND A.industry_type=VERT.industry_type_filled
AND A.customer_revenue=VERT.calculated_annual_revenue_group

---------------------------------------------------------------- ONBOARDING ----------------------------------------------------------------
---------------- CIP
LEFT JOIN
(SELECT cip_connected_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) cip
    FROM autobooks_data
    -- WHERE prequal_week_end_date IS NOT NULL
    -- WHERE prequal_decision='approved'
    -- AND registration_week_end_date IS NOT NULL
    GROUP BY all) CIP
ON A.week_end_date=CIP.cip_connected_week_end_date
AND A.bucket_group=CIP.ob_bucket_group_filled
AND A.partner_name=CIP.partner_name
AND A.industry_type=CIP.industry_type_filled
AND A.customer_revenue=CIP.calculated_annual_revenue_group

---------------- Flow Completed
LEFT JOIN
(SELECT underwritten_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) flow_completed
    FROM autobooks_data
    GROUP BY all) FLOW
ON A.week_end_date=FLOW.underwritten_week_end_date
AND A.bucket_group=FLOW.ob_bucket_group_filled
AND A.partner_name=FLOW.partner_name
AND A.industry_type=FLOW.industry_type_filled
AND A.customer_revenue=FLOW.calculated_annual_revenue_group

---------------- Underwritten
LEFT JOIN
(SELECT underwritten_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) underwritten
          , COUNT(DISTINCT CASE WHEN fico_onboarding IS NOT NULL AND fico_onboarding != 0 THEN fbbid END) AS underwritten_not_null
          , SUM(fico_onboarding) und_fico
          , AVG(calculated_annual_revenue) AS avg_underwritten_revenue -- Added average
          , MEDIAN(calculated_annual_revenue) AS median_underwritten_revenue -- Added median
    FROM autobooks_data
    WHERE is_underwriting_new=1
    GROUP BY all) UND
ON A.week_end_date=UND.underwritten_week_end_date
AND A.bucket_group=UND.ob_bucket_group_filled
AND A.partner_name=UND.partner_name
AND A.industry_type=UND.industry_type_filled
AND A.customer_revenue=UND.calculated_annual_revenue_group

-- Join for overall underwritten revenue
LEFT JOIN overall_underwritten_revenue OUND
ON A.week_end_date = OUND.week_end_date

---------------- Approved
LEFT JOIN
(SELECT first_approved_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) approved
          , COUNT(DISTINCT CASE WHEN fico_onboarding IS NOT NULL AND fico_onboarding != 0 THEN fbbid END) AS approved_not_null
          , SUM(fico_onboarding) app_fico
          , SUM(first_approved_credit_limit) first_approved_advance_amt //Confirm on whether this is the same as First Approved Advance Amount
          , COUNT(DISTINCT CASE WHEN is_ftd_0_7=1 THEN fbbid ELSE NULL END) ftd_0_7
          , AVG(calculated_annual_revenue) AS avg_approved_revenue -- Added average
          , MEDIAN(calculated_annual_revenue) AS median_approved_revenue -- Added median
    FROM autobooks_data
    WHERE is_approved=1
    GROUP BY all) APP
ON A.week_end_date=APP.first_approved_week_end_date
AND A.bucket_group=APP.ob_bucket_group_filled
AND A.partner_name=APP.partner_name
AND A.industry_type=APP.industry_type_filled
AND A.customer_revenue=APP.calculated_annual_revenue_group

-- Join for overall approved revenue
LEFT JOIN overall_approved_revenue OAPP
ON A.week_end_date = OAPP.week_end_date

---------------- FTD
LEFT JOIN
(SELECT first_draw_week_end_date
          , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) num_ftd
          , SUM(first_draw_amount) ftd_amt
          , SUM(first_draw_credit_limit) ftd_cl
    FROM autobooks_data
    WHERE first_draw_week_end_date IS NOT NULL
    GROUP BY all) FTD
ON A.week_end_date=FTD.first_draw_week_end_date
AND A.bucket_group=FTD.ob_bucket_group_filled
AND A.partner_name=FTD.partner_name
AND A.industry_type=FTD.industry_type_filled
AND A.customer_revenue=FTD.calculated_annual_revenue_group

LEFT JOIN ------------  REJECTIONS
      (SELECT dec_weekend_date
            , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
            , COUNT(DISTINCT fico_599_rejections1) as fico_599_rejections

            FROM autobooks_data
            WHERE is_registration_flow_completed=1
          GROUP by 1,2,3,4,5
            order by 1,2,3,4,5) D
          ON A.week_end_date=D.dec_weekend_date
          AND A.bucket_group=D.ob_bucket_group_filled
          AND A.partner_name=D.partner_name
          AND A.industry_type=D.industry_type_filled
          AND A.customer_revenue=D.calculated_annual_revenue_group

LEFT JOIN ------------  FRAUD
      (SELECT fraud_week_end_date
            , partner_name
          -- , fico_band_leads
          , calculated_annual_revenue_group
          , industry_type_filled
          , ob_bucket_group_filled
          , COUNT(DISTINCT fbbid) as fraud_cases

            FROM autobooks_data
            WHERE fraud_review=1
          GROUP by 1,2,3,4,5
            order by 1,2,3,4,5) E
          ON A.week_end_date=E.fraud_week_end_date
          AND A.bucket_group=E.ob_bucket_group_filled
          AND A.partner_name=E.partner_name
          AND A.industry_type=E.industry_type_filled
          AND A.customer_revenue=E.calculated_annual_revenue_group
)
;

-------------------------------------------------------------------- Agg table metrics --------------------------------------------------------------------
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.autobooks_customer_performance_AR AS
(
WITH cust_metrics AS
    (
        SELECT A.fbbid
     , B.week_end_date
     , 'Autobooks' as partner
     , B.new_cust_filter new_cust_filter_customer
     , B.customer_annual_revenue_group
     , B.customer_annual_revenue_group_ob
     , A.ob_bucket_group_retro
     , B.og_bucket_group
     , CASE WHEN new_cust_filter='New Customer' THEN A.ob_bucket_group_retro
            WHEN new_cust_filter='Existing Customer' THEN B.og_bucket_group END AS risk_bucket
     , B.industry_type
     , B.fico_onboarding
     , A.first_approved_date

     , B.credit_limit exposure_end_of_week

     , (CASE WHEN B.account_status = 'active' AND B.is_chargeoff = 0 AND B.dpd_days_corrected < 98
             THEN 1 ELSE 0 END) AS open_accounts
     , (CASE WHEN B.account_status = 'active' AND B.dpd_days_corrected BETWEEN 0 AND 91 AND B.IS_CHARGED_OFF_FMD = 0 AND B.OUTSTANDING_PRINCIPAL_DUE > 0
             THEN 1 ELSE 0 END) active_accounts
     , (CASE WHEN (B.charge_off_date_fmd IS NULL or B.charge_off_date_fmd > B.week_end_date) and B.account_status <> 'suspended'
        AND B.credit_status = 'approved' AND (B.is_locked_dashboard = 0 OR B.is_locked_dashboard IS NULL or (B.is_locked_dashboard = 1 AND
        B.dashboard_status_change_reason = 'full_utilization'))
         THEN 1 ELSE 0 END) able_to_draw

     , CASE WHEN open_accounts=1 THEN exposure_end_of_week END exposure_open_customers
     , CASE WHEN active_accounts=1 THEN exposure_end_of_week END exposure_active_customers
     , CASE WHEN able_to_draw = 1 THEN exposure_end_of_week ELSE NULL END exposure_atd_customers

    FROM (SELECT * FROM INDUS.PUBLIC.onboarding_autobooks_customers_AR WHERE first_approved_date IS NOT NULL AND is_approved=1) A
    LEFT JOIN ANALYTICS.CREDIT.customer_level_data_td B
    ON A.fbbid=B.fbbid
    ),

loan_metrics AS
    (
       SELECT A.fbbid
         , B.loan_key
         , B.week_end_date
         , B.new_cust_filter new_cust_filter_loan
         , B.industry_type
         , B.fico
         , A.ob_bucket_group_retro
         , B.BUCKET_GROUP
         , CASE WHEN os_1_7>0 THEN b.loan_key ELSE NULL END f_1_7 // # Total DPD 1-7 Accounts
         , CASE WHEN os_p_0>0 AND os_1_7>0 THEN b.loan_key ELSE NULL END f_1_7_new // # New DPD 1-7 Accounts
         , os_1_7 //$ OS in DPD 1-7
         , outstanding_principal_due -- how is this different from os_0_90
         , os_0_90
         , f_0_90
         , CASE WHEN os_p_0>0 AND os_1_7>0 THEN os_1_7 ELSE NULL END os_1_7_new // $ New OS in DPD 1-7
         , f_1_90 // # Total DPD 1-90 Accounts
         , os_1_90 // $ OS in DPD 1-90
         , os_p_0 // $ OS DPD 0 Last Week (RR denom)
         , CASE WHEN charge_off_date BETWEEN week_start_date
                AND week_end_date THEN b.loan_key ELSE NULL END new_co_accts // New CO Accounts
         , os_91_new // $ New OS in CO
         , CASE WHEN loan_created_date BETWEEN week_start_date AND week_end_date THEN originated_amount ELSE NULL END originations


        FROM (SELECT * FROM INDUS.PUBLIC.onboarding_autobooks_customers_AR WHERE first_approved_date IS NOT NULL AND is_approved=1) A
        LEFT JOIN ANALYTICS.CREDIT.loan_level_data_pb B
        ON A.fbbid=B.fbbid
        WHERE loan_key IS NOT NULL
    )

SELECT C.week_end_date
     , C.new_cust_filter_customer
     , C.risk_bucket
     , C.industry_type
     , open_accts
     , active_accts
     , atd_accts
     , open_exp
     , active_exp
     , atd_exp
     , os_dpd_1_7
     --, os_p_0
     , os_dpd_0_90
     , os_dpd_0_old
     , os_dpd_1_7_new
     , os_dpd_1_90
     , os_co_new
     , originated_amt
     , f_dpd_1_7
     , f_dpd_1_7_new
     , f_dpd_1_90
     , f_dpd_0_90
     , co_new

FROM

    (
    SELECT week_end_date
         , new_cust_filter_customer
         , risk_bucket
         , industry_type
         , SUM(open_accounts) open_accts
         , SUM(active_accounts) active_accts
         , SUM(able_to_draw) atd_accts
         , SUM(exposure_open_customers) open_exp
         , SUM(exposure_active_customers) active_exp
         , SUM(exposure_atd_customers) atd_exp
        FROM cust_metrics
        GROUP BY ALL
    ) C

LEFT JOIN
    (
        SELECT a.week_end_date
             , new_cust_filter_loan
             , bucket_group
             , industry_type
             , SUM(os_1_7) os_dpd_1_7
             , SUM(CASE WHEN active_accounts=1 THEN os_0_90 ELSE 0 END) os_dpd_0_90
             , SUM(os_p_0) os_dpd_0_old
             , SUM(os_1_7_new) os_dpd_1_7_new
             , SUM(os_1_90) os_dpd_1_90
             , SUM(os_91_new) os_co_new
             , SUM(originations) originated_amt
             , COUNT(DISTINCT f_1_7) f_dpd_1_7
             , COUNT(DISTINCT f_1_7_new) f_dpd_1_7_new
             , COUNT(DISTINCT f_1_90) f_dpd_1_90
             , COUNT(DISTINCT CASE WHEN active_accounts=1 THEN f_0_90 ELSE 0 END) f_dpd_0_90
             , COUNT(DISTINCT new_co_accts) co_new

           FROM loan_metrics a
           LEFT JOIN (SELECT fbbid, week_end_date, active_accounts FROM cust_metrics) b
               ON a.fbbid=b.fbbid
               AND a.week_end_date=b.week_end_date
           GROUP BY ALL
    ) L1
    ON C.week_end_date=L1.week_end_date
    AND C.new_cust_filter_customer=L1.new_cust_filter_loan
    AND C.risk_bucket=L1.bucket_group
    AND C.industry_type=L1.industry_type

ORDER BY 1,2,3,4
);

