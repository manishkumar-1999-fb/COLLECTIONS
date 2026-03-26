--------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------FRAUD_INDUS----------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------
-------------------------last updated on 22nd may -----------------

-- Added FPD table
CREATE OR REPLACE TABLE ANALYTICS.CREDIT.FIRST_PAYMENT_DEFAULT_METRICS_CALCULATED AS
WITH ALL_TRANSMITTED_PMTS_IN_LOAN AS
(
SELECT ROW_NUMBER() OVER (PARTITION  BY psd.loan_key ORDER BY PAYMENT_EVENT_TIME) AS RN
, psd.LOAN_KEY
, PAYMENT_ID
, dpp.PAYMENT_PLAN_ID
, dpp.IS_ORIGINAL_PLAN 
, PAYMENT_STATUS
, PAYMENT_DESCRIPTION 
, PAYMENT_METHOD_TYPE 
, PAYMENT_EVENT_TIME 
, PAYMENT_TRANSMISSION_DATE
, PAYMENT_TOTAL_AMOUNT  
FROM bi.FINANCE.PAYMENTS_STATUSES_DATA psd
INNER JOIN bi.FINANCE.DIM_PAYMENT_PLAN dpp
ON psd.PAYMENT_PLAN_ID = dpp.PAYMENT_PLAN_ID
INNER JOIN bi.finance.DIM_LOAN dl
ON psd.loan_key = dl.loan_key
AND dl.LOAN_CREATED_TIME::date >= '2024-01-01'
WHERE 1=1
AND DIRECTION = 'D'
AND PAYMENT_METHOD_TYPE = 'ACH'
AND PAYMENT_STATUS = 'TRNS'
)
, FIRST_TRANSMITTED_PMT_IN_LOAN AS 
(
SELECT *
FROM ALL_TRANSMITTED_PMTS_IN_LOAN
WHERE RN = 1
)
, SECOND_TRANSMITTED_PMT_IN_LOAN AS 
(
SELECT *
FROM ALL_TRANSMITTED_PMTS_IN_LOAN
WHERE RN = 2
)
, DELINQUENT_PMTS AS
(
SELECT psd.LOAN_KEY
, PAYMENT_ID
, dpp.PAYMENT_PLAN_ID
, dpp.IS_ORIGINAL_PLAN
, PAYMENT_STATUS
, PAYMENT_DESCRIPTION
, PAYMENT_METHOD_TYPE
, PAYMENT_EVENT_TIME
, PAYMENT_TRANSMISSION_DATE
, PAYMENT_TOTAL_AMOUNT
FROM bi.FINANCE.PAYMENTS_STATUSES_DATA psd
INNER JOIN bi.FINANCE.DIM_PAYMENT_PLAN dpp
ON psd.PAYMENT_PLAN_ID = dpp.PAYMENT_PLAN_ID
INNER JOIN bi.finance.DIM_LOAN dl
ON psd.loan_key = dl.loan_key
AND dl.LOAN_CREATED_TIME::date >= '2024-01-01'
WHERE 1=1
AND DIRECTION = 'D'
AND PAYMENT_METHOD_TYPE = 'ACH'
AND PAYMENT_STATUS IN ('DELQ', 'DLMS') 
)
, CHECK_IF_THERE_ARE_FUND_PMTS_BETWEEN_1ST_AND_2ND AS
(
SELECT PSD.LOAN_KEY
, COUNT(DISTINCT PSD.PAYMENT_ID) AS FUND_PMTS_BETWEEN_BETWEEN_1ST_AND_2ND
FROM bi.FINANCE.PAYMENTS_STATUSES_DATA psd
---------------------------
INNER JOIN FIRST_TRANSMITTED_PMT_IN_LOAN
ON psd.loan_key = FIRST_TRANSMITTED_PMT_IN_LOAN.loan_key
AND PSD.DIRECTION = 'D'
AND PSD.PAYMENT_METHOD_TYPE <> 'ACH'
AND PSD.PAYMENT_STATUS = 'FUND'
AND PSD.PAYMENT_EVENT_TIME >= FIRST_TRANSMITTED_PMT_IN_LOAN.PAYMENT_EVENT_TIME
---------------------------
INNER JOIN SECOND_TRANSMITTED_PMT_IN_LOAN
ON psd.loan_key = SECOND_TRANSMITTED_PMT_IN_LOAN.loan_key
AND PSD.DIRECTION = 'D'
AND PSD.PAYMENT_METHOD_TYPE <> 'ACH'
AND PSD.PAYMENT_STATUS = 'FUND'
AND PSD.PAYMENT_EVENT_TIME <= SECOND_TRANSMITTED_PMT_IN_LOAN.PAYMENT_EVENT_TIME
---------------------------
GROUP BY ALL
HAVING FUND_PMTS_BETWEEN_BETWEEN_1ST_AND_2ND > 0 
)
, CHECK_IF_1ST_TRANSMITTED_IS_DELINQUENT AS
(
SELECT 
/* 
 WE NEEED TO CHEK FOR SCENARIO WHEN THE ORIGINAL FIRST PLANNED PAYMENT IS FOR SOME REASON PRE - PAID IN ADVANCE
 SO THE ORIGINAL FIRST PAYMENT GETS CANCELLED AND THE ORIGINAL SECOND ONE TECHNICALLY BECOMES NOW "THE FIST TRANSMITTED"
 IF THE LATTER FAILS - WE SHOULD NOT (!) COUNT THIS FAULURE AS FIRST DELINQUENT PAYMENT BECAUSE IT ALREADY HAS PRECEDING PAID OCP PAYMENT - USUALLY WITH CREDIT CARD 
 example: 
 https://captain.fbx.im:9000/fundbox_business/1217391/direct_draw/1600670
 **/
 CASE WHEN  EXISTS ( SELECT 1 FROM  bi.FINANCE.PAYMENTS_STATUSES_DATA psd_2
                           WHERE psd_2.loan_key = FIRST_TRANSMITTED_PMT_IN_LOAN.loan_key
                           AND  psd_2.PAYMENT_STATUS = 'FUND' AND psd_2.DIRECTION = 'D'
                           AND psd_2.PAYMENT_EVENT_TIME <= FIRST_TRANSMITTED_PMT_IN_LOAN.PAYMENT_EVENT_TIME
                   )
     THEN 1 ELSE 0 END AS HAS_PRECEDENT_FUNDED_PMT,
----> check if first ntransmitted payment is delinquent     
CASE WHEN DELINQUENT_PMTS.PAYMENT_ID IS NOT NULL THEN 1 ELSE 0 END AS delq_status,

----> check there is at least 1 funded OCP (usually - credit card) payment between the first and the second ACH payments
CASE WHEN CHECK_IF_THERE_ARE_FUND_PMTS_BETWEEN_1ST_AND_2ND.LOAN_KEY IS NOT NULL THEN 1 ELSE 0 END AS FUND_PMTS_BETWEEN_1ST_AND_2ND,

----> final conclusion : only if all conditions are met we will consider such a payment as "first delinquent"      
CASE WHEN ( DELQ_STATUS = 1 AND HAS_PRECEDENT_FUNDED_PMT = 0 AND FUND_PMTS_BETWEEN_1ST_AND_2ND = 0) THEN 1 ELSE 0 END AS  _1ST_TRANSMITTED_IS_DELQ,

FIRST_TRANSMITTED_PMT_IN_LOAN.*
----------------
FROM 
FIRST_TRANSMITTED_PMT_IN_LOAN
LEFT JOIN 
DELINQUENT_PMTS
ON FIRST_TRANSMITTED_PMT_IN_LOAN.PAYMENT_ID = DELINQUENT_PMTS.PAYMENT_ID
LEFT JOIN 
CHECK_IF_THERE_ARE_FUND_PMTS_BETWEEN_1ST_AND_2ND
ON FIRST_TRANSMITTED_PMT_IN_LOAN.LOAN_KEY = CHECK_IF_THERE_ARE_FUND_PMTS_BETWEEN_1ST_AND_2ND.LOAN_KEY
)
, FIRST_LOAN_PER_CUSTOMER AS
(
SELECT 
DL.fbbid, DL.loan_key, loan_id, loan_created_time,PRODUCT_TYPE, ORIGINATED_AMOUNT 
FROM bi.FINANCE.DIM_LOAN DL
INNER JOIN bi.FINANCE.LOAN_STATUSES LST_1
ON DL.LOAN_KEY = LST_1.LOAN_KEY
AND LST_1.STATUS_NAME = 'LOAN_CREDITED_DATE'
AND DL.PRODUCT_TYPE IN ('Direct Draw','Fundbox Pay','Invoice Clearing')
QUALIFY loan_created_time = min(loan_created_time) OVER (PARTITION BY DL.fbbid)
)
, FINAL_TABLE AS
(
SELECT 
CASE WHEN FIRST_LOAN_PER_CUSTOMER.LOAN_KEY IS NOT NULL THEN 1 ELSE 0 END AS IS_1ST_LOAN_PER_CUSTOMER
, CHECK_IF_1ST_TRANSMITTED_IS_DELINQUENT.*
, CASE
    WHEN PAYMENT_TRANSMISSION_DATE = CURRENT_DATE() AND DAYOFWEEK(PAYMENT_TRANSMISSION_DATE) <> 3 THEN NULL
    WHEN DATEDIFF(DAY, PAYMENT_TRANSMISSION_DATE, DATE_TRUNC('WEEK',CURRENT_DATE()+4)::DATE-5) < 0 THEN CURRENT_DATE()-1
    ELSE DATE_TRUNC('WEEK', PAYMENT_TRANSMISSION_DATE::DATE+4)::DATE + 2
END AS PAYMENT_DATE_WEEK_END
FROM CHECK_IF_1ST_TRANSMITTED_IS_DELINQUENT
LEFT JOIN FIRST_LOAN_PER_CUSTOMER
ON CHECK_IF_1ST_TRANSMITTED_IS_DELINQUENT.LOAN_KEY = FIRST_LOAN_PER_CUSTOMER.LOAN_KEY
)
SELECT PAYMENT_DATE_WEEK_END
, COUNT(*) AS TOTAL_FIRST_PAYMENTS_SCHEDULED
, SUM(_1ST_TRANSMITTED_IS_DELQ) AS TOTAL_MISSED_PAYMENTS
, SUM(CASE WHEN _1ST_TRANSMITTED_IS_DELQ = 1 THEN PAYMENT_TOTAL_AMOUNT ELSE 0 END) AS TOTAL_MISSED_PAYMENT_AMOUNTS
, SUM(PAYMENT_TOTAL_AMOUNT) AS TOTAL_FIRST_PAYMENT_AMOUNTS
, TOTAL_MISSED_PAYMENT_AMOUNTS/TOTAL_FIRST_PAYMENT_AMOUNTS AS FPD_PERCENTAGE_AMOUNT
, TOTAL_MISSED_PAYMENTS/TOTAL_FIRST_PAYMENTS_SCHEDULED AS FPD_PERCENTAGE_ACCOUNTS
FROM FINAL_TABLE
WHERE IS_1ST_LOAN_PER_CUSTOMER = 1
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_FRAUD_METRICS_AGG AS ( 
select ca.week_end week_end_date

    -- % of Customers Triggering fraud review in onboarding
    , NVL(ca.n_fbbids_triggered_ob_rules, 0) as n_fbbids_triggered_ob_rules
    , NVL(ca.n_credit_approved_fbbids, 0) as n_credit_approved_fbbids
    , NVL(ca.pct_ob_reviewed_for_fraud, 0) as pct_ob_reviewed_for_fraud    
    
    -- # of Customers Triggering fraud review in onboarding
    , NVL(nfr.n_triggered_fr_total, 0) as n_triggered_fr_total
    , NVL(nfr.n_triggered_fr_scorecard, 0) as n_triggered_fr_scorecard
    , NVL(nfr.n_triggered_fr_rules, 0) as n_triggered_fr_rules
    , NVL(nfr.n_triggered_fr_avq, 0) as n_triggered_fr_avq    
    
    -- # of Customers Fraud Reviewed Ongoing
    , NVL(nog.n_triggered_og_total, 0) as n_triggered_og_total
    , NVL(nog.n_triggered_og_avq, 0) as n_triggered_og_avq
    , NVL(nog.n_fbbids_triggered_og_manual, 0) as n_fbbids_triggered_og_manual    
    
    -- # of Cases Tagged as Fraud
    , NVL(ntf.n_fraud_total, 0) as n_fraud_total
    , NVL(ntf.n_fraud_scorecard, 0) as n_fraud_scorecard
    , NVL(ntf.n_fraud_rules, 0) as n_fraud_rules
    , NVL(ntf.n_fraud_avq, 0) as n_fraud_avq
    , NVL(ntf.n_red_flag, 0) as n_red_flag
    , NVL(ntf.n_fraud_post_approval, 0) as n_fraud_post_approval    
    
    --$ Total Loss Avoided 
    , NVL(sum_total_loss_avoided, 0) as sum_total_loss_avoided
    , NVL(sum_loss_avoided_fraud_scorecard, 0) as sum_loss_avoided_fraud_scorecard
    , NVL(sum_loss_avoided_fraud_rules, 0) as sum_loss_avoided_fraud_rules
    , NVL(sum_loss_avoided_fraud_avq, 0) as sum_loss_avoided_fraud_avq
    , NVL(sum_loss_avoided_red_flag, 0) as sum_loss_avoided_red_flag    
    
    -- $ Total Fraud Loss
    , NVL(tfl.total_fraud_loss, 0) as sum_total_fraud_loss
    , fpd.FPD_PERCENTAGE_AMOUNT 
    , fpd.FPD_PERCENTAGE_ACCOUNTS


    from analytics.fraud.v_key_metrics_pct_credit_approved_fraud_review_ob ca 
    
    left join ( 
        select week_end
          , nvl(n_triggered_fr_rules, 0) + nvl(n_triggered_fr_scorecard, 0) + nvl(n_triggered_fr_avq,0) n_triggered_fr_total
          , n_triggered_fr_rules
          , n_triggered_fr_scorecard
          , n_triggered_fr_avq

        from analytics.fraud.v_key_metrics_n_reviews_onboarding
            Pivot(sum(n_fbbids_triggered_ob_rules) for sub_cat in ('Rules','Scorecard','AvengerQ')) 
                as p (metric_name, week_start, week_end, n_triggered_fr_rules, n_triggered_fr_scorecard, n_triggered_fr_avq)

        where week_end >= to_date('2021-01-01')
    ) nfr
        on nfr.week_end = ca.week_end
    
--    left join ( 
--        select week_end
--          , nvl(n_triggered_fr_rules, 0) + nvl(n_triggered_fr_scorecard, 0) + nvl(n_triggered_fr_avq,0) n_triggered_fr_total
--          , n_triggered_fr_rules
--          , n_triggered_fr_scorecard
--          , n_triggered_fr_avq
--
--        from analytics.fraud.v_key_metrics_n_reviews_onboarding
--            Pivot(sum(n_fbbids_triggered_ob_rules) for sub_cat in ('Scorecard', 'Rules', 'AvengerQ')) 
--                as p (metric_name, week_start, week_end, n_triggered_fr_rules, n_triggered_fr_scorecard, n_triggered_fr_avq)
--
--        where week_end >= to_date('2021-01-01')
--    ) nfr
--        on nfr.week_end = ca.week_end

    left join ( 
        select week_end
          , nvl(n_triggered_og_avq,0) + nvl(n_fbbids_triggered_og_manual,0) n_triggered_og_total
          , n_triggered_og_avq
          , n_fbbids_triggered_og_manual

        from analytics.fraud.v_key_metrics_n_reviews_ongoing
            Pivot(sum(n_fbbids_triggered_ob_rules) for sub_cat in ('AvengerQ', 'Manual')) 
                as p (metric_name, week_start, week_end, n_triggered_og_avq, n_fbbids_triggered_og_manual)

        where week_end >= to_date('2021-01-01')
    ) nog
        on nog.week_end = ca.week_end

    left join ( 
        select week_end
              , nvl(n_fraud_scorecard,0)+ nvl(n_fraud_rules,0)+ nvl(n_fraud_avq, 0)+ nvl(n_fraud_post_approval,0) n_fraud_total
              , nvl(n_fraud_scorecard,0) n_fraud_scorecard
              , nvl(n_fraud_rules,0) n_fraud_rules
              , nvl(n_fraud_avq, 0) n_fraud_avq
              , nvl(n_red_flag, 0) n_red_flag
              , nvl(n_fraud_post_approval,0) n_fraud_post_approval
         
        from analytics.fraud.v_key_metrics_n_cases_tagged_fraud tf
            Pivot(sum(n_tagged_fraud) for sub_cat in ('Scorecard', 'Rules','AvengerQ', 'Red Flag', 'Post Approval')) 
                as p (metric_name, week_start, week_end, n_fraud_scorecard, n_fraud_rules, n_fraud_avq , n_red_flag, n_fraud_post_approval)
        where week_end >= to_date('2021-01-01')
    ) ntf
        on ntf.week_end = ca.week_end

    left join (
        select  week_end
                , nvl(sum_loss_avoided_fraud_scorecard, 0)+ nvl(sum_loss_avoided_fraud_rules, 0) +nvl(sum_loss_avoided_fraud_avq, 0) + nvl(sum_loss_avoided_red_flag,0) sum_total_loss_avoided
                , nvl(sum_loss_avoided_fraud_scorecard, 0) sum_loss_avoided_fraud_scorecard
                , nvl(sum_loss_avoided_fraud_rules, 0) sum_loss_avoided_fraud_rules
                , nvl(sum_loss_avoided_fraud_avq, 0) sum_loss_avoided_fraud_avq
                , nvl(sum_loss_avoided_red_flag,0) sum_loss_avoided_red_flag

         from analytics.fraud.v_key_metrics_loss_avoided tf
            Pivot(sum(sum_loss_avoided) for sub_cat in ('Scorecard', 'Rules','AvengerQ', 'Red Flag')) 
                as p (metric_name, week_start, week_end, sum_loss_avoided_fraud_scorecard, sum_loss_avoided_fraud_rules,sum_loss_avoided_fraud_avq, sum_loss_avoided_red_flag)

        where week_end >= to_date('2021-01-01')
    ) tla
        on tla.week_end = ca.week_end

    left join analytics.fraud.v_key_metrics_total_fraud_loss tfl
        on tfl.week_end = ca.week_end

    left join analytics.credit.FIRST_PAYMENT_DEFAULT_METRICS_CALCULATED fpd 
        on ca.week_end = fpd.PAYMENT_DATE_WEEK_END

    order by ca.week_end desc
    );
