-------------------------last updated on 22nd may -----------------

-------------------------------------------------------AGGREGATE QUERY--------------------------------------------



CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_COLLECTIONS_METRICS_AGG_SYSTEM_VSHELL AS ( 

select cst.week_end AS week_end_date

    , cst.partner
    , national_funding_flow
    , cst.new_cust_filter
    , cst.bucket_group

    -- $ Collected by Bucket
    , nvl(dc.dc_1_2, 0) collected_dpd_1_2
    , nvl(dc.dc_3_13, 0) collected_dpd_3_13
    , nvl(dc.dc_int_14, 0) collected_int_dpd_14
    , nvl(dc.dc_ext_14, 0) collected_ext_dpd_14

    -- Cure Rates by Segment (num, den, num, den, num, den)
    , nvl(ercb.cure_cnt_1, 0) cure_cnt_dpd_1
    , nvl(ercb.cust_cnt_1, 0) cust_cnt_dpd_1

    , nvl(ercb.cure_cnt_2, 0) cure_cnt_dpd_2
    , nvl(ercb.cust_cnt_2, 0) cust_cnt_dpd_2

    , nvl(lrcb.cure_cnt, 0) cure_cnt_dpd_3_13
    , nvl(lrcb.cust_cnt, 0) cust_cnt_dpd_3_13

    -- 1,2,3 Consecutive Missed Payments and No Payment COs (num, num, num, num, den)
    , nvl(mpb.first_payment_missed, 0) first_payment_missed
    , nvl(mpb.two_payments_missed, 0) two_payments_missed
    , nvl(mpb.three_payments_missed, 0) three_payments_missed
    , nvl(mpb.no_payment_co, 0) no_payment_co
    , nvl(mpb.total_originations, 0) total_originations

    -- Balance Paid Percentage by Segment (num, den, num, den)
    , nvl(bppb.balance_paid_1_2, 0) balance_paid_dpd_1_2
    , nvl(bppb.balance_dpd_1_2, 0) balance_dpd_1_2
    
    , nvl(bppb.balance_paid_3_13, 0) balance_paid_dpd_3_13
    , nvl(bppb.balance_dpd_3_13, 0) balance_dpd_3_13

    --previous week

        --$ Collected by Bucket
        , lag(dc.dc_1_2, 1) OVER (order by dc.week_end) collected_dpd_1_2_prev_week
        , lag(dc.dc_3_13, 1) OVER (order by dc.week_end) collected_dpd_3_13_prev_week
        , lag(dc.dc_int_14, 1) OVER (order by dc.week_end) collected_int_dpd_14_prev_week
        , lag(dc.dc_ext_14, 1) OVER (order by dc.week_end) collected_ext_dpd_14_prev_week

        -- Cure Rates by Segment (num, den, num, den, num, den)
        , nvl(lag(ercb.cure_cnt_1, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end), 0) cure_cnt_dpd_1_prev_week
        , nvl(lag(ercb.cust_cnt_1, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end), 0) cust_cnt_dpd_1_prev_week
        
        , nvl(lag(ercb.cure_cnt_2, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end), 0) cure_cnt_dpd_2_prev_week
        , nvl(lag(ercb.cust_cnt_2, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end), 0) cust_cnt_dpd_2_prev_week
        
        , nvl(lag(lrcb.cure_cnt, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by lrcb.week_end), 0) cure_cnt_dpd_3_13_prev_week
        , nvl(lag(lrcb.cust_cnt, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by lrcb.week_end), 0) cust_cnt_dpd_3_13_prev_week

        -- 1,2,3 Consecutive Missed Payments and No Payment COs (num, num, num, num, den)
        , nvl(lag(mpb.first_payment_missed, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end), 0) first_payment_missed_prev_week
        , nvl(lag(mpb.two_payments_missed, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end), 0) two_payments_missed_prev_week
        , nvl(lag(mpb.three_payments_missed, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end), 0) three_payments_missed_prev_week
        , nvl(lag(mpb.no_payment_co, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end), 0) no_payment_co_prev_week
        , nvl(lag(mpb.total_originations, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end), 0) total_originations_prev_week

        -- Balance Paid Percentage by Segment (num, den, num, den)
        , nvl(lag(bppb.balance_paid_1_2, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by bppb.week_end), 0) balance_paid_dpd_1_2_prev_week
        , nvl(lag(bppb.balance_dpd_1_2, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by bppb.week_end), 0) balance_dpd_1_2_prev_week
        , nvl(lag(bppb.balance_paid_3_13, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by bppb.week_end), 0) balance_paid_dpd_3_13_prev_week
        , nvl(lag(bppb.balance_dpd_3_13, 1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by bppb.week_end), 0) balance_dpd_3_13_prev_week

    --1 to 4 weeks

        , sum(dc.dc_1_2) OVER (order by dc.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) collected_dpd_1_2_prev_4_weeks
        , sum(dc.dc_3_13) OVER (order by dc.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) collected_dpd_3_13_prev_4_weeks
        , sum(dc.dc_int_14) OVER (order by dc.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) collected_int_dpd_14_prev_4_weeks
        , sum(dc.dc_ext_14) OVER (order by dc.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) collected_ext_dpd_14_prev_4_weeks

        --ER Cures (num, den, num, den)
        , sum(ercb.cure_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cure_cnt_dpd_1_prev_4_weeks
        , sum(ercb.cust_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cust_cnt_dpd_1_prev_4_weeks
        , sum(ercb.cure_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cure_cnt_dpd_2_prev_4_weeks
        , sum(ercb.cust_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cust_cnt_dpd_2_prev_4_weeks

        --LR Cures (num, den)
        , sum(lrcb.cure_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cure_cnt_dpd_3_13_prev_4_weeks
        , sum(lrcb.cust_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cust_cnt_dpd_3_13_prev_4_weeks

        --Cons Missed Payments (num, num, num, num, den)
        , sum(mpb.first_payment_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) first_payment_missed_prev_4_weeks
        , sum(mpb.two_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) two_payments_missed_prev_4_weeks
        , sum(mpb.three_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) three_payments_missed_prev_4_weeks
        , sum(mpb.no_payment_co) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) no_payment_co_prev_4_weeks
        , sum(mpb.total_originations) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) total_originations_prev_4_weeks

        --Balance Paid (num, den, num, den)
        , sum(bppb.balance_paid_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_paid_dpd_1_2_prev_4_weeks
        , sum(bppb.balance_dpd_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_dpd_1_2_prev_4_weeks
        , sum(bppb.balance_paid_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_paid_dpd_3_13_prev_4_weeks
        , sum(bppb.balance_dpd_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_dpd_3_13_prev_4_weeks

    --5 to 8 weeks

        , sum(dc.dc_1_2) OVER (order by dc.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) collected_dpd_1_2_prev_5_to_8_weeks
        , sum(dc.dc_3_13) OVER (order by dc.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) collected_dpd_3_13_prev_5_to_8_weeks
        , sum(dc.dc_int_14) OVER (order by dc.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) collected_int_dpd_14_prev_5_to_8_weeks
        , sum(dc.dc_ext_14) OVER (order by dc.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) collected_ext_dpd_14_prev_5_to_8_weeks

        --ER Cures (num, den, num, den)
        , sum(ercb.cure_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cure_cnt_dpd_1_prev_5_to_8_weeks
        , sum(ercb.cust_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cust_cnt_dpd_1_prev_5_to_8_weeks
        , sum(ercb.cure_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cure_cnt_dpd_2_prev_5_to_8_weeks
        , sum(ercb.cust_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cust_cnt_dpd_2_prev_5_to_8_weeks

        --LR Cures (num, den)
        , sum(lrcb.cure_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cure_cnt_dpd_3_13_prev_5_to_8_weeks
        , sum(lrcb.cust_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) cust_cnt_dpd_3_13_prev_5_to_8_weeks

        --Cons Missed Payments (num, num, num, num, den)
        , sum(mpb.first_payment_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) first_payment_missed_prev_5_to_8_weeks
        , sum(mpb.two_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) two_payments_missed_prev_5_to_8_weeks
        , sum(mpb.three_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) three_payments_missed_prev_5_to_8_weeks
        , sum(mpb.no_payment_co) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) no_payment_co_prev_5_to_8_weeks
        , sum(mpb.total_originations) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) total_originations_prev_5_to_8_weeks

        --Balance Paid (num, den, num, den)
        , sum(bppb.balance_paid_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) balance_paid_dpd_1_2_prev_5_to_8_weeks
        , sum(bppb.balance_dpd_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) balance_dpd_1_2_prev_5_to_8_weeks
        , sum(bppb.balance_paid_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) balance_paid_dpd_3_13_prev_5_to_8_weeks
        , sum(bppb.balance_dpd_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING) balance_dpd_3_13_prev_5_to_8_weeks

    --Prev 12 Weeks

        , sum(dc.dc_1_2) OVER (order by dc.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) collected_dpd_1_2_prev_12_weeks
        , sum(dc.dc_3_13) OVER (order by dc.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) collected_dpd_3_13_prev_12_weeks
        , sum(dc.dc_int_14) OVER (order by dc.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) collected_int_dpd_14_prev_12_weeks
        , sum(dc.dc_ext_14) OVER (order by dc.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) collected_ext_dpd_14_prev_12_weeks

        --ER Cures (num, den, num, den)
        , sum(ercb.cure_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cure_cnt_dpd_1_prev_12_weeks
        , sum(ercb.cust_cnt_1) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cust_cnt_dpd_1_prev_12_weeks
        , sum(ercb.cure_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cure_cnt_dpd_2_prev_12_weeks
        , sum(ercb.cust_cnt_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cust_cnt_dpd_2_prev_12_weeks

        --LR Cures (num, den, num, den)
        , sum(lrcb.cure_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cure_cnt_dpd_3_13_prev_12_weeks
        , sum(lrcb.cust_cnt) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) cust_cnt_dpd_3_13_prev_12_weeks

        --Cons Missed Payments (num, num, num, num, den)
        , sum(mpb.first_payment_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) first_payment_missed_prev_12_weeks
        , sum(mpb.two_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) two_payments_missed_prev_12_weeks
        , sum(mpb.three_payments_missed) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) three_payments_missed_prev_12_weeks
        , sum(mpb.no_payment_co) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) no_payment_co_prev_12_weeks
        , sum(mpb.total_originations) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by mpb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) total_originations_prev_12_weeks

        --Balance Paid (num, den, num, den)
        , sum(bppb.balance_paid_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) balance_paid_dpd_1_2_prev_12_weeks
        , sum(bppb.balance_dpd_1_2) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) balance_dpd_1_2_prev_12_weeks
        , sum(bppb.balance_paid_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) balance_paid_dpd_3_13_prev_12_weeks
        , sum(bppb.balance_dpd_3_13) OVER (partition by ercb.partner, ercb.bucket_group, ercb.new_cust_filter order by ercb.week_end ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) balance_dpd_3_13_prev_12_weeks

/*        
FROM (SELECT * 
FROM (SELECT week_end, partner, new_cust_filter, bucket_group FROM analytics.credit.cjk_v_er_cure_rate_breakdown) UNION
(SELECT week_end, partner, new_cust_filter, bucket_group FROM analytics.credit.cjk_v_key_metrics_cure_rate_3_13_breakdown) UNION 
(SELECT week_end, partner, new_cust_filter, bucket_group FROM analytics.credit.cjk_v_key_metrics_1_3_5_missed_payments_breakdown) UNION 
(SELECT week_end, partner, new_cust_filter, bucket_group FROM analytics.credit.cjk_v_key_metrics_balance_paid_perc_breakdown) UNION 
(SELECT week_end, partner, new_cust_filter, bucket_group FROM analytics.credit.cjk_v_key_metrics_collected))cst*/
        
FROM indus.PUBLIC.COLLECTIONS_SHELL_Table_AGG_final cst
LEFT JOIN analytics.credit.cjk_v_er_cure_rate_breakdown ercb
ON cst.week_end = ercb.week_end and cst.partner = ercb.partner and cst.new_cust_filter = ercb.new_cust_filter and cst.bucket_group = ercb.bucket_group
LEFT join analytics.credit.cjk_v_key_metrics_cure_rate_3_13_breakdown lrcb
ON cst.week_end = lrcb.week_end and cst.partner = lrcb.partner and cst.new_cust_filter = lrcb.new_cust_filter and cst.bucket_group = lrcb.bucket_group
LEFT join analytics.credit.cjk_v_key_metrics_1_3_5_missed_payments_breakdown mpb
ON cst.week_end = mpb.week_end and cst.partner = mpb.partner and cst.new_cust_filter = mpb.new_cust_filter and cst.bucket_group = mpb.bucket_group
LEFT join analytics.credit.cjk_v_key_metrics_balance_paid_perc_breakdown bppb
ON cst.week_end = DATEADD('day', 1, bppb.week_end) and cst.partner = bppb.partner and cst.new_cust_filter = bppb.new_cust_filter and cst.bucket_group = bppb.bucket_group
LEFT join analytics.credit.cjk_v_key_metrics_collected dc
ON cst.week_end = DATEADD('day', 1, dc.week_end)      
                
/*from analytics.credit.cjk_v_er_cure_rate_breakdown ercb
join analytics.credit.cjk_v_key_metrics_cure_rate_3_13_breakdown lrcb
ON ercb.week_end = lrcb.week_end and ercb.partner = lrcb.partner and ercb.new_cust_filter = lrcb.new_cust_filter and ercb.bucket_group = lrcb.bucket_group
join analytics.credit.cjk_v_key_metrics_1_3_5_missed_payments_breakdown mpb
ON ercb.week_end = mpb.week_end and ercb.partner = mpb.partner and ercb.new_cust_filter = mpb.new_cust_filter and ercb.bucket_group = mpb.bucket_group
join analytics.credit.cjk_v_key_metrics_balance_paid_perc_breakdown bppb
ON ercb.week_end = DATEADD('day', 1, bppb.week_end) and ercb.partner = bppb.partner and ercb.new_cust_filter = bppb.new_cust_filter and ercb.bucket_group = bppb.bucket_group
join analytics.credit.cjk_v_key_metrics_collected dc
ON ercb.week_end = DATEADD('day', 1, dc.week_end)  */

--where ercb.week_end >= '2022-1-1'

where cst.week_end >= '2022-01-01'
order by 1, 2, 3, 4

);


CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_COLLECTIONS_METRICS_AGG_SYSTEM_VSHELL2 AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY WEEK_END_DATE ORDER BY WEEK_END_DATE) AS rank_1
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_COLLECTIONS_METRICS_AGG_SYSTEM_VSHELL
);


CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_COLLECTIONS_METRICS_AGG_SYSTEM_VSHELL3 AS (
    SELECT *,
        CASE WHEN
        rank_1 = 1 THEN COLLECTED_DPD_1_2
        ELSE 0
        END AS COLLECTED_DPD_1_2_v2
        , CASE WHEN
        rank_1 = 1 THEN COLLECTED_DPD_3_13
        ELSE 0
        END AS COLLECTED_DPD_3_13_v2
        ,CASE WHEN 
        rank_1 = 1 THEN COLLECTED_INT_DPD_14
        ELSE 0
        END AS COLLECTED_INT_DPD_14_v2
        ,CASE WHEN
        rank_1 = 1 THEN COLLECTED_EXT_DPD_14
        ELSE 0
        END AS COLLECTED_EXT_DPD_14_v2
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_COLLECTIONS_METRICS_AGG_SYSTEM_VSHELL2
);





