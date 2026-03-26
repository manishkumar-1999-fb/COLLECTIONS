create or replace table indus.public.forecast_data AS(

with f1 as (
    select t1.*
    , t2.actual_approvals
    , t2.actual_approvals - t1.total_approvals as approval_difference
    , (CASE WHEN total_approvals IS NOT NULL AND total_approvals <> 0 THEN approval_difference/total_approvals ELSE 0 END) as delta_approvals
    , t2.actual_new_exposure
    , t2.actual_new_exposure - t1.new_customer_exposure as new_exposure_difference
    , (CASE WHEN new_customer_exposure IS NOT NULL AND new_customer_exposure <> 0 THEN new_exposure_difference/new_customer_exposure ELSE 0 END) as delta_new_exposure
    from ANALYTICS.FINANCE.KM_FORECAST_DATA_APPROVAL t1
    left join 
    (
    select week_end_date, TERMUNITS, sum(approvals) as actual_approvals, sum(app_credit_limit) as actual_new_exposure
    from indus.public.indus_key_metrics_funnel_agg
    group by 1,2
    ) t2
    on t1.week_end_date = t2.week_end_date
    and lower(t1.channel) = lower(t2.TERMUNITS)
    -- where t2.week_end_date IS NOT NULL
)

,f2 as (

    SELECT t1.*
    , t3.actual_origination
    , t3.actual_origination - t1.total_origination as total_origination_difference
    , (CASE WHEN total_origination IS NOT NULL AND total_origination <> 0 THEN total_origination_difference/total_origination ELSE 0 END) as delta_total_origination
    
    , t3.actual_new_origination
    , t3.actual_new_origination - t1.new_customer_origination as new_origination_difference
    , (CASE WHEN new_customer_origination IS NOT NULL AND new_customer_origination <> 0 THEN new_origination_difference/new_customer_origination ELSE 0 END) as delta_new_origination
    
    , t3.actual_exist_origination
    , t3.actual_exist_origination - t1.existing_customer_origination as exist_origination_difference
    , (CASE WHEN existing_customer_origination IS NOT NULL AND existing_customer_origination <> 0 THEN exist_origination_difference/existing_customer_origination ELSE 0 END) as delta_exist_origination
    
    -- Adding t3 values, differences, and deltas for different payment plans
    , t3.actual_origination_12
    , t3.actual_origination_12 - t1.total_origination_12 as total_origination_difference_12
    , (CASE WHEN t1.total_origination_12 IS NOT NULL AND t1.total_origination_12 <> 0 THEN total_origination_difference_12/t1.total_origination_12 ELSE 0 END) as delta_total_origination_12
    
    , t3.actual_origination_24
    , t3.actual_origination_24 - t1.total_origination_24 as total_origination_difference_24
    , (CASE WHEN t1.total_origination_24 IS NOT NULL AND t1.total_origination_24 <> 0 THEN total_origination_difference_24/t1.total_origination_24 ELSE 0 END) as delta_total_origination_24
    
    , t3.actual_origination_52
    , t3.actual_origination_52 - t1.total_origination_52 as total_origination_difference_52
    , (CASE WHEN t1.total_origination_52 IS NOT NULL AND t1.total_origination_52 <> 0 THEN total_origination_difference_52/t1.total_origination_52 ELSE 0 END) as delta_total_origination_52
    
    , t3.actual_new_origination_12
    , t3.actual_new_origination_12 - t1.new_customer_origination_12 as new_origination_difference_12
    , (CASE WHEN t1.new_customer_origination_12 IS NOT NULL AND t1.new_customer_origination_12 <> 0 THEN new_origination_difference_12/t1.new_customer_origination_12 ELSE 0 END) as delta_new_origination_12
    
    , t3.actual_new_origination_24
    , t3.actual_new_origination_24 - t1.new_customer_origination_24 as new_origination_difference_24
    , (CASE WHEN t1.new_customer_origination_24 IS NOT NULL AND t1.new_customer_origination_24 <> 0 THEN new_origination_difference_24/t1.new_customer_origination_24 ELSE 0 END) as delta_new_origination_24
    
    , t3.actual_new_origination_52
    , t3.actual_new_origination_52 - t1.new_customer_origination_52 as new_origination_difference_52
    , (CASE WHEN t1.new_customer_origination_52 IS NOT NULL AND t1.new_customer_origination_52 <> 0 THEN new_origination_difference_52/t1.new_customer_origination_52 ELSE 0 END) as delta_new_origination_52
    
    , t3.actual_exist_origination_12
    , t3.actual_exist_origination_12 - t1.existing_customer_origination_12 as exist_origination_difference_12
    , (CASE WHEN t1.existing_customer_origination_12 IS NOT NULL AND t1.existing_customer_origination_12 <> 0 THEN exist_origination_difference_12/t1.existing_customer_origination_12 ELSE 0 END) as delta_exist_origination_12
    
    , t3.actual_exist_origination_24
    , t3.actual_exist_origination_24 - t1.existing_customer_origination_24 as exist_origination_difference_24
    , (CASE WHEN t1.existing_customer_origination_24 IS NOT NULL AND t1.existing_customer_origination_24 <> 0 THEN exist_origination_difference_24/t1.existing_customer_origination_24 ELSE 0 END) as delta_exist_origination_24
    
    , t3.actual_exist_origination_52
    , t3.actual_exist_origination_52 - t1.existing_customer_origination_52 as exist_origination_difference_52
    , (CASE WHEN t1.existing_customer_origination_52 IS NOT NULL AND t1.existing_customer_origination_52 <> 0 THEN exist_origination_difference_52/t1.existing_customer_origination_52 ELSE 0 END) as delta_exist_origination_52
    from 
    (
        select week_end_date, 
        channel, 
        sum(NEW_CUSTOMER_ORIGINATION) AS NEW_CUSTOMER_ORIGINATION, 
        sum(case when term = 12 then NEW_CUSTOMER_ORIGINATION else 0 end) NEW_CUSTOMER_ORIGINATION_12,
        sum(case when term = 24 then NEW_CUSTOMER_ORIGINATION else 0 end) NEW_CUSTOMER_ORIGINATION_24,
        sum(case when term = 52 then NEW_CUSTOMER_ORIGINATION else 0 end) NEW_CUSTOMER_ORIGINATION_52,
        sum(EXISTING_CUSTOMER_ORIGINATION) AS EXISTING_CUSTOMER_ORIGINATION, 
        sum(case when term = 12 then EXISTING_CUSTOMER_ORIGINATION else 0 end) EXISTING_CUSTOMER_ORIGINATION_12,
        sum(case when term = 24 then EXISTING_CUSTOMER_ORIGINATION else 0 end) EXISTING_CUSTOMER_ORIGINATION_24,
        sum(case when term = 52 then EXISTING_CUSTOMER_ORIGINATION else 0 end) EXISTING_CUSTOMER_ORIGINATION_52, 
        sum(TOTAL_ORIGINATION) AS TOTAL_ORIGINATION,
        sum(case when term = 12 then TOTAL_ORIGINATION else 0 end) TOTAL_ORIGINATION_12,
        sum(case when term = 24 then TOTAL_ORIGINATION else 0 end) TOTAL_ORIGINATION_24,
        sum(case when term = 52 then TOTAL_ORIGINATION else 0 end) TOTAL_ORIGINATION_52
        from ANALYTICS.FINANCE.KM_FORECAST_DATA_METRICS
        group by 1,2
    )t1
    left join 
    (   
        select week_end_date
        ,TERMUNITS
        ,sum(orig) as actual_origination
        ,sum(case when payment_plan = '12 Week' then orig else 0 end) actual_origination_12
        ,sum(case when payment_plan = '24 Week' then orig else 0 end) actual_origination_24
        ,sum(case when payment_plan = '52 Week' then orig else 0 end) actual_origination_52
        ,sum(case when new_cust_filter = 'New Customer' then orig else 0 end) actual_new_origination
        ,sum(case when new_cust_filter = 'New Customer' and payment_plan = '12 Week' then orig else 0 end) actual_new_origination_12
        ,sum(case when new_cust_filter = 'New Customer' and payment_plan = '24 Week' then orig else 0 end) actual_new_origination_24
        ,sum(case when new_cust_filter = 'New Customer' and payment_plan = '52 Week' then orig else 0 end) actual_new_origination_52
        ,sum(case when new_cust_filter = 'Existing Customer' then orig else 0 end) actual_exist_origination
        ,sum(case when new_cust_filter = 'Existing Customer' and payment_plan = '12 Week' then orig else 0 end) actual_exist_origination_12
        ,sum(case when new_cust_filter = 'Existing Customer' and payment_plan = '24 Week' then orig else 0 end) actual_exist_origination_24
        ,sum(case when new_cust_filter = 'Existing Customer' and payment_plan = '52 Week' then orig else 0 end) actual_exist_origination_52
    from indus.public.indus_key_metrics_loan_originations_agg_test_1
    group by 1,2
    ) t3
    on t1.week_end_date = t3.week_end_date
    and lower(t1.channel) = lower(t3.TERMUNITS)

)
,f3 as (
    SELECT t1.*,
    -- Total Outstanding Balance and Differences
    t3.actual_os,
    t3.actual_os - t1.TOTAL_OUTSTANDING_BALANCE AS total_os_difference,
    (CASE 
        WHEN TOTAL_OUTSTANDING_BALANCE IS NOT NULL AND TOTAL_OUTSTANDING_BALANCE <> 0 THEN total_os_difference / TOTAL_OUTSTANDING_BALANCE 
        ELSE 0 
    END) AS delta_total_os,
    t3.actual_os_12, t3.actual_os_24, t3.actual_os_52,
    t3.actual_os_12 - t1.TOTAL_OUTSTANDING_BALANCE_12 AS total_os_difference_12,
    t3.actual_os_24 - t1.TOTAL_OUTSTANDING_BALANCE_24 AS total_os_difference_24,
    t3.actual_os_52 - t1.TOTAL_OUTSTANDING_BALANCE_52 AS total_os_difference_52,

    -- New Outstanding Balance and Differences
    t3.actual_new_os,
    t3.actual_new_os - t1.NEW_CUSTOMER_OUTSTANDING_BALANCE AS new_os_difference,
    (CASE 
        WHEN NEW_CUSTOMER_OUTSTANDING_BALANCE IS NOT NULL AND NEW_CUSTOMER_OUTSTANDING_BALANCE <> 0 THEN new_os_difference / NEW_CUSTOMER_OUTSTANDING_BALANCE 
        ELSE 0 
    END) AS delta_new_os,
    t3.actual_new_os_12, t3.actual_new_os_24, t3.actual_new_os_52,
    t3.actual_new_os_12 - t1.NEW_CUSTOMER_OUTSTANDING_BALANCE_12 AS new_os_difference_12,
    t3.actual_new_os_24 - t1.NEW_CUSTOMER_OUTSTANDING_BALANCE_24 AS new_os_difference_24,
    t3.actual_new_os_52 - t1.NEW_CUSTOMER_OUTSTANDING_BALANCE_52 AS new_os_difference_52,

    -- Existing Outstanding Balance and Differences
    t3.actual_exist_os,
    t3.actual_exist_os - t1.EXISTING_CUSTOMER_OUTSTANDING_BALANCE AS exist_os_difference,
    (CASE 
        WHEN EXISTING_CUSTOMER_OUTSTANDING_BALANCE IS NOT NULL AND EXISTING_CUSTOMER_OUTSTANDING_BALANCE <> 0 THEN exist_os_difference / EXISTING_CUSTOMER_OUTSTANDING_BALANCE 
        ELSE 0 
    END) AS delta_exist_os,
    t3.actual_exist_os_12, t3.actual_exist_os_24, t3.actual_exist_os_52,
    t3.actual_exist_os_12 - t1.EXISTING_CUSTOMER_OUTSTANDING_BALANCE_12 AS exist_os_difference_12,
    t3.actual_exist_os_24 - t1.EXISTING_CUSTOMER_OUTSTANDING_BALANCE_24 AS exist_os_difference_24,
    t3.actual_exist_os_52 - t1.EXISTING_CUSTOMER_OUTSTANDING_BALANCE_52 AS exist_os_difference_52,

    -- Total Revenue and Differences
    t3.actual_revenue,
    t3.actual_revenue - t1.TOTAL_REVENUE AS revenue_difference,
    (CASE 
        WHEN TOTAL_REVENUE IS NOT NULL AND TOTAL_REVENUE <> 0 THEN revenue_difference / TOTAL_REVENUE 
        ELSE 0 
    END) AS delta_revenue,
    t3.actual_revenue_12, t3.actual_revenue_24, t3.actual_revenue_52,
    t3.actual_revenue_12 - t1.TOTAL_REVENUE_12 AS revenue_difference_12,
    t3.actual_revenue_24 - t1.TOTAL_REVENUE_24 AS revenue_difference_24,
    t3.actual_revenue_52 - t1.TOTAL_REVENUE_52 AS revenue_difference_52,

    -- New Revenue and Differences
    t3.actual_new_revenue,
    t3.actual_new_revenue - t1.NEW_CUSTOMER_REVENUE AS new_revenue_difference,
    (CASE 
        WHEN NEW_CUSTOMER_REVENUE IS NOT NULL AND NEW_CUSTOMER_REVENUE <> 0 THEN new_revenue_difference / NEW_CUSTOMER_REVENUE 
        ELSE 0 
    END) AS delta_new_revenue,
    t3.actual_new_revenue_12, t3.actual_new_revenue_24, t3.actual_new_revenue_52,
    t3.actual_new_revenue_12 - t1.NEW_CUSTOMER_REVENUE_12 AS new_revenue_difference_12,
    t3.actual_new_revenue_24 - t1.NEW_CUSTOMER_REVENUE_24 AS new_revenue_difference_24,
    t3.actual_new_revenue_52 - t1.NEW_CUSTOMER_REVENUE_52 AS new_revenue_difference_52,

    -- Existing Revenue and Differences
    t3.actual_exist_revenue,
    t3.actual_exist_revenue - t1.EXISTING_CUSTOMER_REVENUE AS exist_revenue_difference,
    (CASE 
        WHEN EXISTING_CUSTOMER_REVENUE IS NOT NULL AND EXISTING_CUSTOMER_REVENUE <> 0 THEN exist_revenue_difference / EXISTING_CUSTOMER_REVENUE 
        ELSE 0 
    END) AS delta_exist_revenue,
    t3.actual_exist_revenue_12, t3.actual_exist_revenue_24, t3.actual_exist_revenue_52,
    t3.actual_exist_revenue_12 - t1.EXISTING_CUSTOMER_REVENUE_12 AS exist_revenue_difference_12,
    t3.actual_exist_revenue_24 - t1.EXISTING_CUSTOMER_REVENUE_24 AS exist_revenue_difference_24,
    t3.actual_exist_revenue_52 - t1.EXISTING_CUSTOMER_REVENUE_52 AS exist_revenue_difference_52,

    -- NCO and Differences
    t3.actual_nco,
    t3.actual_nco - t1.TOTAL_NCO AS nco_difference,
    (CASE 
        WHEN TOTAL_NCO IS NOT NULL AND TOTAL_NCO <> 0 THEN nco_difference / TOTAL_NCO 
        ELSE 0 
    END) AS delta_nco,
    t3.actual_nco_12, t3.actual_nco_24, t3.actual_nco_52,
    t3.actual_nco_12 - t1.TOTAL_NCO_12 AS nco_difference_12,
    t3.actual_nco_24 - t1.TOTAL_NCO_24 AS nco_difference_24,
    t3.actual_nco_52 - t1.TOTAL_NCO_52 AS nco_difference_52,

    -- New NCO and Differences
    t3.actual_new_nco,
    t3.actual_new_nco - t1.NEW_CUSTOMER_NCO AS new_nco_difference,
    (CASE 
        WHEN NEW_CUSTOMER_NCO IS NOT NULL AND NEW_CUSTOMER_NCO <> 0 THEN new_nco_difference / NEW_CUSTOMER_NCO 
        ELSE 0 
    END) AS delta_new_nco,
    t3.actual_new_nco_12, t3.actual_new_nco_24, t3.actual_new_nco_52,
    t3.actual_new_nco_12 - t1.NEW_CUSTOMER_NCO_12 AS new_nco_difference_12,
    t3.actual_new_nco_24 - t1.NEW_CUSTOMER_NCO_24 AS new_nco_difference_24,
    t3.actual_new_nco_52 - t1.NEW_CUSTOMER_NCO_52 AS new_nco_difference_52,

    -- Existing NCO and Differences
    t3.actual_exist_nco,
    t3.actual_exist_nco - t1.EXISTING_CUSTOMER_NCO AS exist_nco_difference,
    (CASE 
        WHEN EXISTING_CUSTOMER_NCO IS NOT NULL AND EXISTING_CUSTOMER_NCO <> 0 THEN exist_nco_difference / EXISTING_CUSTOMER_NCO 
        ELSE 0 
    END) AS delta_exist_nco,
    t3.actual_exist_nco_12, t3.actual_exist_nco_24, t3.actual_exist_nco_52,
    t3.actual_exist_nco_12 - t1.EXISTING_CUSTOMER_NCO_12 AS exist_nco_difference_12,
    t3.actual_exist_nco_24 - t1.EXISTING_CUSTOMER_NCO_24 AS exist_nco_difference_24,
    t3.actual_exist_nco_52 - t1.EXISTING_CUSTOMER_NCO_52 AS exist_nco_difference_52,

    -- Lending Margin and Differences
    t3.actual_lending_margin,
    t3.actual_lending_margin - t1.TOTAL_LENDING_MARGIN AS lending_margin_difference,
    (CASE 
        WHEN TOTAL_LENDING_MARGIN IS NOT NULL AND TOTAL_LENDING_MARGIN <> 0 THEN lending_margin_difference / TOTAL_LENDING_MARGIN 
        ELSE 0 
    END) AS delta_lending_margin,
    t3.actual_lending_margin_12, t3.actual_lending_margin_24, t3.actual_lending_margin_52,
    t3.actual_lending_margin_12 - t1.TOTAL_LENDING_MARGIN_12 AS lending_margin_difference_12,
    t3.actual_lending_margin_24 - t1.TOTAL_LENDING_MARGIN_24 AS lending_margin_difference_24,
    t3.actual_lending_margin_52 - t1.TOTAL_LENDING_MARGIN_52 AS lending_margin_difference_52,

    -- New Lending Margin and Differences
    t3.actual_new_lending_margin,
    t3.actual_new_lending_margin - t1.NEW_CUSTOMER_LENDING_MARGIN AS new_lending_margin_difference,
    (CASE 
        WHEN NEW_CUSTOMER_LENDING_MARGIN IS NOT NULL AND NEW_CUSTOMER_LENDING_MARGIN <> 0 THEN new_lending_margin_difference / NEW_CUSTOMER_LENDING_MARGIN 
        ELSE 0 
    END) AS delta_new_lending_margin,
    t3.actual_new_lending_margin_12, t3.actual_new_lending_margin_24, t3.actual_new_lending_margin_52,
    t3.actual_new_lending_margin_12 - t1.NEW_CUSTOMER_LENDING_MARGIN_12 AS new_lending_margin_difference_12,
    t3.actual_new_lending_margin_24 - t1.NEW_CUSTOMER_LENDING_MARGIN_24 AS new_lending_margin_difference_24,
    t3.actual_new_lending_margin_52 - t1.NEW_CUSTOMER_LENDING_MARGIN_52 AS new_lending_margin_difference_52,

    -- Existing Lending Margin and Differences
    t3.actual_exist_lending_margin,
    t3.actual_exist_lending_margin - t1.EXISTING_CUSTOMER_LENDING_MARGIN AS exist_lending_margin_difference,
    (CASE 
        WHEN EXISTING_CUSTOMER_LENDING_MARGIN IS NOT NULL AND EXISTING_CUSTOMER_LENDING_MARGIN <> 0 THEN exist_lending_margin_difference / EXISTING_CUSTOMER_LENDING_MARGIN 
        ELSE 0 
    END) AS delta_exist_lending_margin,
    t3.actual_exist_lending_margin_12, t3.actual_exist_lending_margin_24, t3.actual_exist_lending_margin_52,
    t3.actual_exist_lending_margin_12 - t1.EXISTING_CUSTOMER_LENDING_MARGIN_12 AS exist_lending_margin_difference_12,
    t3.actual_exist_lending_margin_24 - t1.EXISTING_CUSTOMER_LENDING_MARGIN_24 AS exist_lending_margin_difference_24,
    t3.actual_exist_lending_margin_52 - t1.EXISTING_CUSTOMER_LENDING_MARGIN_52 AS exist_lending_margin_difference_52

    from 
    (
        select week_end_date, 
        channel, 
        sum(NEW_CUSTOMER_OUTSTANDING_BALANCE) AS NEW_CUSTOMER_OUTSTANDING_BALANCE, 
        sum(case when term = 12 then NEW_CUSTOMER_OUTSTANDING_BALANCE else 0 end) NEW_CUSTOMER_OUTSTANDING_BALANCE_12,
        sum(case when term = 24 then NEW_CUSTOMER_OUTSTANDING_BALANCE else 0 end) NEW_CUSTOMER_OUTSTANDING_BALANCE_24,
        sum(case when term = 52 then NEW_CUSTOMER_OUTSTANDING_BALANCE else 0 end) NEW_CUSTOMER_OUTSTANDING_BALANCE_52,
        sum(EXISTING_CUSTOMER_OUTSTANDING_BALANCE) AS EXISTING_CUSTOMER_OUTSTANDING_BALANCE,
        sum(case when term = 12 then EXISTING_CUSTOMER_OUTSTANDING_BALANCE else 0 end) EXISTING_CUSTOMER_OUTSTANDING_BALANCE_12,
        sum(case when term = 24 then EXISTING_CUSTOMER_OUTSTANDING_BALANCE else 0 end) EXISTING_CUSTOMER_OUTSTANDING_BALANCE_24,
        sum(case when term = 52 then EXISTING_CUSTOMER_OUTSTANDING_BALANCE else 0 end) EXISTING_CUSTOMER_OUTSTANDING_BALANCE_52, 
        sum(TOTAL_OUTSTANDING_BALANCE) AS TOTAL_OUTSTANDING_BALANCE, 
        sum(case when term = 12 then TOTAL_OUTSTANDING_BALANCE else 0 end) TOTAL_OUTSTANDING_BALANCE_12,
        sum(case when term = 24 then TOTAL_OUTSTANDING_BALANCE else 0 end) TOTAL_OUTSTANDING_BALANCE_24,
        sum(case when term = 52 then TOTAL_OUTSTANDING_BALANCE else 0 end) TOTAL_OUTSTANDING_BALANCE_52, 
        sum(TOTAL_REVENUE) AS TOTAL_REVENUE,
        sum(case when term = 12 then TOTAL_REVENUE else 0 end) TOTAL_REVENUE_12,
        sum(case when term = 24 then TOTAL_REVENUE else 0 end) TOTAL_REVENUE_24,
        sum(case when term = 52 then TOTAL_REVENUE else 0 end) TOTAL_REVENUE_52,
        sum(NEW_CUSTOMER_revenue) as NEW_CUSTOMER_revenue, 
        sum(case when term = 12 then NEW_CUSTOMER_revenue else 0 end) NEW_CUSTOMER_revenue_12,
        sum(case when term = 24 then NEW_CUSTOMER_revenue else 0 end) NEW_CUSTOMER_revenue_24,
        sum(case when term = 52 then NEW_CUSTOMER_revenue else 0 end) NEW_CUSTOMER_revenue_52,
        sum(EXISTING_CUSTOMER_revenue) as EXISTING_CUSTOMER_revenue,
        sum(case when term = 12 then EXISTING_CUSTOMER_revenue else 0 end) EXISTING_CUSTOMER_revenue_12,
        sum(case when term = 24 then EXISTING_CUSTOMER_revenue else 0 end) EXISTING_CUSTOMER_revenue_24,
        sum(case when term = 52 then EXISTING_CUSTOMER_revenue else 0 end) EXISTING_CUSTOMER_revenue_52,
        sum(total_nco) AS total_nco,
        sum(case when term = 12 then total_nco else 0 end) total_nco_12,
        sum(case when term = 24 then total_nco else 0 end) total_nco_24,
        sum(case when term = 52 then total_nco else 0 end) total_nco_52,
        sum(NEW_CUSTOMER_NCO) AS NEW_CUSTOMER_NCO,
        sum(case when term = 12 then NEW_CUSTOMER_NCO else 0 end) NEW_CUSTOMER_NCO_12,
        sum(case when term = 24 then NEW_CUSTOMER_NCO else 0 end) NEW_CUSTOMER_NCO_24,
        sum(case when term = 52 then NEW_CUSTOMER_NCO else 0 end) NEW_CUSTOMER_NCO_52,
        sum(EXISTING_CUSTOMER_NCO) AS EXISTING_CUSTOMER_NCO,
        sum(case when term = 12 then EXISTING_CUSTOMER_NCO else 0 end) EXISTING_CUSTOMER_NCO_12,
        sum(case when term = 24 then EXISTING_CUSTOMER_NCO else 0 end) EXISTING_CUSTOMER_NCO_24,
        sum(case when term = 52 then EXISTING_CUSTOMER_NCO else 0 end) EXISTING_CUSTOMER_NCO_52,
        sum(TOTAL_LENDING_MARGIN) AS TOTAL_LENDING_MARGIN,
        sum(case when term = 12 then TOTAL_LENDING_MARGIN else 0 end) TOTAL_LENDING_MARGIN_12,
        sum(case when term = 24 then TOTAL_LENDING_MARGIN else 0 end) TOTAL_LENDING_MARGIN_24,
        sum(case when term = 52 then TOTAL_LENDING_MARGIN else 0 end) TOTAL_LENDING_MARGIN_52,
        sum(NEW_CUSTOMER_LENDING_MARGIN) AS NEW_CUSTOMER_LENDING_MARGIN,
        sum(case when term = 12 then NEW_CUSTOMER_LENDING_MARGIN else 0 end) NEW_CUSTOMER_LENDING_MARGIN_12,
        sum(case when term = 24 then NEW_CUSTOMER_LENDING_MARGIN else 0 end) NEW_CUSTOMER_LENDING_MARGIN_24,
        sum(case when term = 52 then NEW_CUSTOMER_LENDING_MARGIN else 0 end) NEW_CUSTOMER_LENDING_MARGIN_52,
        sum(EXISTING_CUSTOMER_LENDING_MARGIN) AS EXISTING_CUSTOMER_LENDING_MARGIN,
        sum(case when term = 12 then EXISTING_CUSTOMER_LENDING_MARGIN else 0 end) EXISTING_CUSTOMER_LENDING_MARGIN_12,
        sum(case when term = 24 then EXISTING_CUSTOMER_LENDING_MARGIN else 0 end) EXISTING_CUSTOMER_LENDING_MARGIN_24,
        sum(case when term = 52 then EXISTING_CUSTOMER_LENDING_MARGIN else 0 end) EXISTING_CUSTOMER_LENDING_MARGIN_52,

        
        from ANALYTICS.FINANCE.KM_FORECAST_DATA_METRICS
        group by 1,2
    )t1
    left join 
    (   
        select week_end_date
        ,TERMUNITS
        ,sum(open_outstanding) as actual_os
        ,sum(os_12_week) as actual_os_12
        ,sum(os_24_week) as actual_os_24
        ,sum(os_52_week) as actual_os_52
        ,sum(revenue) as actual_revenue
        ,sum(revenue_12_week) as actual_revenue_12
        ,sum(revenue_24_week) as actual_revenue_24
        ,sum(revenue_52_week) as actual_revenue_52
        ,sum(net_co_os_90_dpd) as actual_nco
        ,sum(net_co_os_90_dpd_12_week) as actual_nco_12
        ,sum(net_co_os_90_dpd_24_week) as actual_nco_24
        ,sum(net_co_os_90_dpd_52_week) as actual_nco_52
        ,sum(net_yield) as actual_LENDING_MARGIN
        ,sum(net_yield_12_week) as actual_LENDING_MARGIN_12
        ,sum(net_yield_24_week) as actual_LENDING_MARGIN_24
        ,sum(net_yield_52_week) as actual_LENDING_MARGIN_52
        ,sum(case when new_cust_filter = 'New Customer' then open_outstanding else 0 end) actual_new_os
        ,sum(case when new_cust_filter = 'New Customer' then os_12_week else 0 end) actual_new_os_12
        ,sum(case when new_cust_filter = 'New Customer' then os_24_week else 0 end) actual_new_os_24
        ,sum(case when new_cust_filter = 'New Customer' then os_52_week else 0 end) actual_new_os_52
        ,sum(case when new_cust_filter = 'Existing Customer' then open_outstanding else 0 end) actual_exist_os
        ,sum(case when new_cust_filter = 'Existing Customer' then os_12_week else 0 end) actual_exist_os_12
        ,sum(case when new_cust_filter = 'Existing Customer' then os_24_week else 0 end) actual_exist_os_24
        ,sum(case when new_cust_filter = 'Existing Customer' then os_52_week else 0 end) actual_exist_os_52
        ,sum(case when new_cust_filter = 'New Customer' then revenue else 0 end) actual_new_revenue
        ,sum(case when new_cust_filter = 'New Customer' then revenue_12_week else 0 end) actual_new_revenue_12
        ,sum(case when new_cust_filter = 'New Customer' then revenue_24_week else 0 end) actual_new_revenue_24
        ,sum(case when new_cust_filter = 'New Customer' then revenue_52_week else 0 end) actual_new_revenue_52
        ,sum(case when new_cust_filter = 'Existing Customer' then revenue else 0 end) actual_exist_revenue
        ,sum(case when new_cust_filter = 'Existing Customer' then revenue_12_week else 0 end) actual_exist_revenue_12
        ,sum(case when new_cust_filter = 'Existing Customer' then revenue_24_week else 0 end) actual_exist_revenue_24
        ,sum(case when new_cust_filter = 'Existing Customer' then revenue_52_week else 0 end) actual_exist_revenue_52
        ,sum(case when new_cust_filter = 'New Customer' then net_yield else 0 end) actual_new_LENDING_MARGIN
        ,sum(case when new_cust_filter = 'New Customer' then net_yield_12_week else 0 end) actual_new_LENDING_MARGIN_12
        ,sum(case when new_cust_filter = 'New Customer' then net_yield_24_week else 0 end) actual_new_LENDING_MARGIN_24
        ,sum(case when new_cust_filter = 'New Customer' then net_yield_52_week else 0 end) actual_new_LENDING_MARGIN_52
        ,sum(case when new_cust_filter = 'Existing Customer' then net_yield else 0 end) actual_exist_LENDING_MARGIN
        ,sum(case when new_cust_filter = 'Existing Customer' then net_yield_12_week else 0 end) actual_exist_LENDING_MARGIN_12
        ,sum(case when new_cust_filter = 'Existing Customer' then net_yield_24_week else 0 end) actual_exist_LENDING_MARGIN_24
        ,sum(case when new_cust_filter = 'Existing Customer' then net_yield_52_week else 0 end) actual_exist_LENDING_MARGIN_52
        ,sum(case when new_cust_filter = 'New Customer' then net_co_os_90_dpd else 0 end) actual_new_nco
        ,sum(case when new_cust_filter = 'New Customer' then net_co_os_90_dpd_12_week else 0 end) actual_new_nco_12
        ,sum(case when new_cust_filter = 'New Customer' then net_co_os_90_dpd_24_week else 0 end) actual_new_nco_24
        ,sum(case when new_cust_filter = 'New Customer' then net_co_os_90_dpd_52_week else 0 end) actual_new_nco_52
        ,sum(case when new_cust_filter = 'Existing Customer' then net_co_os_90_dpd else 0 end) actual_exist_nco
        ,sum(case when new_cust_filter = 'Existing Customer' then net_co_os_90_dpd_12_week else 0 end) actual_exist_nco_12
        ,sum(case when new_cust_filter = 'Existing Customer' then net_co_os_90_dpd_24_week else 0 end) actual_exist_nco_24
        ,sum(case when new_cust_filter = 'Existing Customer' then net_co_os_90_dpd_52_week else 0 end) actual_exist_nco_52
        
    from INDUS.PUBLIC.loan_helper
    group by 1,2
    ) t3
    on t1.week_end_date = t3.week_end_date
    -- and lower(t1.channel) = lower(t3.TERMUNITS)
    and lower(t3.TERMUNITS) like '%' || lower(t1.channel) || '%'
)

select 
  T3.*
,  T1.actual_approvals
, T1.total_approvals
, T1.approval_difference
, T1.delta_approvals
, T1.actual_new_exposure
, T1.new_customer_exposure
, T1.new_exposure_difference
, T1.delta_new_exposure
, T2.actual_origination
, T2.total_origination
, T2.total_origination_difference
, T2.delta_total_origination
, T2.actual_origination_12
, T2.total_origination_12
, T2.total_origination_difference_12
, T2.delta_total_origination_12
, T2.actual_origination_24
, T2.total_origination_24
, T2.total_origination_difference_24
, T2.delta_total_origination_24
, T2.actual_origination_52
, T2.total_origination_52
, T2.total_origination_difference_52
, T2.delta_total_origination_52
, T2.actual_new_origination
, T2.new_customer_origination
, T2.delta_new_origination
, T2.new_origination_difference
, T2.actual_new_origination_12
, T2.new_customer_origination_12
, T2.delta_new_origination_12
, T2.new_origination_difference_12
, T2.actual_new_origination_24
, T2.new_customer_origination_24
, T2.delta_new_origination_24
, T2.new_origination_difference_24
, T2.actual_new_origination_52
, T2.new_customer_origination_52
, T2.delta_new_origination_52
, T2.new_origination_difference_52
, T2.actual_exist_origination
, T2.existing_customer_origination
, T2.exist_origination_difference
, T2.delta_exist_origination
, T2.actual_exist_origination_12
, T2.existing_customer_origination_12
, T2.exist_origination_difference_12
, T2.delta_exist_origination_12
, T2.actual_exist_origination_24
, T2.existing_customer_origination_24
, T2.exist_origination_difference_24
, T2.delta_exist_origination_24
, T2.actual_exist_origination_52
, T2.existing_customer_origination_52
, T2.exist_origination_difference_52
, T2.delta_exist_origination_52



FROM
(
    SELECT * FROM F1
)T1
LEFT JOIN 
(
    SELECT * FROM F2
)T2
on T1.week_end_date = T2.week_end_date
and t1.channel = t2.channel
LEFT JOIN 
(
    SELECT * FROM F3
)T3
on T1.week_end_date = T3.week_end_date
and t1.channel = t3.channel

);