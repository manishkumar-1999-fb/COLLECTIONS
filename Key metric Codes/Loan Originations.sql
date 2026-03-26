CREATE OR REPLACE TABLE analytics.credit.loan_originations_pb AS 
(
SELECT t1.week_end_date
     , new_cust_filter
     , bucket_group
     , termunits
     , partner
     , intuit_flow
     , nav_flow
     , national_funding_flow
     , evercommerce_flow
     , lendio_flow
     , payment_plan
     , industry_type
     , customer_annual_revenue_group
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN t1.originated_amount ELSE NULL END) originations
     , count(DISTINCT CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN t1.loan_key ELSE NULL END) num_draws
     , sum(CASE WHEN is_charged_off = 0 THEN t1.outstanding_principal_due ELSE NULL END) os
     , sum(CASE WHEN new_cust_filter = 'New Customer' AND is_charged_off = 0 THEN t1.outstanding_principal_due ELSE NULL END) new_os
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN risk_bucket*t1.ORIGINATED_AMOUNT ELSE NULL END) risk_buckets
     , sum(CASE WHEN risk_bucket IS NOT NULL AND loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN t1.ORIGINATED_AMOUNT ELSE NULL END) risk_bucket_not_null
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN fico*t1.ORIGINATED_AMOUNT ELSE NULL END) total_fico
     , sum( CASE WHEN fico IS NOT NULL AND loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN t1.ORIGINATED_AMOUNT ELSE NULL END) fico_not_null
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN vantage4*t1.ORIGINATED_AMOUNT ELSE NULL END) total_vantage
     , sum(CASE WHEN vantage4 IS NOT NULL AND loan_created_date BETWEEN week_start_date AND t1.week_end_date THEN t1.ORIGINATED_AMOUNT ELSE NULL END) vantage_not_null
     ------horizontal metrics
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd1.dpd_days_corrected > 1 AND DATEADD(DAY, 7, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd1.outstanding_principal_due ELSE NULL END) fdd1week_dpd1
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd2.dpd_days_corrected > 7 AND DATEADD(day, 14, t1.first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd2.outstanding_principal_due ELSE NULL END) fdd2week_dpd7
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd3.dpd_days_corrected > 7 AND DATEADD(day, 21, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd3.outstanding_principal_due ELSE NULL END) fdd3week_dpd7
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd4.dpd_days_corrected > 7 AND DATEADD(day, 28, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd4.outstanding_principal_due ELSE NULL END) fdd4week_dpd7
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd4.dpd_days_corrected > 14 AND DATEADD(day, 28, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd4.outstanding_principal_due ELSE NULL END) fdd4week_dpd14
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd8.dpd_days_corrected > 14 AND DATEADD(day, 56, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd8.outstanding_principal_due ELSE NULL END) fdd8week_dpd14
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date 
    			AND fdd8.dpd_days_corrected > 35 AND DATEADD(day, 56, first_planned_transmission_date) < CURRENT_DATE()
     			THEN fdd8.outstanding_principal_due ELSE NULL END) fdd8week_dpd35
     , sum(CASE WHEN loan_created_date BETWEEN week_start_date AND t1.week_end_date AND DATEADD(DAY, 42, loan_created_date)< CURRENT_DATE()
    			AND wob6.dpd_days_corrected > 7
     			THEN wob6.outstanding_principal_due ELSE NULL END) dpd7wob6
     , sum(COALESCE(fees_paid,0)) revenue
     , sum(COALESCE(fees_paid*52,0)) revenue_sum
     , sum(CASE WHEN charge_off_date BETWEEN week_start_date AND t1.week_end_date THEN t1.outstanding_principal_due ELSE 0 END) gross_co
     , sum(CASE WHEN charge_off_date BETWEEN week_start_date AND t1.week_end_date THEN t1.outstanding_principal_due * 52 ELSE 0 END) gross_co_sum
     , sum(CASE WHEN is_after_co = 1 THEN principal_paid ELSE 0 END) recoveries
     , sum(CASE WHEN is_after_co = 1 THEN principal_paid * 52 ELSE 0 END) recoveries_sum
     , gross_co - recoveries net_co
     , gross_co_sum - recoveries_sum net_co_sum
     , revenue - net_co net_yield
     , revenue_sum - net_co_sum net_yield_sum
FROM 
(SELECT loan_key
     , fbbid
     , week_end_date
     , week_start_date
     , loan_created_date
     , first_planned_transmission_date
     , is_charged_off
     , charge_off_date
     , outstanding_principal_due
     , originated_amount
     , dpd_days_corrected
     , new_cust_filter
     , bucket_group
     , risk_bucket
     , termunits
     , partner
     , intuit_flow
     , nav_flow
     , national_funding_flow
     , evercommerce_flow
     , lendio_flow
     , payment_plan
     , industry_type
     , fico
     , vantage4
     , total_paid
     , fees_paid
     , principal_paid
     , is_after_co
     , customer_annual_revenue_group
FROM 
analytics.credit.loan_level_data_pb
where sub_product <> 'mca'
) t1
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , outstanding_principal_due
     , dpd_days_corrected
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 7, first_planned_transmission_date) BETWEEN week_start_date AND week_end_date
) fdd1
ON t1.loan_key = fdd1.loan_key
--AND T1.week_end_date = fdd1.week_end_date
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , outstanding_principal_due
     , dpd_days_corrected
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 14, first_planned_transmission_date) BETWEEN week_start_date AND week_end_date
) fdd2
ON t1.loan_key = fdd2.loan_key
--AND T1.week_end_date = fdd2.week_end_date
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , outstanding_principal_due
     , dpd_days_corrected
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 21, first_planned_transmission_date) BETWEEN week_start_date AND week_end_date
) fdd3
ON t1.loan_key = fdd3.loan_key
--AND T1.week_end_date = fdd3.week_end_date
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , outstanding_principal_due
     , dpd_days_corrected
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 28, first_planned_transmission_date) BETWEEN week_start_date AND week_end_date
) fdd4
ON t1.loan_key = fdd4.loan_key
--AND T1.week_end_date = fdd4.week_end_date
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , outstanding_principal_due
     , dpd_days_corrected
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 56, first_planned_transmission_date) BETWEEN week_start_date AND week_end_date
) fdd8
ON t1.loan_key = fdd8.loan_key
--AND T1.week_end_date = fdd8.week_end_date
LEFT JOIN 
(
SELECT loan_key
     , originated_amount
     , dpd_days_corrected
     , outstanding_principal_due
     , week_end_date
FROM 
analytics.credit.loan_level_data_pb
WHERE DATEADD(DAY, 42, loan_created_date) BETWEEN week_start_date AND week_end_date
) wob6
ON t1.loan_key = wob6.loan_key
--AND T1.week_end_date = wob6.week_end_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
);
