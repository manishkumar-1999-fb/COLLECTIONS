CREATE OR REPLACE TABLE analytics.credit.summary_td AS 
(
SELECT week_end_date
     , new_cust_filter
     , payment_plan
     , termunits
     , customer_annual_revenue_group
     , COUNT(DISTINCT CASE WHEN os_1_7>0 THEN loan_key ELSE NULL END) num_delq_1_7 //# Total DPD 1-7 draws
     
     , COUNT(DISTINCT CASE WHEN os_p_0>0  AND os_1_7>0 THEN loan_key ELSE NULL END) num_dpd_1_7_new // # Draws from DPD 0 -> DPD 1-7 num
     , COUNT(DISTINCT CASE WHEN os_p_0>0 THEN loan_key ELSE NULL END) num_dpd_0_old // # Draws from DPD 0 -> DPD 1-7 denom

     , SUM(CASE WHEN os_p_0>0  AND os_1_7>0 THEN os_1_7 ELSE NULL END) dollar_dpd_1_7_new // $ OS from DPD 0 -> DPD 1-7
     , SUM(CASE WHEN os_p_0>0 THEN os_p_0 ELSE NULL END) dollar_dpd_0_old // %$ Roll Rate from DPD 0 -> DPD 1-7 denom

     , SUM(CASE WHEN (loan_created_date BETWEEN week_start_date AND week_end_date) 
                AND bucket_group = 'OG: 13+' THEN originated_amount ELSE 0 END) og_13_orig_num// # 13+ orig num
     , SUM(CASE WHEN (loan_created_date BETWEEN week_start_date AND week_end_date) THEN originated_amount ELSE 0 END) og_13_orig_denom // # 13+ orig denom (total #orig)

     , COUNT(DISTINCT CASE WHEN os_1_90>0 THEN loan_key ELSE NULL end) num_delq_draws //# Total DPD 1-90 Draws (also % # DPD 1-90 Draws numerator)
     , COUNT(DISTINCT CASE WHEN outstanding_principal_due>0 THEN loan_key ELSE NULL END) num_draws // % # DPD 1-90 Draws denominator

     , COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date
                            AND week_end_date THEN fbbid ELSE NULL END) new_co_accts // New CO accounts 
     , COUNT(DISTINCT CASE WHEN charge_off_date BETWEEN week_start_date
                            AND week_end_date THEN loan_key ELSE NULL END) new_co_draws // New CO draws

     , COUNT(DISTINCT CASE WHEN outstanding_principal_due>0 THEN loan_key ELSE NULL END) active_draws // denom for % # New CO (% of Active draws)

     , SUM(CASE WHEN is_charged_off=1 AND (charge_off_date BETWEEN week_start_date AND week_end_date) 
                THEN outstanding_principal_due ELSE 0 END) new_os_co //$ New OS in CO (assuming this is wrt originations by new customers) --------- % $ New CO (Percent of Active OS) numerator

     , SUM(CASE WHEN dpd_days_corrected < 98 AND is_charged_off=0 THEN outstanding_principal_due ELSE 0 END) active_os // Denominator for % $ New CO (% of Active OS)

FROM analytics.credit.loan_level_data_pb
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4);

CREATE OR REPLACE TABLE ANALYTICS.CREDIT.SUMMARY_ACQ_TD AS (

with sum1 as (

SELECT week_end_date,
    industry_type,
    termunits,
    customer_annual_revenue_group,
    sum(active_os) open_outstanding,
    sum(exposure_active_customers) exposure_active_customers
    from analytics.credit.customer_management_AGG_pb
    GROUP BY 1,2,3,4
)


,sum2 as (
    select week_end_date,
    industry_type,
    termunits,
    customer_revenue_group,
    sum(REGISTRATIONS) REGISTRATIONS,
    sum(UNDERWRITTEN) UNDERWRITTEN,
    sum(APPROVALS) APPROVALS,
    sum(FTDS) FTDS,
    sum(FTDS_7) FTDS_7,
    sum(FTDS_28) FTDS_28,
    sum(SUM_FICO_UND) SUM_FICO_UND,
    sum(SUM_FICO_APP) SUM_FICO_APP,
    from INDUS.PUBLIC.ACQUISITIONS_AGG_TD
    GROUP BY 1,2,3,4
)
select 
T2.*,
T1.open_outstanding,
T1.exposure_active_customers
FROM SUM1 T1
LEFT JOIN SUM2 T2
ON T1.WEEK_END_DATE = T2.WEEK_END_DATE
AND T1.industry_type = T2.industry_type
AND T1.TERMUNITS = T2.TERMUNITS
AND T1.customer_annual_revenue_group = T2.customer_revenue_group

);

