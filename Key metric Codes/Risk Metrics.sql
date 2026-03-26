CREATE OR REPLACE TABLE analytics.credit.risk_tab_km AS 
SELECT week_end_date
, new_cust_filter
, bucket_group
, INDUSTRY_type
, TERMUNITS
, PARTNER
, national_funding_flow
, PAYMENT_PLAN
, AUW_SEGMENT
, IS_AUW
, Loan_TENURE_BUCKET
, customer_annual_revenue_group
----------------------------------------------------
--
--
, SUM(os_0) os_dpd_0
, SUM(os_p_0) os_dpd_0_old
, SUM(os_1_7) os_dpd_1_7
, SUM(CASE WHEN os_p_0>0 THEN os_1_7 ELSE 0 END) os_dpd_1_7_new
, SUM(os_p_1_7) os_dpd_1_7_old
, SUM(os_8_14) os_dpd_8_14
, SUM(CASE WHEN os_p_1_7>0 THEN os_8_14 ELSE 0 END) os_dpd_8_14_new
, SUM(os_p_8_14) os_dpd_8_14_old
, SUM(os_15_21) os_dpd_15_21
, SUM(CASE WHEN os_p_8_14>0 THEN os_15_21 ELSE 0 END) os_dpd_15_21_new
, SUM(os_p_15_21) os_dpd_15_21_old
, SUM(os_22_28) os_dpd_22_28
, SUM(CASE WHEN os_p_15_21>0 THEN os_22_28 ELSE 0 END) os_dpd_22_28_new
, SUM(os_p_22_28) os_dpd_22_28_old
, SUM(os_29_35) os_dpd_29_35
, SUM(CASE WHEN os_p_22_28>0 THEN os_29_35 ELSE 0 END) os_dpd_29_35_new
, SUM(os_p_29_35) os_dpd_29_35_old
, SUM(os_36_42) os_dpd_36_42
, SUM(CASE WHEN os_p_29_35>0 THEN os_36_42 ELSE 0 END) os_dpd_36_42_new
, SUM(os_p_36_42) os_dpd_36_42_old
, SUM(os_43_49) os_dpd_43_49
, SUM(CASE WHEN os_p_36_42>0 THEN os_43_49 ELSE 0 END) os_dpd_43_49_new
, SUM(os_p_43_49) os_dpd_43_49_old
, SUM(os_50_56) os_dpd_50_56
, SUM(CASE WHEN os_p_43_49>0 THEN os_50_56 ELSE 0 END) os_dpd_50_56_new
, SUM(os_p_50_56) os_dpd_50_56_old
, SUM(os_57_63) os_dpd_57_63
, SUM(CASE WHEN os_p_50_56>0 THEN os_57_63 ELSE 0 END) os_dpd_57_63_new
, SUM(os_p_57_63) os_dpd_57_63_old
, SUM(os_64_70) os_dpd_64_70
, SUM(CASE WHEN os_p_57_63>0 THEN os_64_70 ELSE 0 END) os_dpd_64_70_new
, SUM(os_p_64_70) os_dpd_64_70_old
, SUM(os_71_77) os_dpd_71_77
, SUM(CASE WHEN os_p_64_70>0 THEN os_71_77 ELSE 0 END) os_dpd_71_77_new
, SUM(os_p_71_77) os_dpd_71_77_old
, SUM(os_78_84) os_dpd_78_84
, SUM(CASE WHEN os_p_71_77>0 THEN os_78_84 ELSE 0 END) os_dpd_78_84_new
, SUM(os_p_78_84) os_dpd_78_84_old
, SUM(os_85_90) os_dpd_85_90
, SUM(CASE WHEN os_p_78_84>0 THEN os_85_90 ELSE 0 END) os_dpd_85_90_new
, SUM(os_85_91) os_dpd_85_91
, SUM(CASE WHEN os_p_78_84>0 THEN os_85_91 ELSE 0 END) os_dpd_85_91_new
, SUM(os_15_35) os_dpd_15_35
, SUM(os_36_63) os_dpd_36_63
, SUM(os_64_90) os_dpd_64_90
, SUM(os_p_84_90) os_dpd_84_90_old
, SUM(os_p_85_90) os_dpd_85_90_old
, SUM(os_p_64_90) os_dpd_64_90_old
, sum(os_91_from_64_90) os_CO_from_64_90
, sum(os_91_from_85_90) os_CO_from_85_90
, SUM(os_91) os_co
, SUM(os_91_new) os_co_new
, SUM(os_0_90) os_dpd_0_90
, SUM(os_1_90) os_dpd_1_90
, sum(os_p_0_90) os_dpd_0_90_old
--
--
, count( f_0) num_dpd_0
, count( f_p_0) num_dpd_0_old
, count( f_1_7) num_dpd_1_7
, count( CASE WHEN os_p_0>0 AND os_1_7>0 THEN loan_key ELSE NULL END) num_dpd_1_7_new
, count( f_p_1_7) num_dpd_1_7_old
, count( f_8_14) num_dpd_8_14
, count( CASE WHEN os_p_1_7>0 AND os_8_14>0 THEN loan_key ELSE NULL END) num_dpd_8_14_new
, count( f_p_8_14) num_dpd_8_14_old
, count( f_15_21) num_dpd_15_21
, count( CASE WHEN os_p_8_14>0 AND os_15_21>0 THEN loan_key ELSE NULL END) num_dpd_15_21_new
, count( f_p_15_21) num_dpd_15_21_old
, count( f_22_28) num_dpd_22_28
, count( CASE WHEN os_p_15_21>0 AND os_22_28>0 THEN loan_key ELSE NULL END) num_dpd_22_28_new
, count( f_p_22_28) num_dpd_22_28_old
, count( f_29_35) num_dpd_29_35
, count( CASE WHEN os_p_22_28>0 AND os_29_35>0 THEN loan_key ELSE NULL END) num_dpd_29_35_new
, count( f_p_29_35) num_dpd_29_35_old
, count( f_36_42) num_dpd_36_42
, count( CASE WHEN os_p_29_35>0 AND os_36_42>0 THEN loan_key ELSE NULL END) num_dpd_36_42_new
, count( f_p_36_42) num_dpd_36_42_old
, count( f_43_49) num_dpd_43_49
, count( CASE WHEN os_p_36_42>0 AND os_43_49>0 THEN loan_key ELSE NULL END) num_dpd_43_49_new
, count( f_p_43_49) num_dpd_43_49_old
, count( f_50_56) num_dpd_50_56
, count( CASE WHEN os_p_43_49>0 AND os_50_56>0 THEN loan_key ELSE NULL END) num_dpd_50_56_new
, count( f_p_50_56) num_dpd_50_56_old
, count( f_57_63) num_dpd_57_63
, count( CASE WHEN os_p_50_56>0 AND os_57_63>0 THEN loan_key ELSE NULL END) num_dpd_57_63_new
, count( f_p_57_63) num_dpd_57_63_old
, count( f_64_70) num_dpd_64_70
, count( CASE WHEN os_p_57_63>0 AND os_64_70>0 THEN loan_key ELSE NULL END) num_dpd_64_70_new
, count( f_p_64_70) num_dpd_64_70_old
, count( f_71_77) num_dpd_71_77
, count( CASE WHEN os_p_64_70>0 AND os_71_77>0 THEN loan_key ELSE NULL END) num_dpd_71_77_new
, count( f_p_71_77) num_dpd_71_77_old
, count( f_78_84) num_dpd_78_84
, count( CASE WHEN os_p_71_77>0 AND os_78_84>0 THEN loan_key ELSE NULL END) num_dpd_78_84_new
, count( f_p_78_84) num_dpd_78_84_old
, count( f_85_90) num_dpd_85_90
, count( CASE WHEN os_p_78_84>0 AND os_85_90>0 THEN loan_key ELSE NULL END) num_dpd_85_90_new
, count( f_85_91) num_dpd_85_91
, count( CASE WHEN os_p_78_84>0 AND os_85_91>0 THEN loan_key ELSE NULL END) num_dpd_85_91_new
, count( f_15_35) num_dpd_15_35
, count( f_36_63) num_dpd_36_63
, count( f_64_90) num_dpd_64_90
, count( f_p_85_90) num_dpd_85_90_old
, count( f_P_64_90) num_dpd_64_90_Old
, count( f_91_from_64_90) num_co_from_64_90
, count( f_91_from_85_90) num_co_from_85_90
, count( f_p_84_90) num_dpd_84_90_old
, count( f_91) num_co
, count( f_91_new) num_co_new
, count( f_0_90) num_dpd_0_90
, count( f_1_90) num_dpd_1_90
FROM analytics.credit.loan_level_data_pb
WHERE SUB_PRODUCT <> 'mca'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1 DESC,2,3,4,5,6,7,8,9,10,11
-----------------------------------------------------
--
;

