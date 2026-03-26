CREATE OR REPLACE TABLE ANALYTICS.CREDIT.POST_CHARGEOFF_RECOVERIES_DATA AS
WITH PAYMENT_DATA AS (
SELECT FBBID
, TRANSACTION_ID 
, TRANSACTION_ACTUAL_DATE 
, TRANSACTION_PROVIDER_TYPE 
, PAYMENT_TYPE 
, LOAN_TYPE 
, LOAN_KEY
, SUM(PAYMENT_TOTAL_AMOUNT) AS total_paid
, SUM(PAYMENT_PRINCIPAL_AMOUNT) AS principal_paid
, SUM(PAYMENT_FEES_TOTAL_AMOUNT) AS total_fees_paid
, SUM(PAYMENT_BASE_FEE_AMOUNT) AS bsae_fees_paid
, SUM(PAYMENT_LATE_FEE_AMOUNT) AS late_fees_paid
, SUM(PAYMENT_NSF_FEE_AMOUNT) AS nsf_fees_paid
, SUM(PAYMENT_BREATHER_FEE_AMOUNT) AS breather_fees_paid
, SUM(PAYMENT_PAYOFF_FEE_AMOUNT) AS payoff_fees_paid
, SUM(PAYMENT_FLEXPAY_OVERDUE_FEE_AMOUNT) AS flexpay_overdue_fees_paid
, SUM(PAYMENT_DISCOUNT_FEE_AMOUNT) AS discount_applied
, SUM(PAYMENT_DISCOUNT_LATE_FEE_AMOUNT) AS late_fee_discount_applied
FROM BI.FINANCE.PAYMENTS_DATA 
WHERE PAYMENT_STATUS = 'FUND'
AND LOAN_KEY IS NOT NULL
GROUP BY 1,2,3,4,5,6,7
),
FINANCE_STATUSES_START AS (
SELECT * 
FROM BI.FINANCE.CUSTOMER_FINANCE_STATUSES_SCD_V
WHERE STATUS_NAME = 'IS_CHARGEOFF' AND STATUS_VALUE = 1 AND FIRST_ROW = 1
),
FINANCE_STATUSES_END AS (
SELECT * 
FROM BI.FINANCE.CUSTOMER_FINANCE_STATUSES_SCD_V
WHERE STATUS_NAME = 'IS_CHARGEOFF' AND LAST_ROW = 1
),
AGGREGATED_DATA AS (
SELECT CD.FBBID 
, PD.TRANSACTION_ID
, PD.TRANSACTION_ACTUAL_DATE
, CASE
    WHEN TRANSACTION_ACTUAL_DATE = CURRENT_DATE() AND DAYOFWEEK(TRANSACTION_ACTUAL_DATE) <> 3 THEN NULL
    WHEN DATEDIFF(DAY, TRANSACTION_ACTUAL_DATE, DATE_TRUNC('WEEK',CURRENT_DATE()+4)::DATE-5) < 0 THEN CURRENT_DATE()-1
    ELSE DATE_TRUNC('WEEK', TRANSACTION_ACTUAL_DATE::DATE+4)::DATE + 2
END AS TRANSACTION_WEEK_START
, CASE
    WHEN TRANSACTION_ACTUAL_DATE = CURRENT_DATE() AND DAYOFWEEK(TRANSACTION_ACTUAL_DATE) <> 3 THEN NULL
    WHEN DATEDIFF(DAY, TRANSACTION_ACTUAL_DATE, DATE_TRUNC('WEEK',CURRENT_DATE()+4)::DATE-5) < 0 THEN CURRENT_DATE()-1
    ELSE DATE_TRUNC('WEEK', TRANSACTION_ACTUAL_DATE::DATE+4)::DATE + 2
END AS TRANSACTION_WEEK_END
, PD.TRANSACTION_PROVIDER_TYPE
, PD.PAYMENT_TYPE
, PD.LOAN_TYPE
, PD.TOTAL_PAID
, PD.LOAN_KEY
, CHOF.FROM_DATE AS chargeoff_date
--, ROW_NUMBER()OVER(PARTITION BY CD.FBBID, TRANSACTION_ID ORDER BY PAYMENT_TYPE ASC) AS ROW_NUM
FROM BI."PUBLIC".CUSTOMERS_DATA AS CD
LEFT JOIN PAYMENT_DATA AS PD ON CD.FBBID = PD.FBBID
LEFT JOIN FINANCE_STATUSES_START AS CHOF ON CHOF.FBBID = CD.FBBID 
LEFT JOIN FINANCE_STATUSES_END AS CHOF_REMOVED ON CHOF_REMOVED.FBBID = CD.FBBID 
WHERE CHOF.FBBID IS NOT NULL 
AND CHOF_REMOVED.STATUS_VALUE = 1 
AND pd.total_paid IS NOT NULL 
AND pd.transaction_actual_date IS NOT NULL
AND pd.transaction_actual_date > chof.from_date
--AND TRANSACTION_ACTUAL_DATE IS NOT NULL
--QUALIFY ROW_NUM = 1
)
SELECT FBBID
, LOAN_KEY
, TRANSACTION_ACTUAL_DATE
, TRANSACTION_WEEK_END
, SUM(TOTAL_PAID) AS DEFAULT_PRINCIPAL_PAID
FROM AGGREGATED_DATA
GROUP BY 1,2,3,4;




CREATE OR REPLACE TABLE INDUS.PUBLIC.loan_helper as (



WITH sum2 as (

SELECT
f.fbbid
,f.edate
,f.sub_product
,f.TERMUNITS
, ld.new_cust_filter
, f.channel
, f.partner
, f.intuit_flow
, f.national_funding_flow
, f.nav_flow
, f.lendio_flow
, CASE
	WHEN LD.new_cust_filter = 'New Customer' AND ld.ob_bucket_group_dal IN ('OB: 11-12','OB: 13+') THEN 'OB: 11+'
	WHEN LD.new_cust_filter = 'New Customer' THEN ld.ob_bucket_group_dal
	WHEN LD.new_cust_filter = 'Existing Customer' AND ld.og_bucket_group IN ('OG: 11-12','OG: 13+') THEN 'OG: 11+'
	WHEN LD.new_cust_filter = 'Existing Customer' THEN ld.og_bucket_group
END bucket_group
, CASE 
		WHEN ld.new_cust_filter = 'New Customer' THEN ld.ob_bucket_group_dal
		WHEN ld.new_cust_filter = 'Existing Customer' THEN ld.og_bucket_group
	END bucket_group_PORT
, ld.loan_key
, ld.OUTSTANDING_PRINCIPAL_DUE
, ld.payment_plan
, ld.dpd_days_corrected_fmd
, ld.is_charged_off
, b.originated_amount
, b.co_os_90_dpd
, c.DEFAULT_PRINCIPAL_PAID

FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f

LEFT JOIN 
(SELECT fmd.edate, fmd.loan_key, fmd.fbbid, fmd.OUTSTANDING_PRINCIPAL_DUE,
		CASE 
			WHEN fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%12%' THEN '12 Week'
			WHEN fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%24%' THEN '24 Week'
			WHEN fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%52%' THEN '52 Week'
			ELSE 'Others'
		END payment_plan
		, CASE 
			WHEN fmd.dpd_days IS NULL THEN 0 
			ELSE fmd.dpd_days
		END dpd_days_corrected_fmd
		, fmd.IS_CHARGED_OFF 
    , f1.new_cust_filter
    , f1.ob_bucket_group_dal
    , f1.og_bucket_group

	FROM (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' ) fmd
	--LEFT JOIN INDUS."PUBLIC".DAILY_LOAN_DATA_INDUS dld
	--ON fmd.loan_key = dld.loan_key
	--AND fmd.edate = dld.edate 
  LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f1
  ON fmd.fbbid = f1.fbbid
  AND fmd.loan_created_date = f1.edate) ld
 -- WHERE fmd.PRODUCT_TYPE <> 'Flexpay') ld

ON f.fbbid = ld.fbbid
AND f.edate = ld.edate 

left join (
SELECT
    fmd.fbbid,
	  fmd.loan_key,
    CASE 
		WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', fmd.edate::date+4)::date+2
		WHEN datediff('day', fmd.edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', fmd.edate, current_date()) <= 0 THEN NULL 
		WHEN datediff('day', fmd.edate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
		ELSE DATE_TRUNC('WEEK', fmd.edate::date+4)::date+2
	  END week_of_edate,
    --f1.new_cust_filter,
	  sum(case when fmd.edate = fmd.loan_created_date::date then originated_amount else 0 end) originated_amount, 
	  sum(case when fmd.edate = fmd.CHARGE_OFF_DATE then CHARGEOFF_PRINCIPAL else 0 end) co_os_90_dpd

    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd

    WHERE PRODUCT_TYPE <> 'Flexpay'
    GROUP BY 1, 2, 3
    ORDER BY 1 desc) b

on ld.loan_key = b.loan_key
and LD.edate = b.week_of_edate

LEFT JOIN ANALYTICS.CREDIT.POST_CHARGEOFF_RECOVERIES_DATA AS c
ON ld.loan_key = c.loan_key
AND ld.edate  = c.TRANSACTION_WEEK_END

LEFT JOIN (
  SELECT loan_key
     , loan_operational_status
     , ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate desc) rnk
FROM (
SELECT * FROM BI.FINANCE.FINANCE_METRICS_DAILY 
WHERE PRODUCT_TYPE <> 'Flexpay' )
QUALIFY rnk = 1
) TLD
ON LD.LOAN_KEY = TLD.LOAN_KEY

WHERE TRUE
AND f.sub_product <> 'Credit Builder'
AND f.sub_product <> 'mca'
AND (tld.loan_operational_status <> 'CNCL' OR tld.loan_operational_status IS NULL))

,sum3 as 
(

SELECT

f.fbbid
,f.edate
,f.sub_product
--, ld.new_cust_filter
, f.channel
, f.TERMUNITS
, f.partner
, f.intuit_flow
, f.national_funding_flow
, f.nav_flow
, f.lendio_flow
, CASE
	WHEN LD.new_cust_filter = 'New Customer' AND ld.ob_bucket_group_dal IN ('OB: 11-12','OB: 13+') THEN 'OB: 11+'
	WHEN LD.new_cust_filter = 'New Customer' THEN ld.ob_bucket_group_dal
	WHEN LD.new_cust_filter = 'Existing Customer' AND ld.og_bucket_group IN ('OG: 11-12','OG: 13+') THEN 'OG: 11+'
	WHEN LD.new_cust_filter = 'Existing Customer' THEN ld.og_bucket_group
END bucket_group
, CASE 
		WHEN ld.new_cust_filter = 'New Customer' THEN ld.ob_bucket_group_dal
		WHEN ld.new_cust_filter = 'Existing Customer' THEN ld.og_bucket_group
	END bucket_group_PORT
, ld.loan_key
, ld.REVENUE
, ld.payment_plan
, ld.new_cust_filter
--, ld.dpd_days_corrected_fmd
--, ld.is_charged_off
--, b.originated_amount
--, b.co_os_90_dpd
--, c.DEFAULT_PRINCIPAL_PAID
--, e.revenue
--, b.co_os_90_dpd
--, c.DEFAULT_PRINCIPAL_PAID
FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f

LEFT JOIN (
    SELECT 
        ld1.revenue,
        ld1.loan_key,
        ld1.fbbid,
        ld1.payment_plan,
        ld1.week_of_edate,
        f1.new_cust_filter,
        f1.ob_bucket_group_dal, f1.og_bucket_group

    FROM (
        SELECT 
            fls.loan_key,
            fls.fbbid,
            ffd.loan_created_date,
          --  f1.new_cust_filter,
            CASE 
                WHEN DAYOFWEEK(CURRENT_DATE()) = 3 THEN DATE_TRUNC('WEEK', fls.edate::DATE + 4)::DATE + 2
                WHEN DATEDIFF('day', fls.edate, DATE_TRUNC('WEEK', CURRENT_DATE() + 4)::DATE - 5) < 0 
                     AND DATEDIFF('day', fls.edate, CURRENT_DATE()) <= 0 THEN NULL 
                WHEN DATEDIFF('day', fls.edate, DATE_TRUNC('WEEK', CURRENT_DATE() + 4)::DATE - 5) < 0 THEN CURRENT_DATE() - 1
                ELSE DATE_TRUNC('WEEK', fls.edate::DATE + 4)::DATE + 2
            END AS week_of_edate,
            CASE 
                WHEN ffd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%12%' THEN '12 Week'
                WHEN ffd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%24%' THEN '24 Week'
                WHEN ffd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION LIKE '%52%' THEN '52 Week'
                ELSE 'Others'
            END AS payment_plan,
            SUM(CASE WHEN fls.STATUS_NAME = 'REVENUE' THEN fls.STATUS_VALUE ELSE 0 END) AS revenue
        FROM 
            BI.FINANCE.LOAN_STATUSES fls
        LEFT JOIN (
            SELECT 
                LOAN_KEY, 
                ORIGINAL_PAYMENT_PLAN_DESCRIPTION,
                MAX(LOAN_CREATED_DATE) AS loan_created_date
            FROM 
                BI.FINANCE.FINANCE_METRICS_DAILY 
            WHERE 
                PRODUCT_TYPE <> 'Flexpay'
            GROUP BY 1,2
        ) ffd ON fls.loan_key = ffd.loan_key
        GROUP BY 1,2,3,4,5
    ) ld1
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f1
    ON ld1.fbbid = f1.fbbid
    AND ld1.loan_created_date = f1.edate
) ld
ON f.fbbid = ld.fbbid
AND f.edate = ld.week_of_edate
where TRUE
AND f.sub_product <> 'Credit Builder'
AND f.sub_product <> 'mca')


SELECT 
T2.*

--,T2.open_outstanding
--,T2.originations
,T3.REVENUE
--,T2.co_os_90_dpd
--,T2.co_os_90_dpd_52
,T3.revenue_52
, T3.revenue_52_12_week
, T3.revenue_52_24_week
, T3.revenue_52_52_week
, T3.revenue_52_others
, T3.revenue_12_week
, T3.revenue_24_week
, T3.revenue_52_week
, T3.revenue_others
, COALESCE(T3.revenue,0) - COALESCE(T2.co_os_90_dpd,0) + COALESCE(T2.principal_recoveries,0) AS net_yield
, (COALESCE(T3.revenue,0) - COALESCE(T2.co_os_90_dpd,0) + COALESCE(T2.principal_recoveries,0)) * 52 AS net_yield_52
, (COALESCE(T3.revenue_12_week,0) - COALESCE(T2.co_os_90_dpd_12_week,0) + COALESCE(T2.principal_recoveries_12_week,0)) AS net_yield_12_week
, (COALESCE(T3.revenue_24_week,0) - COALESCE(T2.co_os_90_dpd_24_week,0) + COALESCE(T2.principal_recoveries_24_week,0)) AS net_yield_24_week
, (COALESCE(T3.revenue_52_week,0) - COALESCE(T2.co_os_90_dpd_52_week,0) + COALESCE(T2.principal_recoveries_52_week,0)) AS net_yield_52_week
, (COALESCE(T3.revenue_others,0) - COALESCE(T2.co_os_90_dpd_others,0) + COALESCE(T2.principal_recoveries_others,0)) AS net_yield_others

, (COALESCE(T3.revenue_52_12_week,0) - COALESCE(T2.co_os_90_dpd_52_12_week,0) + COALESCE(T2.principal_recoveries_12_week,0)) AS net_yield_52_12_week
, (COALESCE(T3.revenue_52_24_week,0) - COALESCE(T2.co_os_90_dpd_52_24_week,0) + COALESCE(T2.principal_recoveries_24_week,0)) AS net_yield_52_24_week
, (COALESCE(T3.revenue_52_52_week,0) - COALESCE(T2.co_os_90_dpd_52_52_week,0) + COALESCE(T2.principal_recoveries_52_week,0)) AS net_yield_52_52_week
, (COALESCE(T3.revenue_52_others,0) - COALESCE(T2.co_os_90_dpd_52_others,0) + COALESCE(T2.principal_recoveries_others,0)) AS net_yield_52_others



FROM (

    SELECT	edate week_end_date
--,payment_plan
,sub_product
, new_cust_filter
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
,bucket_group_PORT
,bucket_group
, sum(case when dpd_days_corrected_fmd  BETWEEN 0 AND 91 and IS_CHARGED_OFF = 0 then OUTSTANDING_PRINCIPAL_DUE else 0 end) open_outstanding
,SUM(case when dpd_days_corrected_fmd  BETWEEN 0 AND 91 and IS_CHARGED_OFF = 0 THEN (CASE WHEN payment_plan = '12 Week' THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) ELSE 0 END) os_12_week
,SUM(case when dpd_days_corrected_fmd  BETWEEN 0 AND 91 and IS_CHARGED_OFF = 0 THEN (CASE WHEN payment_plan = '24 Week' THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) ELSE 0 END) os_24_week
,SUM(case when dpd_days_corrected_fmd  BETWEEN 0 AND 91 and IS_CHARGED_OFF = 0 THEN (CASE WHEN payment_plan = '52 Week' THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) ELSE 0 END) os_52_week
,SUM(case when dpd_days_corrected_fmd  BETWEEN 0 AND 91 and IS_CHARGED_OFF = 0 THEN (CASE WHEN payment_plan = 'Others' THEN OUTSTANDING_PRINCIPAL_DUE ELSE 0 END) ELSE 0 END) os_others

, sum(originated_amount) originations
-- Originations by Payment Plan
, SUM(CASE 
         WHEN payment_plan = '12 Week' THEN originated_amount 
        ELSE 0 
      END) AS originations_12_week

, SUM(CASE 
         WHEN payment_plan = '24 Week' THEN originated_amount 
        ELSE 0 
      END) AS originations_24_week

, SUM(CASE 
         WHEN payment_plan = '52 Week' THEN originated_amount 
        ELSE 0 
      END) AS originations_52_week

, SUM(CASE 
         WHEN payment_plan = 'Others' THEN originated_amount 
        ELSE 0 
      END) AS originations_others

, sum(COALESCE(co_os_90_dpd,0)) co_os_90_dpd

-- CO OS 90 DPD by Payment Plan
, SUM(CASE 
        WHEN payment_plan = '12 Week' THEN COALESCE(co_os_90_dpd, 0) 
        ELSE 0 
      END) AS co_os_90_dpd_12_week

, SUM(CASE 
        WHEN payment_plan = '24 Week' THEN COALESCE(co_os_90_dpd, 0) 
        ELSE 0 
      END) AS co_os_90_dpd_24_week

, SUM(CASE 
        WHEN payment_plan = '52 Week' THEN COALESCE(co_os_90_dpd, 0) 
        ELSE 0 
      END) AS co_os_90_dpd_52_week

, SUM(CASE 
        WHEN payment_plan = 'Others' THEN COALESCE(co_os_90_dpd, 0) 
        ELSE 0 
      END) AS co_os_90_dpd_others


, sum(COALESCE(co_os_90_dpd,0))*52 co_os_90_dpd_52

-- CO OS 90 DPD (52 Weeks) by Payment Plan
, (SUM(CASE 
             WHEN payment_plan = '12 Week' THEN COALESCE(co_os_90_dpd, 0)
            ELSE 0 
          END)) * 52 AS co_os_90_dpd_52_12_week

, (SUM(CASE 
             WHEN payment_plan = '24 Week' THEN COALESCE(co_os_90_dpd, 0)
            ELSE 0 
          END)) * 52 AS co_os_90_dpd_52_24_week
, (SUM(CASE 
             WHEN payment_plan = '52 Week' THEN COALESCE(co_os_90_dpd, 0)
            ELSE 0 
          END)) * 52 AS co_os_90_dpd_52_52_week
, (SUM(CASE 
             WHEN payment_plan = 'Others' THEN COALESCE(co_os_90_dpd, 0)
            ELSE 0 
          END)) * 52 AS co_os_90_dpd_52_others

, sum(COALESCE(DEFAULT_PRINCIPAL_PAID,0)) principal_recoveries
, SUM(CASE 
        WHEN payment_plan = '12 Week' THEN COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS principal_recoveries_12_week
, SUM(CASE 
        WHEN payment_plan = '24 Week' THEN COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS principal_recoveries_24_week
, SUM(CASE 
        WHEN payment_plan = '52 Week' THEN COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS principal_recoveries_52_week
, SUM(CASE 
        WHEN payment_plan = 'Others' THEN COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS principal_recoveries_others

, sum(COALESCE(co_os_90_dpd,0)) - sum(COALESCE(DEFAULT_PRINCIPAL_PAID,0)) net_co_os_90_dpd
, SUM(CASE 
        WHEN payment_plan = '12 Week' THEN COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS net_co_os_90_dpd_12_week
, SUM(CASE 
        WHEN payment_plan = '24 Week' THEN COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS net_co_os_90_dpd_24_week
, SUM(CASE 
        WHEN payment_plan = '52 Week' THEN COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS net_co_os_90_dpd_52_week
, SUM(CASE 
        WHEN payment_plan = 'Others' THEN COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)
        ELSE 0 
      END) AS net_co_os_90_dpd_others

, (sum(COALESCE(co_os_90_dpd,0)) - sum(COALESCE(DEFAULT_PRINCIPAL_PAID,0)))*52 net_co_os_90_dpd_52
, SUM(CASE 
        WHEN payment_plan = '12 Week' THEN (COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)) * 52
        ELSE 0 
      END) AS net_co_os_90_dpd_52_12_week
, SUM(CASE 
        WHEN payment_plan = '24 Week' THEN (COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)) * 52
        ELSE 0 
      END) AS net_co_os_90_dpd_52_24_week
, SUM(CASE 
        WHEN payment_plan = '52 Week' THEN (COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)) * 52
        ELSE 0 
      END) AS net_co_os_90_dpd_52_52_week
, SUM(CASE 
        WHEN payment_plan = 'Others' THEN (COALESCE(co_os_90_dpd,0) - COALESCE(DEFAULT_PRINCIPAL_PAID,0)) * 52
        ELSE 0 
      END) AS net_co_os_90_dpd_52_others

FROM sum2
group BY 1,2,3,4,5,6,7,8,9,10,11,12
)T2

LEFT JOIN(
SELECT	edate week_end_date
--,payment_plan
,sub_product
, new_cust_filter
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
,bucket_group_PORT
,bucket_group

,sum(COALESCE(revenue,0)) revenue

-- Revenue by Payment Plan
, SUM(CASE 
         WHEN payment_plan = '12 Week' THEN COALESCE(revenue, 0) 
        ELSE 0 
      END) AS revenue_12_week

, SUM(CASE 
         WHEN payment_plan = '24 Week' THEN COALESCE(revenue, 0) 
        ELSE 0 
      END) AS revenue_24_week

, SUM(CASE 
         WHEN payment_plan = '52 Week' THEN COALESCE(revenue, 0) 
        ELSE 0 
      END) AS revenue_52_week

, SUM(CASE 
         WHEN payment_plan = 'Others' THEN COALESCE(revenue, 0) 
        ELSE 0 
      END) AS revenue_others

,sum(COALESCE(revenue,0))*52 revenue_52

-- Revenue (52 Weeks) by Payment Plan
, (SUM(CASE 
             WHEN payment_plan = '12 Week' THEN COALESCE(revenue, 0)
            ELSE 0 
          END)) * 52 AS revenue_52_12_week

, (SUM(CASE 
             WHEN payment_plan = '24 Week' THEN COALESCE(revenue, 0)
            ELSE 0 
          END)) * 52 AS revenue_52_24_week
, (SUM(CASE 
             WHEN payment_plan = '52 Week' THEN COALESCE(revenue, 0)
            ELSE 0 
          END)) * 52 AS revenue_52_52_week
, (SUM(CASE 
             WHEN payment_plan = 'Others' THEN COALESCE(revenue, 0)
            ELSE 0 
          END)) * 52 AS revenue_52_others
FROM sum3
group BY 1,2,3,4,5,6,7,8,9,10,11,12
)T3

ON t2.week_end_date = t3.week_end_date
AND t2.new_cust_filter = t3.new_cust_filter
AND t2.bucket_group = t3.bucket_group
AND t2.bucket_group_port = t3.bucket_group_port
AND t2.channel = t3.channel
AND t2.TERMUNITS = t3.TERMUNITS
AND t2.partner = t3.partner
AND t2.intuit_flow = t3.intuit_flow
AND t2.national_funding_flow = t3.national_funding_flow
AND t2.nav_flow = t3.nav_flow
AND t2.lendio_flow = t3.lendio_flow 
AND t2.sub_product = t3.sub_product 
where TRUE
AND (( t2.week_end_date = current_date()-1 AND dayofweek(current_date()) <> 3) OR dayofweek(t2.week_end_date) = 3)
);
