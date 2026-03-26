CREATE OR REPLACE TABLE INDUS.PUBLIC.CUSTOM_BREATHER AS (
WITH RECURSIVE week_cte AS (
    SELECT
        DATE '2020-12-31' AS week_start_date,
        DATE '2021-01-06' AS week_end_date

    UNION ALL

    SELECT
        DATEADD(DAY, 7, week_start_date),
        DATEADD(DAY, 7, week_end_date)
    FROM week_cte
    WHERE week_end_date < CURRENT_DATE - INTERVAL '7 day'
)
, aggregated_breather_data AS (
    SELECT
        t2.week_end_date,
        COALESCE(F.TERMUNITS, 'UNKNOWN') AS TERMUNITS_KEY,
        COALESCE(F.PARTNER, 'UNKNOWN') AS PARTNER_KEY,
        SUM(t1.event_json:breather_principal_amount::NUMBER(18, 2)) AS total_breather_principal,
        COUNT(DISTINCT t1.fbbid) AS distinct_breather_fbbids,
        AVG(t1.event_json:breather_days::NUMBER / 7.0) AS avg_breather_duration_weeks -- Updated calculation
    FROM bi.finance.DIM_BREATHER_EVENT t1
    JOIN week_cte t2 ON t1.from_date BETWEEN t2.week_start_date AND t2.week_end_date
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 F ON T1.FBBID = F.FBBID AND F.EDATE = F.min_edate
    GROUP BY t2.week_end_date, TERMUNITS_KEY, PARTNER_KEY
)
, aggregated_custom_plan_data AS (
    SELECT
        t2.week_end_date,
        COALESCE(F.TERMUNITS, 'UNKNOWN') AS TERMUNITS_KEY,
        COALESCE(F.PARTNER, 'UNKNOWN') AS PARTNER_KEY,
        SUM(t1.PAYMENT_PLAN_EXPECTED_PRINCIPAL) AS total_custom_plan_principal,
        COUNT(DISTINCT t1.FBBID) AS distinct_custom_plan_fbbids,
        AVG(
            CASE
                WHEN UPPER(t1.time_units) = 'DAY' THEN t1.duration / 7.0
                WHEN UPPER(t1.time_units) = 'WEEK' THEN t1.duration * 1.0 -- ensure numeric
                WHEN UPPER(t1.time_units) = 'MONTH' THEN t1.duration * (365.25 / 12.0 / 7.0) -- Avg weeks in a month
                ELSE NULL -- Or 0, if you prefer to not ignore unknown units in AVG
            END
        ) AS avg_custom_plan_duration_weeks
    FROM bi.FINANCE.DIM_PAYMENT_PLAN t1
    JOIN week_cte t2 ON t1.PAYMENT_PLAN_START_DATE BETWEEN t2.week_start_date AND t2.week_end_date
    LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 F ON T1.FBBID = F.FBBID AND F.EDATE = F.min_edate
    WHERE t1.is_custom_plan = 1
    GROUP BY t2.week_end_date, TERMUNITS_KEY, PARTNER_KEY
)
, all_keys AS (
    SELECT week_end_date, TERMUNITS_KEY, PARTNER_KEY FROM aggregated_breather_data
    UNION
    SELECT week_end_date, TERMUNITS_KEY, PARTNER_KEY FROM aggregated_custom_plan_data
)
SELECT
    ak.week_end_date,
    ak.TERMUNITS_KEY AS TERMUNITS,
    ak.PARTNER_KEY AS PARTNER,
    --1 AS ONE, -- This line is commented out as per your latest version

    -- Breather Metrics
    COALESCE(abd.total_breather_principal, 0) AS breather_total_principal_amount,
    COALESCE(abd.distinct_breather_fbbids, 0) AS breather_distinct_fbbid_count,
    COALESCE(abd.avg_breather_duration_weeks, 0) AS breather_avg_duration_weeks,

    -- Custom Plan Metrics
    COALESCE(acpd.total_custom_plan_principal, 0) AS custom_plan_total_principal_amount,
    COALESCE(acpd.distinct_custom_plan_fbbids, 0) AS custom_plan_distinct_fbbid_count,
    COALESCE(acpd.avg_custom_plan_duration_weeks, 0) AS custom_plan_avg_duration_weeks

FROM all_keys ak
LEFT JOIN aggregated_breather_data abd
    ON ak.week_end_date = abd.week_end_date
    AND ak.TERMUNITS_KEY = abd.TERMUNITS_KEY
    AND ak.PARTNER_KEY = abd.PARTNER_KEY
LEFT JOIN aggregated_custom_plan_data acpd
    ON ak.week_end_date = acpd.week_end_date
    AND ak.TERMUNITS_KEY = acpd.TERMUNITS_KEY
    AND ak.PARTNER_KEY = acpd.PARTNER_KEY
ORDER BY ak.week_end_date, ak.TERMUNITS_KEY, ak.PARTNER_KEY
);

-----------------------------------------------OTHER METRICS--------------------------------------------------------------

CREATE OR REPLACE TABLE INDUS.PUBLIC.COLLECTIONS_AR AS (
WITH ReportingCutoffDate AS (
    -- Calculate the date of the most recent Wednesday (inclusive of today if today is Wednesday)
    -- DAYOFWEEKISO: Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6, Sunday=7
    -- ((DAYOFWEEKISO(CURRENT_DATE) - 3 + 7) % 7) calculates the number of days to subtract.
    SELECT
        (CURRENT_DATE - ((DAYOFWEEKISO(CURRENT_DATE) - 3 + 7) % 7))::DATE AS last_wednesday_date
),
RankedLoanCreationRecords AS (
    -- Step 1a: Rank loan records from FINANCE_METRICS_DAILY
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE,
        ROW_NUMBER() OVER (PARTITION BY FBBID, LOAN_KEY ORDER BY EDATE ASC) as rn
    FROM
        BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE PRODUCT_TYPE <> 'Flexpay'
),
DistinctLoanCreation AS (
    -- Step 1b: Select the definitive LOAN_CREATED_DATE
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE
    FROM
        RankedLoanCreationRecords
    WHERE
        rn = 1
),
LoanCreatedWeek AS (
    -- Step 2: Calculate LOAN_CREATED_WEEK_END for loan cohort grouping
    SELECT
        FBBID,
        LOAN_KEY,
        LOAN_CREATED_DATE,
        (date_trunc('week', (LOAN_CREATED_DATE::date + INTERVAL '4 days'))::date + INTERVAL '2 days') AS LOAN_CREATED_WEEK_END
    FROM
        DistinctLoanCreation
),
FbbidPartner AS (
    -- Step 2b: Get PARTNER for each FBBID
    SELECT
        FBBID,
        is_test,
        sub_product,
        termunits,
        PARTNER AS PARTNER
    FROM
        INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2
    WHERE
        EDATE = MIN_EDATE
),
LoanCreatedWeekWithPartner AS (
    -- Step 2c: Combine LoanCreatedWeek with Partner information
    SELECT
        lcw.FBBID,
        lcw.LOAN_KEY,
        lcw.LOAN_CREATED_DATE,
        lcw.LOAN_CREATED_WEEK_END,
        fp.PARTNER,
        fp.termunits
    FROM
        LoanCreatedWeek lcw
    LEFT JOIN
        FbbidPartner fp ON lcw.FBBID = fp.FBBID
    WHERE fp.is_test = 0
    AND fp.sub_product <> 'Credit Builder'
    AND fp.sub_product <> 'mca'
),
RankedPayments AS (
    -- Step 3: Rank payments for each loan by their planned transmission date
    -- Filtered for payments due on or before the last Wednesday and for Debits
    SELECT
        pd.LOAN_KEY,
        pd.PAYMENT_PLANNED_TRANSMISSION_DATE,
        pd.PAYMENT_STATUS,
        ROW_NUMBER() OVER (PARTITION BY pd.LOAN_KEY ORDER BY pd.PAYMENT_PLANNED_TRANSMISSION_DATE ASC) as payment_rank
    FROM
        BI.FINANCE.PAYMENTS_DATA pd, ReportingCutoffDate rcd -- Cross join to get the cutoff date
    WHERE
        pd.PAYMENT_PLANNED_TRANSMISSION_DATE <= rcd.last_wednesday_date
        AND pd.DIRECTION = 'D'
),
FirstPaymentDefault AS (
    -- Step 4a: Identify loans with a first payment default
    SELECT
        LOAN_KEY,
        CASE WHEN PAYMENT_STATUS <> 'FUND' THEN 1 ELSE 0 END as is_first_payment_default
    FROM
        RankedPayments
    WHERE
        payment_rank = 1
),
LoanPaymentSpecifics AS (
    -- Step 4b: Get status for 1st, 2nd, 3rd payments
    SELECT
        LOAN_KEY,
        MAX(CASE WHEN payment_rank = 1 THEN PAYMENT_STATUS END) as p1_status,
        MAX(CASE WHEN payment_rank = 1 THEN 1 ELSE 0 END) as has_p1_record,

        MAX(CASE WHEN payment_rank = 2 THEN PAYMENT_STATUS END) as p2_status,
        MAX(CASE WHEN payment_rank = 2 THEN 1 ELSE 0 END) as has_p2_record,

        MAX(CASE WHEN payment_rank = 3 THEN PAYMENT_STATUS END) as p3_status,
        MAX(CASE WHEN payment_rank = 3 THEN 1 ELSE 0 END) as has_p3_record
    FROM
        RankedPayments
    WHERE
        payment_rank <= 3
    GROUP BY
        LOAN_KEY
),
FirstTwoPaymentsDefault AS (
    -- Step 4c: Identify loans where the first TWO payments were missed
    SELECT
        LOAN_KEY,
        CASE
            WHEN has_p1_record = 1 AND p1_status <> 'FUND' AND
                 has_p2_record = 1 AND p2_status <> 'FUND'
            THEN 1
            ELSE 0
        END as is_first_two_payments_default
    FROM
        LoanPaymentSpecifics
),
FirstThreePaymentsDefault AS (
    -- Step 4d: Identify loans where the first THREE payments were missed
    SELECT
        LOAN_KEY,
        CASE
            WHEN has_p1_record = 1 AND p1_status <> 'FUND' AND
                 has_p2_record = 1 AND p2_status <> 'FUND' AND
                 has_p3_record = 1 AND p3_status <> 'FUND'
            THEN 1
            ELSE 0
        END as is_first_three_payments_default
    FROM
        LoanPaymentSpecifics
),
LoansWithLatestStatus AS (
    -- Step 5: Get the latest IS_CHARGED_OFF status for each loan
    SELECT
        LOAN_KEY,
        FBBID,
        IS_CHARGED_OFF,
        ROW_NUMBER() OVER (PARTITION BY LOAN_KEY ORDER BY EDATE DESC) as rn
    FROM
        BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE PRODUCT_TYPE <> 'Flexpay'
),
LoanFundingSummary AS (
    -- Step 6: Determine if each loan has ever received any 'FUND' payment (debits due by last Wednesday)
    SELECT
        lcwp.LOAN_KEY,
        COALESCE(MAX(CASE WHEN pd.PAYMENT_STATUS = 'FUND' THEN 1 ELSE 0 END), 0) as has_any_funding
    FROM
        LoanCreatedWeekWithPartner lcwp
    CROSS JOIN ReportingCutoffDate rcd -- Make cutoff date available
    LEFT JOIN
        BI.FINANCE.PAYMENTS_DATA pd
            ON lcwp.LOAN_KEY = pd.LOAN_KEY
            AND pd.DIRECTION = 'D'
            AND pd.PAYMENT_PLANNED_TRANSMISSION_DATE <= rcd.last_wednesday_date
    GROUP BY
        lcwp.LOAN_KEY
),
Metric4Data AS (
    -- Step 7: Prepare data for Metric 4
    SELECT
        lcwp.LOAN_KEY,
        CASE
            WHEN lfs.has_any_funding = 0 AND (lls.IS_CHARGED_OFF = 1)
            THEN 1 ELSE 0
        END as is_no_funding_and_charge_off,
        CASE
            WHEN (lls.IS_CHARGED_OFF = 1)
            THEN 1 ELSE 0
        END as IS_CHARGED_OFF_for_denom
    FROM
        LoanCreatedWeekWithPartner lcwp
    LEFT JOIN
        LoanFundingSummary lfs ON lcwp.LOAN_KEY = lfs.LOAN_KEY
    LEFT JOIN
        LoansWithLatestStatus lls ON lcwp.LOAN_KEY = lls.LOAN_KEY AND lls.rn = 1
)
    -- Step 8: Aggregate all counts by LOAN_CREATED_WEEK_END and PARTNER
    SELECT
        lcwp.LOAN_CREATED_WEEK_END AS WEEK_END_DATE, -- This is for cohort grouping
        lcwp.PARTNER,
        lcwp.termunits,
        COUNT(DISTINCT lcwp.LOAN_KEY) as total_loans_originated,

        SUM(COALESCE(fpd.is_first_payment_default, 0)) as count_first_payment_default,
        SUM(COALESCE(f2pd.is_first_two_payments_default, 0)) as count_first_two_payments_default,
        SUM(COALESCE(f3pd.is_first_three_payments_default, 0)) as count_first_three_payments_default,

        SUM(COALESCE(m4d.is_no_funding_and_charge_off, 0)) as count_no_funding_and_charge_off,
        SUM(COALESCE(m4d.IS_CHARGED_OFF_for_denom, 0)) as count_total_charge_off_for_metric4
    FROM
        LoanCreatedWeekWithPartner lcwp
    LEFT JOIN
        FirstPaymentDefault fpd ON lcwp.LOAN_KEY = fpd.LOAN_KEY
    LEFT JOIN
        FirstTwoPaymentsDefault f2pd ON lcwp.LOAN_KEY = f2pd.LOAN_KEY
    LEFT JOIN
        FirstThreePaymentsDefault f3pd ON lcwp.LOAN_KEY = f3pd.LOAN_KEY
    LEFT JOIN
        Metric4Data m4d ON lcwp.LOAN_KEY = m4d.LOAN_KEY
    GROUP BY
        1,2,3
)
;

------------------------------Aggregated Metrics-----------------------------------------------------

CREATE OR REPLACE TABLE INDUS.PUBLIC.COLLECTIONS_AGG AS (
    SELECT
        -- Key Columns
        COALESCE(T1.WEEK_END_DATE, T2.WEEK_END_DATE) AS WEEK_END_DATE,
        COALESCE(T1.PARTNER, T2.PARTNER) AS PARTNER,
        COALESCE(T1.TERMUNITS, T2.TERMUNITS) AS TERMUNITS,

        -- Metrics from CUSTOM_BREATHER (T2)
        case when T2.BREATHER_TOTAL_PRINCIPAL_AMOUNT > 0 then 1 else 0 end as ONE,
        case when T2.CUSTOM_PLAN_TOTAL_PRINCIPAL_AMOUNT > 0 then 1 else 0 end as TWO,
        COALESCE(T2.BREATHER_TOTAL_PRINCIPAL_AMOUNT, 0) AS BREATHER_TOTAL_PRINCIPAL_AMOUNT,
        COALESCE(T2.BREATHER_DISTINCT_FBBID_COUNT, 0) AS BREATHER_DISTINCT_FBBID_COUNT,
        COALESCE(T2.BREATHER_AVG_DURATION_WEEKS, 0) AS BREATHER_AVG_DURATION_WEEKS, -- Added
        COALESCE(T2.CUSTOM_PLAN_TOTAL_PRINCIPAL_AMOUNT, 0) AS CUSTOM_PLAN_TOTAL_PRINCIPAL_AMOUNT,
        COALESCE(T2.CUSTOM_PLAN_DISTINCT_FBBID_COUNT, 0) AS CUSTOM_PLAN_DISTINCT_FBBID_COUNT,
        COALESCE(T2.CUSTOM_PLAN_AVG_DURATION_WEEKS, 0) AS CUSTOM_PLAN_AVG_DURATION_WEEKS, -- Added

        -- Metrics from COLLECTIONS_AR (T1)
        COALESCE(T1.TOTAL_LOANS_ORIGINATED, 0) AS TOTAL_LOANS_ORIGINATED,
        COALESCE(T1.COUNT_FIRST_PAYMENT_DEFAULT, 0) AS COUNT_FIRST_PAYMENT_DEFAULT,
        COALESCE(T1.COUNT_FIRST_TWO_PAYMENTS_DEFAULT, 0) AS COUNT_FIRST_TWO_PAYMENTS_DEFAULT,
        COALESCE(T1.COUNT_FIRST_THREE_PAYMENTS_DEFAULT, 0) AS COUNT_FIRST_THREE_PAYMENTS_DEFAULT,
        COALESCE(T1.COUNT_NO_FUNDING_AND_CHARGE_OFF, 0) AS COUNT_NO_FUNDING_AND_CHARGE_OFF,
        COALESCE(T1.COUNT_TOTAL_CHARGE_OFF_FOR_METRIC4, 0) AS COUNT_TOTAL_CHARGE_OFF_FOR_METRIC4
        
    FROM INDUS.PUBLIC.CUSTOM_BREATHER T2 -- Contains the new columns
    FULL OUTER JOIN INDUS.PUBLIC.COLLECTIONS_AR T1
    ON T1.WEEK_END_DATE = T2.WEEK_END_DATE 
       AND T1.PARTNER = T2.PARTNER 
       AND T1.TERMUNITS = T2.TERMUNITS
);