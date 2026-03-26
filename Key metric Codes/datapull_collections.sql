-- =================================================================================================
--                                              Metric 1 and 2
-- =================================================================================================
--
-- Instructions:
-- 1. This version correctly sums 'principal_paid' for each week (Tuesday to Monday, based on your week definition).
-- 2. It now properly deduplicates charge-off events to ensure accurate cohort-level charge-off totals.
-- 3. To run for a different period, simply update the 'Z_QUARTER_END_DATE' variable and execute the script.
--
-- =================================================================================================

-- Step 1: Define the Quarter End Date
SET Z_QUARTER_END_DATE = '2025-03-25'::DATE;


-- Step 2: Use Common Table Expressions (CTEs) for the Analysis
WITH cohort_customers_1 AS (
    SELECT fbbid
    FROM bi.finance.finance_metrics_daily
    WHERE edate = $Z_QUARTER_END_DATE AND product_type <> 'Flexpay' AND is_charged_off <> 1
    GROUP BY fbbid
    HAVING MAX(dpd_days) BETWEEN 1 AND 7
),

cohort_loans AS (
    SELECT fbbid, loan_key, $Z_QUARTER_END_DATE AS cohort_quarter_end_date
    FROM bi.finance.finance_metrics_daily
    WHERE edate = $Z_QUARTER_END_DATE AND fbbid IN (SELECT DISTINCT fbbid FROM cohort_customers_1)
    AND product_type <> 'Flexpay'
    GROUP BY 1, 2, 3
),

cohort_initial_state AS (
    -- Stores the starting principal and DPD for EACH specific loan in the cohort.
    SELECT d.fbbid, d.loan_key, d.outstanding_principal_due AS initial_os, d.dpd_days AS initial_dpd
    FROM bi.finance.finance_metrics_daily d
    INNER JOIN cohort_loans c ON d.fbbid = c.fbbid AND d.loan_key = c.loan_key
    WHERE d.edate = $Z_QUARTER_END_DATE
),

cohort_denominator AS (
    -- The total starting principal for ALL loans in the cohort.
    SELECT SUM(initial_os) AS total_initial_outstanding_principal
    FROM cohort_initial_state
),

incremental_chargeoffs_by_week AS (
    -- MODIFIED: Uses QUALIFY to deduplicate charge-off events.
    SELECT
        CEIL(DATEDIFF(DAY, coe_deduped.cohort_quarter_end_date, coe_deduped.charge_off_date) / 7) AS week_after_quarter_end,
        SUM(coe_deduped.chargeoff_principal) AS weekly_chargeoff
    FROM (
        SELECT
            d.loan_key,
            d.chargeoff_principal,
            d.charge_off_date,
            d.fbbid,
            c.cohort_quarter_end_date,
            -- Assign a row number to each record for a specific loan and charge_off_date.
            -- This assumes charge_off_date identifies a unique charge-off event for a loan.
            ROW_NUMBER() OVER (PARTITION BY d.loan_key ORDER BY d.charge_off_date) as rn
        FROM bi.finance.finance_metrics_daily d
        INNER JOIN cohort_loans c ON d.fbbid = c.fbbid AND d.loan_key = c.loan_key
        WHERE d.charge_off_date IS NOT NULL AND d.chargeoff_principal > 0 AND d.is_charged_off = 1
          AND d.charge_off_date > c.cohort_quarter_end_date
        QUALIFY rn = 1 -- Only pick one record per unique (loan_key, charge_off_date) event
    ) coe_deduped
    GROUP BY 1
),

-- VVVVVV--  CORE LOGIC FOR METRICS -- VVVVVV
weekly_series AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as week_after_quarter_end
    FROM TABLE(GENERATOR(ROWCOUNT => 52))
),

cohort_loan_weekly_scaffold AS (
    -- Creates a complete template of every loan for every week.
    SELECT
        ws.week_after_quarter_end,
        cl.fbbid,
        cl.loan_key,
        cl.cohort_quarter_end_date
    FROM cohort_loans cl
    CROSS JOIN weekly_series ws
),

weekly_loan_current_status AS (
    -- Gets OS and DPD from the specific Tuesday snapshot for each loan.
    SELECT
        s.week_after_quarter_end,
        s.fbbid,
        s.loan_key,
        COALESCE(d.outstanding_principal_due, 0) AS current_os,
        COALESCE(d.dpd_days, 0) AS dpd_days
    FROM cohort_loan_weekly_scaffold s
    LEFT JOIN bi.finance.finance_metrics_daily d
      ON s.fbbid = d.fbbid
      AND s.loan_key = d.loan_key
      AND s.week_after_quarter_end = CEIL(DATEDIFF(DAY, s.cohort_quarter_end_date, d.edate) / 7)
      AND DAYNAME(d.edate) = 'Tue'
),

weekly_loan_paid_amounts AS (
    -- Sums principal_paid for the ENTIRE week (all days) for each loan.
    SELECT
        s.week_after_quarter_end,
        s.fbbid,
        s.loan_key,
        COALESCE(SUM(d.principal_paid), 0) AS principal_paid_this_week_total
    FROM cohort_loan_weekly_scaffold s
    LEFT JOIN bi.finance.finance_metrics_daily d
      ON s.fbbid = d.fbbid
      AND s.loan_key = d.loan_key
      AND s.week_after_quarter_end = CEIL(DATEDIFF(DAY, s.cohort_quarter_end_date, d.edate) / 7)
      AND d.edate > s.cohort_quarter_end_date
    GROUP BY 1, 2, 3
),

weekly_loan_payment_contribution AS (
    -- Combines snapshot data and summed payments, then categorizes.
    SELECT
        wls_status.week_after_quarter_end,
        wls_status.fbbid,
        wls_status.loan_key,
        wls_paid.principal_paid_this_week_total,
        wls_status.current_os,
        wls_status.dpd_days,

        COALESCE(
            LAG(wls_status.dpd_days) OVER (PARTITION BY wls_status.fbbid, wls_status.loan_key ORDER BY wls_status.week_after_quarter_end),
            cis.initial_dpd
        ) AS previous_week_dpd,

        -- Payment to DPD 0
        CASE
            WHEN wls_status.dpd_days = 0 THEN wls_paid.principal_paid_this_week_total
            ELSE 0
        END AS payment_to_dpd0,

        -- Payment within DPD > 0 and <= 7
        CASE
            WHEN wls_status.dpd_days > 0 AND wls_status.dpd_days <= 7 THEN wls_paid.principal_paid_this_week_total
            ELSE 0
        END AS payment_within_dpd7_or_current
    FROM weekly_loan_current_status wls_status
    INNER JOIN weekly_loan_paid_amounts wls_paid
        ON wls_status.week_after_quarter_end = wls_paid.week_after_quarter_end
        AND wls_status.fbbid = wls_paid.fbbid
        AND wls_status.loan_key = wls_paid.loan_key
    INNER JOIN cohort_initial_state cis ON wls_status.fbbid = cis.fbbid AND wls_status.loan_key = cis.loan_key
),

total_weekly_paid_by_category AS (
    -- Aggregates the weekly payment contributions by category for the entire cohort.
    SELECT
        week_after_quarter_end,
        SUM(payment_to_dpd0) AS weekly_paid_to_dpd0_total,
        SUM(payment_within_dpd7_or_current) AS weekly_paid_within_dpd7_or_current_total,
        SUM(payment_to_dpd0) + SUM(payment_within_dpd7_or_current) AS weekly_total_paid_by_satisfactory_loans
    FROM weekly_loan_payment_contribution
    GROUP BY 1
)
-- ^^^^^^-- END OF CORE LOGIC -- ^^^^^^

-- Final Step: Combine all metrics
SELECT
    ws.week_after_quarter_end,
    d.total_initial_outstanding_principal AS denominator_cohort_principal,

    -- Metric 1: Cumulative Charge-Off (This sum should now be aligned)
    SUM(COALESCE(ic.weekly_chargeoff, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_chargeoff,
    (numerator_cumulative_chargeoff / d.total_initial_outstanding_principal) AS cumulative_cents_charged_off_per_dollar,

    -- Metric 2: Total Cumulative Amount Paid (Sum of both categories)
    SUM(COALESCE(twpc.weekly_total_paid_by_satisfactory_loans, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_total,
    (numerator_cumulative_paid_total / d.total_initial_outstanding_principal) AS cumulative_cents_paid_total_per_dollar,

   /* -- Metric 2 - Sub-category: Cumulative Amount Paid to DPD 0
    SUM(COALESCE(twpc.weekly_paid_to_dpd0_total, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_to_dpd0,
    (numerator_cumulative_paid_to_dpd0 / d.total_initial_outstanding_principal) * 100 AS cumulative_cents_paid_to_dpd0_per_dollar,

    -- Metric 2 - Sub-category: Cumulative Amount Paid within DPD <= 7
    SUM(COALESCE(twpc.weekly_paid_within_dpd7_or_current_total, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_within_dpd7_or_current,
    (numerator_cumulative_paid_within_dpd7_or_current / d.total_initial_outstanding_principal) * 100 AS cumulative_cents_paid_within_dpd7_or_current_per_dollar,*/

    -- Weekly (Non-Cumulative) Charge-Off Metric
    COALESCE(ic.weekly_chargeoff, 0) AS numerator_weekly_chargeoff,
    (numerator_weekly_chargeoff / d.total_initial_outstanding_principal) AS weekly_cents_charged_off_per_dollar,

    -- Weekly (Non-Cumulative) Paid Amount - Total
    COALESCE(twpc.weekly_total_paid_by_satisfactory_loans, 0) AS numerator_weekly_paid_total,
    (numerator_weekly_paid_total / d.total_initial_outstanding_principal)  AS weekly_cents_paid_total_per_dollar,

   /* -- Weekly (Non-Cumulative) Paid Amount - To DPD 0
    COALESCE(twpc.weekly_paid_to_dpd0_total, 0) AS numerator_weekly_paid_to_dpd0,
    (numerator_weekly_paid_to_dpd0 / d.total_initial_outstanding_principal) * 100 AS weekly_cents_paid_to_dpd0_per_dollar,

    -- Weekly (Non-Cumulative) Paid Amount - Within DPD <= 7
    COALESCE(twpc.weekly_paid_within_dpd7_or_current_total, 0) AS numerator_weekly_paid_within_dpd7_or_current,
    (numerator_weekly_paid_within_dpd7_or_current / d.total_initial_outstanding_principal) * 100 AS weekly_cents_paid_within_dpd7_or_current_per_dollar*/

FROM weekly_series ws
LEFT JOIN incremental_chargeoffs_by_week ic ON ws.week_after_quarter_end = ic.week_after_quarter_end
LEFT JOIN total_weekly_paid_by_category twpc ON ws.week_after_quarter_end = twpc.week_after_quarter_end
CROSS JOIN cohort_denominator d
ORDER BY ws.week_after_quarter_end;





--------------------------------------------------Metric 3-------------------------------------------------------

WITH ChargeOffCohorts AS (
    -- 1. Define Charge-off Cohorts and their total charged-off principal
    SELECT
        loan_key,
        charge_off_date,
        chargeoff_principal,
        -- Determine the quarter of charge-off
        DATE_TRUNC('quarter', charge_off_date) AS charge_off_quarter_start,
        -- Calculate the end date of the charge-off quarter
        --(DATE_TRUNC('quarter', charge_off_date) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS charge_off_quarter_end_date
    FROM
        bi.finance.finance_metrics_daily
    WHERE
        is_charged_off = 1
        AND charge_off_date >= '2021-01-01' -- Start from Q1-2021
        AND PRODUCT_TYPE <> 'Flexpay'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY loan_key ORDER BY edate DESC) = 1 -- Ensure unique charge-off per loan_key for its details
),
TotalCohortChargeOffs AS (
    -- 2. Aggregate total charged-off principal per cohort
    SELECT
        charge_off_quarter_start,
        SUM(chargeoff_principal) AS total_charged_off_principal
    FROM
        ChargeOffCohorts
    GROUP BY
        charge_off_quarter_start
),
RecoveryTransactions AS (
    -- 3. Identify and categorize recovery payments
    SELECT
        fmd.loan_key,
        fmd.edate AS payment_date,
        fmd.principal_paid,
        coh.charge_off_quarter_start,
        --coh.charge_off_quarter_end_date,
        -- Calculate days after the charge-off quarter end
        -- Add 1 to ensure Week 1 starts from day 1, not day 0
        FLOOR(CAST(fmd.edate - coh.charge_off_date AS DECIMAL) / 7) + 1 AS recovery_week
    FROM
        bi.finance.finance_metrics_daily fmd
    INNER JOIN
        ChargeOffCohorts coh ON fmd.loan_key = coh.loan_key
    WHERE
        fmd.principal_paid > 0 -- Only consider records with actual payments
        -- Ensure payment date is after the quarter end of charge-off
        AND fmd.edate > coh.charge_off_date
        -- Limit to 48 months (approx. 208 weeks, since 48 months * ~4.33 weeks/month)
        -- Adjust this upper limit if you want to go shorter or longer, but 48 months is the request
        AND FLOOR(CAST(fmd.edate - coh.charge_off_date AS DECIMAL) / 7) + 1 <= (48 * 4.34524)::INT -- Approximately 208 weeks
),
AggregatedWeeklyRecoveries AS (
    -- 4. Sum principal_paid for each cohort and recovery week
    SELECT
        charge_off_quarter_start,
        recovery_week,
        SUM(principal_paid) AS total_principal_paid_in_week
    FROM
        RecoveryTransactions
    GROUP BY
        charge_off_quarter_start,
        recovery_week
)
-- 5. Calculate Non-Cumulative and Cumulative Recovery Rates
SELECT
    awr.charge_off_quarter_start,
    awr.recovery_week,
    -- Non-Cumulative Rate: (Cents collected in this week / Dollar charged off)
    (awr.total_principal_paid_in_week / tc.total_charged_off_principal) AS non_cumulative_recovery_cents,
    -- Cumulative Rate: (Total Cents collected up to this week / Dollar charged off)
    (SUM(awr.total_principal_paid_in_week) OVER (PARTITION BY awr.charge_off_quarter_start ORDER BY awr.recovery_week) / tc.total_charged_off_principal) AS cumulative_recovery_cents
FROM
    AggregatedWeeklyRecoveries awr
INNER JOIN
    TotalCohortChargeOffs tc ON awr.charge_off_quarter_start = tc.charge_off_quarter_start
WHERE
    tc.total_charged_off_principal > 0 -- Avoid division by zero
ORDER BY
    awr.charge_off_quarter_start,
    awr.recovery_week;