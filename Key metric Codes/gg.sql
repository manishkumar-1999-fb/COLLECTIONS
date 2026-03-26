-- =================================================================================================
--                              FINAL DEFINITIVE SINGLE-QUARTER ANALYSIS
-- =================================================================================================
--
-- Instructions:
-- 1. This version correctly sums 'principal_paid' for each week (Tuesday to Monday, based on your week definition).
-- 2. It tracks all cohort loans for the full 52 weeks to prevent survivorship bias.
-- 3. To run for a different period, simply update the 'Z_QUARTER_END_DATE' variable and execute the script.
--
-- =================================================================================================

-- Step 1: Define the Quarter End Date
SET Z_QUARTER_END_DATE = '2021-03-30'::DATE; -- Example: Q1 2024


-- Step 2: Use Common Table Expressions (CTEs) for the Analysis
WITH cohort_customers_1 AS (
    SELECT fbbid
    FROM bi.finance.finance_metrics_daily
    WHERE edate = $Z_QUARTER_END_DATE AND product_type <> 'Flexpay' AND is_charged_off <> 1
    GROUP BY fbbid
    HAVING MAX(dpd_days) BETWEEN 1 AND 7
),

cohort_loans AS (
    SELECT fbbid, loan_key
    FROM bi.finance.finance_metrics_daily
    WHERE edate = $Z_QUARTER_END_DATE AND fbbid IN (SELECT DISTINCT fbbid FROM cohort_customers_1)
    and product_type <> 'Flexpay'
    GROUP BY 1, 2
),

cohort_initial_state AS (
    -- Stores the starting principal for EACH specific loan in the cohort.
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
    -- Metric 1 Logic: This calculates charge-offs for the specific cohort loans.
    SELECT
        CEIL(DATEDIFF(DAY, $Z_QUARTER_END_DATE, charge_off_date) / 7) AS week_after_quarter_end,
        SUM(chargeoff_principal) AS weekly_chargeoff
    FROM (
        SELECT DISTINCT d.loan_key, d.chargeoff_principal, d.charge_off_date
        FROM bi.finance.finance_metrics_daily d
        INNER JOIN cohort_loans c ON d.fbbid = c.fbbid AND d.loan_key = c.loan_key
        WHERE d.charge_off_date IS NOT NULL AND d.chargeoff_principal > 0 and d.is_charged_off = 1
    )
    WHERE charge_off_date > $Z_QUARTER_END_DATE
    GROUP BY 1
),

-- VVVVVV--  NEW LOGIC FOR METRIC 2: USING PRINCIPAL_PAID SUMMED WEEKLY -- VVVVVV
weekly_series AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as week_after_quarter_end
    FROM TABLE(GENERATOR(ROWCOUNT => 52))
),

cohort_loan_weekly_scaffold AS (
    -- Creates a complete template of every loan for every week.
    SELECT
        ws.week_after_quarter_end,
        cl.fbbid,
        cl.loan_key
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
      AND s.week_after_quarter_end = CEIL(DATEDIFF(DAY, $Z_QUARTER_END_DATE, d.edate) / 7)
      AND DAYNAME(d.edate) = 'Tue' -- Keep Tuesday filter here for snapshot
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
      -- Join on the defined week based on DATEDIFF from quarter_end_date (no DAYNAME filter)
      AND s.week_after_quarter_end = CEIL(DATEDIFF(DAY, $Z_QUARTER_END_DATE, d.edate) / 7)
      AND d.edate > $Z_QUARTER_END_DATE -- Only consider data after the quarter end
    GROUP BY 1, 2, 3 -- Group by week, fbbid, loan_key to sum payments for the week
),

weekly_loan_payment_contribution AS (
    -- Combines snapshot data and summed payments, then categorizes.
    SELECT
        wls_status.week_after_quarter_end,
        wls_status.fbbid,
        wls_status.loan_key,
        wls_paid.principal_paid_this_week_total, -- Total principal paid for this loan this week
        wls_status.dpd_days, -- DPD status from Tuesday snapshot

        -- Get previous week's DPD for categorization (for first week, use initial DPD from cohort)
        COALESCE(
            LAG(wls_status.dpd_days) OVER (PARTITION BY wls_status.fbbid, wls_status.loan_key ORDER BY wls_status.week_after_quarter_end),
            cis.initial_dpd -- Now this is correctly from initial state
        ) AS previous_week_dpd,

        -- Case 1: Payment from loan that is currently at DPD 0
        CASE
            WHEN wls_status.dpd_days = 0 THEN wls_paid.principal_paid_this_week_total
            ELSE 0
        END AS payment_to_dpd0,

        -- Case 2: Payment from loan that is currently at DPD > 0 and <= 7
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
-- ^^^^^^-- END OF NEW LOGIC FOR METRIC 2 -- ^^^^^^

-- Final Step: Combine all metrics
SELECT
    ws.week_after_quarter_end,
    d.total_initial_outstanding_principal AS denominator_cohort_principal,

    -- Metric 1: Cumulative Charge-Off
    SUM(COALESCE(ic.weekly_chargeoff, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_chargeoff,
    (numerator_cumulative_chargeoff / d.total_initial_outstanding_principal) AS cumulative_cents_charged_off_per_dollar,

    -- Metric 2: Total Cumulative Amount Paid (Sum of both categories)
    SUM(COALESCE(twpc.weekly_total_paid_by_satisfactory_loans, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_total,
    (numerator_cumulative_paid_total / d.total_initial_outstanding_principal) AS cumulative_cents_paid_total_per_dollar,

    -- Metric 2 - Sub-category: Cumulative Amount Paid to DPD 0
    SUM(COALESCE(twpc.weekly_paid_to_dpd0_total, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_to_dpd0,
    (numerator_cumulative_paid_to_dpd0 / d.total_initial_outstanding_principal) AS cumulative_cents_paid_to_dpd0_per_dollar,

    -- Metric 2 - Sub-category: Cumulative Amount Paid within DPD <= 7
    SUM(COALESCE(twpc.weekly_paid_within_dpd7_or_current_total, 0)) OVER (ORDER BY ws.week_after_quarter_end) AS numerator_cumulative_paid_within_dpd7_or_current,
    (numerator_cumulative_paid_within_dpd7_or_current / d.total_initial_outstanding_principal) AS cumulative_cents_paid_within_dpd7_or_current_per_dollar,

    -- Weekly (Non-Cumulative) Charge-Off Metric
    COALESCE(ic.weekly_chargeoff, 0) AS numerator_weekly_chargeoff,
    (numerator_weekly_chargeoff / d.total_initial_outstanding_principal) AS weekly_cents_charged_off_per_dollar,

    -- Weekly (Non-Cumulative) Paid Amount - Total
    COALESCE(twpc.weekly_total_paid_by_satisfactory_loans, 0) AS numerator_weekly_paid_total,
    (numerator_weekly_paid_total / d.total_initial_outstanding_principal) AS weekly_cents_paid_total_per_dollar,

    -- Weekly (Non-Cumulative) Paid Amount - To DPD 0
    COALESCE(twpc.weekly_paid_to_dpd0_total, 0) AS numerator_weekly_paid_to_dpd0,
    (numerator_weekly_paid_to_dpd0 / d.total_initial_outstanding_principal) AS weekly_cents_paid_to_dpd0_per_dollar,

    -- Weekly (Non-Cumulative) Paid Amount - Within DPD <= 7
    COALESCE(twpc.weekly_paid_within_dpd7_or_current_total, 0) AS numerator_weekly_paid_within_dpd7_or_current,
    (numerator_weekly_paid_within_dpd7_or_current / d.total_initial_outstanding_principal) AS weekly_cents_paid_within_dpd7_or_current_per_dollar

FROM weekly_series ws
LEFT JOIN incremental_chargeoffs_by_week ic ON ws.week_after_quarter_end = ic.week_after_quarter_end
LEFT JOIN total_weekly_paid_by_category twpc ON ws.week_after_quarter_end = twpc.week_after_quarter_end
CROSS JOIN cohort_denominator d
ORDER BY ws.week_after_quarter_end;