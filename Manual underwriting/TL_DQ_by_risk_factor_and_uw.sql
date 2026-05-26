/*
  DQ rate by risk factor rating and underwriter
  Source: ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD (TL Dashboard build)

  DQ definition: udr35 = 1 (customer hit DPD >= 35 on or after review_complete_date)

  Grain: one row per (review_month, uw_name, risk_factor_name, risk_factor_rating)
  Use in Tableau/Sheets or run as-is for a pivot-friendly long table.
*/

WITH base AS (
    SELECT
        review_month,
        review_week_start,
        review_week_label,
        is_in_t4w,
        is_in_3mo,
        program_name,
        program_group,
        uw_type,
        is_complete,
        review_complete_date,
        tl_name,
        manager_name,
        uw_name,
        uw_id_resolved,
        udr35,
        rf_bank_balance,
        rf_quality_of_revenue,
        rf_personal_credit,
        rf_industry,
        rf_debt_service,
        rf_residual
    FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
    WHERE uw_name IS NOT NULL
      AND is_complete = 1
      -- Period: IN_T4W | IN_3MO | IN_12MO
      AND is_in_3mo = 1
      -- AND is_in_t4w = 1
      -- AND is_mature_6mo = 1   -- recommended for DQ% (avoids right-censoring)
      -- Optional: single program
      -- AND program_name = 'OB AUW'
      -- Optional: TL filter
      -- AND tl_name = 'Your TL Name'
),

unpivoted AS (
    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Bank Balance' AS risk_factor_name, rf_bank_balance AS risk_factor_rating
    FROM base
    WHERE rf_bank_balance IN ('Low', 'Medium', 'High')

    UNION ALL

    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Quality of Revenue', rf_quality_of_revenue
    FROM base
    WHERE rf_quality_of_revenue IN ('Low', 'Medium', 'High')

    UNION ALL

    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Personal Credit', rf_personal_credit
    FROM base
    WHERE rf_personal_credit IN ('Low', 'Medium', 'High')

    UNION ALL

    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Industry', rf_industry
    FROM base
    WHERE rf_industry IN ('Low', 'Medium', 'High')

    UNION ALL

    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Existing Debt Service', rf_debt_service
    FROM base
    WHERE rf_debt_service IN ('Low', 'Medium', 'High')

    UNION ALL

    SELECT review_month, review_week_start, program_name, program_group, uw_type, tl_name, manager_name, uw_name, uw_id_resolved, udr35,
           'Residual', rf_residual
    FROM base
    WHERE rf_residual IN ('Low', 'Medium', 'High')
)

SELECT
    review_month,
    review_week_start,
    program_name,
    program_group,
    uw_type,
    tl_name,
    manager_name,
    uw_name,
    risk_factor_name,
    risk_factor_rating,
    COUNT(*)                                              AS review_count,
    SUM(udr35)                                            AS dq_count,
    ROUND(SUM(udr35) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS dq_pct
FROM unpivoted
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY
    review_month DESC,
    review_week_start DESC,
    uw_name,
    risk_factor_name,
    risk_factor_rating;


/* -------------------------------------------------------------------------
   VARIANT A — One risk factor at a time (simpler, no UNPIVOT)
   Replace rf_bank_balance with any rf_* column.
-------------------------------------------------------------------------
SELECT
    review_month,
    uw_name,
    tl_name,
    rf_bank_balance                                       AS risk_factor_rating,
    COUNT(*)                                              AS review_count,
    SUM(udr35)                                            AS dq_count,
    ROUND(SUM(udr35) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS dq_pct
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE uw_name IS NOT NULL
  AND rf_bank_balance IS NOT NULL
  AND TRIM(rf_bank_balance) != ''
  AND rf_bank_balance IN ('Low', 'Medium', 'High')
  AND review_month >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2, 4;
------------------------------------------------------------------------- */


/* -------------------------------------------------------------------------
   VARIANT B — Underwriter rollup (all risk factors combined, overall DQ%)
-------------------------------------------------------------------------
SELECT
    review_month,
    uw_name,
    tl_name,
    COUNT(*)                                              AS review_count,
    SUM(udr35)                                            AS dq_count,
    ROUND(SUM(udr35) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS dq_pct
FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
WHERE uw_name IS NOT NULL
  AND review_month >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2;
------------------------------------------------------------------------- */
