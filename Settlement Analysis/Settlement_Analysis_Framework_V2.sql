-- =====================================================
-- SETTLEMENT ANALYSIS FRAMEWORK  v2.0
-- =====================================================
-- Fundbox Settlement Portfolio — Comprehensive Analysis
-- Scope: Pre-CO (DPD 1-90) and Post-CO Settlements from 2022 onwards
--
-- Changes vs v1.0:
--   [FIX-1]  Settlement offer terms captured from backy_settlements
--   [FIX-2]  Re-settlement detection: full history preserved, repeat flag added
--   [FIX-3]  Balance fallback hardened — Post-CO uses payments-adjusted balance
--   [FIX-4]  Payment plan structure: lump-sum vs installment, failure stage
--   [FIX-5]  Agent attribution: 30-day window, all touching agents ranked,
--             primary/assist split with weighted credit
--   [NEW-1]  Pre-CO vintage cohort analysis (by first-delinquency month & DPD bucket)
--   [NEW-2]  Settlement offer acceptance funnel (offer → accepted → active → funded)
--   [NEW-3]  Cohort survival curve for active settlements
--   [NEW-4]  Payment waterfall: which payment in series triggers failure
--
-- Framework Components:
--   1. Settlement History Base (all events, re-settlement aware)
--   2. Settlement Portfolio Base (latest record, enriched)
--   3. Settlement Conversion Funnel (offer → active → funded/failed)
--   4. Settlement Velocity & Payment Behavior
--   5. Recovery Rate & Discount Analysis
--   6. Vintage Cohort Analysis — Post-CO (CO quarter × MOB)
--   7. Vintage Cohort Analysis — Pre-CO (delinquency month × DPD bucket)
--   8. Agent Settlement Performance (primary + assist attribution)
--   9. Portfolio Health & Liquidity Risk
--  10. Industry & Geography Segmentation
--  11. Settlement Survival Curve (active portfolio projection)
--  12. Payment Waterfall & Failure Stage Analysis
--  13. Raw Settlement Data Table (fbbid-level for ad-hoc analysis)
-- =====================================================


-- =====================================================
-- SHARED CTE: SETTLEMENT RAW HISTORY
-- =====================================================
-- [FIX-2] Preserve ALL settlement records per fbbid (not just latest).
-- Used as the foundation for re-settlement detection and offer funnel.
-- Referenced inline in views that need full history.


-- =====================================================
-- VIEW 1: SETTLEMENT HISTORY BASE
-- =====================================================
-- Full event-level settlement history per merchant.
-- Enables re-settlement detection and offer-to-active funnel.
-- [FIX-2] No deduplication here — downstream views deduplicate as needed.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_history_base AS

WITH settlements_all AS (
    SELECT
        fbbid,
        settlement_id,                                          -- [FIX-1] unique offer identifier
        current_status                              AS settlement_status,
        settlement_created_date,
        settlement_end_time,
        event_time,
        COALESCE(corrected_created_time,
                 settlement_created_date)           AS corrected_created_date,
        -- [FIX-1] Offer terms from source
        offered_settlement_amount,                              -- agreed amount offered
        scheduled_payment_count,                                -- installments offered
        settlement_type_code,                                   -- lump_sum / installment
        -- Rank per merchant, newest first
        ROW_NUMBER() OVER (
            PARTITION BY fbbid
            ORDER BY settlement_created_date DESC, event_time DESC
        )                                           AS settlement_seq_desc,
        -- Rank oldest first (to detect first vs subsequent settlements)
        ROW_NUMBER() OVER (
            PARTITION BY fbbid
            ORDER BY settlement_created_date ASC, event_time ASC
        )                                           AS settlement_seq_asc,
        COUNT(*) OVER (PARTITION BY fbbid)          AS total_settlement_attempts
    FROM analytics.credit.cjk_v_backy_settlements
    WHERE settlement_created_date >= '2022-01-01'
),

-- [FIX-2] Detect prior funded settlements — needed for re-settlement flag
prior_funded AS (
    SELECT DISTINCT fbbid
    FROM settlements_all
    WHERE settlement_status = 'FUNDED'
      AND settlement_seq_asc > 1   -- at least one earlier settlement existed
)

SELECT
    s.*,
    -- [FIX-2] Re-settlement flags
    CASE WHEN s.total_settlement_attempts > 1
              AND s.settlement_seq_asc > 1 THEN TRUE ELSE FALSE END  AS is_re_settlement,
    CASE WHEN pf.fbbid IS NOT NULL THEN TRUE ELSE FALSE END           AS has_prior_funded_settlement,
    -- Convenience: is this the most-recent settlement record for this merchant?
    CASE WHEN s.settlement_seq_desc = 1 THEN TRUE ELSE FALSE END      AS is_latest_settlement

FROM settlements_all s
LEFT JOIN prior_funded pf ON s.fbbid = pf.fbbid;


-- =====================================================
-- VIEW 2: SETTLEMENT PORTFOLIO BASE
-- =====================================================
-- Master settlement table — latest record per merchant, fully enriched.
-- Source of truth for all downstream analytical views.
--
-- Key fixes vs v1:
--   [FIX-1] Offer terms (offered_amount, scheduled installments, payment type)
--   [FIX-2] Re-settlement and repeat-breach flags from history base
--   [FIX-3] Balance fallback hardened: Post-CO balance = CO principal minus
--            any payments received between CO date and settlement creation,
--            so recovery rates are not overstated for aged Post-CO cases
--   [FIX-4] Payment plan structure: lump-sum vs installment flag,
--            payment failure stage captured in View 12

CREATE OR REPLACE VIEW analytics.credit.v_settlement_portfolio_base AS

WITH
-- Latest settlement record per merchant
settlements AS (
    SELECT *
    FROM analytics.credit.v_settlement_history_base
    WHERE is_latest_settlement = TRUE
),

-- Charge-off data
charge_off_data AS (
    SELECT
        fbbid,
        MIN(charge_off_date)             AS charge_off_date,
        SUM(outstanding_principal_due)   AS co_principal
    FROM bi.finance.finance_metrics_daily
    WHERE is_charged_off = 1
      AND product_type <> 'Flexpay'
      AND original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fbbid
),

-- [FIX-3] Payments received between charge-off and settlement creation.
-- Used to compute a truer Post-CO balance at the moment of settlement.
payments_between_co_and_settlement AS (
    SELECT
        p.fbbid,
        SUM(TO_DOUBLE(p.payment_components_json:PAYMENT_AMOUNT)) AS payments_post_co_pre_settlement
    FROM bi.finance.payments_model p
    INNER JOIN charge_off_data co ON p.fbbid = co.fbbid
    INNER JOIN settlements s      ON p.fbbid = s.fbbid
    WHERE p.payment_status = 'FUND'
      AND DATE(p.payment_event_time) > co.charge_off_date
      AND DATE(p.payment_event_time) < s.settlement_created_date
      AND p.parent_payment_id IS NOT NULL
    GROUP BY p.fbbid
),

-- Customer state at settlement creation date (point-in-time)
customer_state_at_settlement AS (
    SELECT
        s.fbbid,
        dacd.recovery_suggested_state,
        dacd.recovery_suggested_substate,
        dacd.outstanding_principal,
        dacd.fees_due,
        dacd.discount_pending,
        (dacd.outstanding_principal
            + dacd.fees_due
            - COALESCE(dacd.discount_pending, 0))  AS balance_at_settlement_dacd,
        CASE
            WHEN dacd.recovery_suggested_state IN (
                     'ILR','LR','ER','FB_TL','CB_DLQ','HEAL',
                     'TR_ILR','EOL','PRELIT','LPD','MCA_HE')
                 OR dacd.recovery_suggested_state IS NULL THEN 'Internal'
            WHEN dacd.recovery_suggested_state IN ('ELR','PROLIT','TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_type_at_settlement
    FROM settlements s
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON s.fbbid  = dacd.fbbid
       AND dacd.edate = s.settlement_created_date
),

-- DPD at settlement creation
dpd_at_settlement AS (
    SELECT
        fmd.fbbid,
        fmd.edate,
        MAX(COALESCE(fmd.dpd_days, 0))                AS dpd_days,
        MAX(fmd.is_charged_off)                        AS is_charged_off,
        SUM(fmd.outstanding_principal_due)             AS outstanding_principal_at_date
    FROM bi.finance.finance_metrics_daily fmd
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fmd.fbbid, fmd.edate
),

-- [FIX-3] First delinquency date — needed for Pre-CO vintage cohort
first_delinquency AS (
    SELECT
        fbbid,
        MIN(edate) AS first_delinquency_date
    FROM bi.finance.finance_metrics_daily
    WHERE dpd_days >= 1
      AND product_type <> 'Flexpay'
      AND original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fbbid
),

-- Payments within the settlement window
settlement_payments AS (
    SELECT
        p.fbbid,
        SUM(CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time)
                     BETWEEN s.settlement_created_date
                         AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                THEN TO_DOUBLE(p.payment_components_json:PAYMENT_AMOUNT)
                ELSE 0
            END)                                                  AS settlement_payment_amount,
        COUNT(CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time)
                     BETWEEN s.settlement_created_date
                         AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                THEN 1
              END)                                                AS settlement_payment_count,
        MIN(CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time) >= s.settlement_created_date
                THEN DATE(p.payment_event_time)
            END)                                                  AS first_settlement_payment_date,
        MAX(CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time)
                     BETWEEN s.settlement_created_date
                         AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                THEN DATE(p.payment_event_time)
            END)                                                  AS last_settlement_payment_date,
        -- [FIX-4] Lump-sum: exactly 1 funded payment; installment: 2+
        CASE
            WHEN COUNT(CASE
                        WHEN p.payment_status = 'FUND'
                         AND DATE(p.payment_event_time)
                             BETWEEN s.settlement_created_date
                                 AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                        THEN 1
                       END) = 1 THEN 'Lump Sum'
            WHEN COUNT(CASE
                        WHEN p.payment_status = 'FUND'
                         AND DATE(p.payment_event_time)
                             BETWEEN s.settlement_created_date
                                 AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                        THEN 1
                       END) > 1  THEN 'Installment'
            ELSE 'No Payments'
        END                                                       AS payment_structure_actual
    FROM bi.finance.payments_model p
    INNER JOIN settlements s ON p.fbbid = s.fbbid
    WHERE p.parent_payment_id IS NOT NULL
    GROUP BY p.fbbid
),

-- Customer attributes (most-recent record)
customer_attributes AS (
    SELECT DISTINCT
        fbbid,
        FIRST_VALUE(industry)     IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS industry,
        FIRST_VALUE(state)        IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS customer_state,
        FIRST_VALUE(credit_limit) IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS credit_limit,
        FIRST_VALUE(channel)      IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS channel,
        FIRST_VALUE(partner)      IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS partner,
        FIRST_VALUE(tier)         IGNORE NULLS
            OVER (PARTITION BY fbbid ORDER BY edate DESC) AS tier
    FROM bi.public.daily_approved_customers_data
    WHERE fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
),

-- Time on platform (first draw date)
time_on_platform AS (
    SELECT
        fbbid,
        MIN(edate) AS first_draw_date
    FROM bi.finance.finance_metrics_daily
    WHERE outstanding_principal_due > 0
    GROUP BY fbbid
),

-- Acquisition cohort
acquisition_cohort AS (
    SELECT 
        fbbid,
        first_approved_time,
        CASE 
            WHEN first_approved_time::DATE < '2020-01-01' THEN '1. Pre-2020'
            WHEN first_approved_time::DATE BETWEEN '2020-01-01' AND '2021-12-31' THEN '2. 2020-2021'
            WHEN first_approved_time::DATE BETWEEN '2022-01-01' AND '2023-12-31' THEN '3. 2022-2023'
            WHEN first_approved_time::DATE BETWEEN '2024-01-01' AND '2024-12-31' THEN '4. 2024'
            WHEN first_approved_time::DATE >= '2025-01-01' THEN '5. 2025+'
            ELSE '6. Unknown'
        END AS acquisition_cohort
    FROM bi.public.customers_data
    WHERE fbbid IS NOT NULL
),

-- Risk grade (OG Model Score) at settlement
risk_grade_at_settlement AS (
    SELECT 
        s.fbbid,
        og.OG_BUCKET AS og_bucket,
        CASE 
            WHEN og.OG_BUCKET BETWEEN 1 AND 4 THEN '1. Low Risk (1-4)'
            WHEN og.OG_BUCKET BETWEEN 5 AND 7 THEN '2. Medium-Low (5-7)'
            WHEN og.OG_BUCKET BETWEEN 8 AND 10 THEN '3. Medium (8-10)'
            WHEN og.OG_BUCKET BETWEEN 11 AND 12 THEN '4. Medium-High (11-12)'
            WHEN og.OG_BUCKET BETWEEN 13 AND 15 THEN '5. High Risk (13-15)'
            ELSE '6. Unknown'
        END AS og_bucket_group
    FROM settlements s
    LEFT JOIN ANALYTICS.CREDIT.OG_MODEL_SCORES_RETROSCORED_V1_1 og
        ON s.fbbid = og.fbbid
        AND og.edate = DATEADD('day', -1, s.settlement_created_date)
),

-- VantageScore at settlement
vantage_at_settlement AS (
    SELECT 
        s.fbbid,
        CASE 
            WHEN DATEDIFF('day', dacd.credit_score_json:"VantageScore 4.0":"created_time"::TIMESTAMP, dacd.edate) <= 90
            THEN dacd.credit_score_json:"VantageScore 4.0":"score"::INT
            ELSE NULL
        END AS vantage_score
    FROM settlements s
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON s.fbbid = dacd.fbbid
        AND dacd.edate = DATEADD('day', -1, s.settlement_created_date)
),

-- [FIX-3] Resolve balance for each settlement record with explicit fallback chain:
--   Priority 1: daily_approved_customers_data balance at settlement date (most precise)
--   Priority 2: finance_metrics_daily outstanding principal at settlement date
--   Priority 3 (Post-CO only): CO principal MINUS any payments received after CO
--              and before settlement creation — this prevents overstatement
balance_resolution AS (
    SELECT
        s.fbbid,
        -- [FIX-3] Post-CO adjusted balance
        CASE
            WHEN co.charge_off_date IS NOT NULL
             AND s.settlement_created_date >= co.charge_off_date
             AND cs.balance_at_settlement_dacd IS NULL
             AND dpd.outstanding_principal_at_date IS NULL
            THEN GREATEST(
                    co.co_principal
                        - COALESCE(pco.payments_post_co_pre_settlement, 0),
                    0)
            ELSE NULL
        END AS co_adjusted_balance,
        cs.balance_at_settlement_dacd,
        dpd.outstanding_principal_at_date,
        co.co_principal
    FROM settlements s
    LEFT JOIN charge_off_data co              ON s.fbbid = co.fbbid
    LEFT JOIN customer_state_at_settlement cs ON s.fbbid = cs.fbbid
    LEFT JOIN dpd_at_settlement dpd
        ON s.fbbid = dpd.fbbid
       AND dpd.edate = s.settlement_created_date
    LEFT JOIN payments_between_co_and_settlement pco ON s.fbbid = pco.fbbid
)

SELECT
    -- ── Identifiers ──────────────────────────────────────────────────────────
    s.fbbid,
    s.settlement_id,
    s.settlement_status,
    s.settlement_created_date,
    s.settlement_end_time,
    s.corrected_created_date,

    -- ── Time dimensions ───────────────────────────────────────────────────────
    DATE_TRUNC('month',   s.settlement_created_date) AS settlement_month,
    DATE_TRUNC('quarter', s.settlement_created_date) AS settlement_quarter,
    YEAR(s.settlement_created_date)                   AS settlement_year,

    -- ── Settlement lifecycle ─────────────────────────────────────────────────
    CASE
        WHEN s.settlement_status = 'FUNDED'                   THEN 'Completed'
        WHEN s.settlement_status = 'ACTIVE'                   THEN 'In Progress'
        WHEN s.settlement_status IN ('FAILED','CANCELLED')    THEN 'Failed/Cancelled'
        ELSE 'Other'
    END AS settlement_lifecycle_stage,

    DATEDIFF('day', s.settlement_created_date,
             COALESCE(s.settlement_end_time, CURRENT_DATE))   AS days_in_settlement,

    -- ── Pre-CO vs Post-CO ─────────────────────────────────────────────────────
    CASE
        WHEN co.charge_off_date IS NULL                       THEN 'Pre-CO'
        WHEN s.settlement_created_date < co.charge_off_date   THEN 'Pre-CO'
        ELSE 'Post-CO'
    END AS settlement_type,

    -- ── Charge-off context ────────────────────────────────────────────────────
    co.charge_off_date,
    LAST_DAY(co.charge_off_date)                              AS co_month,
    YEAR(co.charge_off_date) || '-Q' || QUARTER(co.charge_off_date) AS co_quarter,
    co.co_principal,
    DATEDIFF('day', co.charge_off_date,
             s.settlement_created_date)                       AS days_since_co_at_settlement,
    FLOOR(DATEDIFF('day', co.charge_off_date,
                   s.settlement_created_date) / 30)           AS mob_at_settlement,

    -- ── DPD context (Pre-CO) ──────────────────────────────────────────────────
    dpd.dpd_days                                              AS dpd_at_settlement,
    dpd.is_charged_off                                        AS was_charged_off_at_settlement,
    CASE
        WHEN dpd.dpd_days BETWEEN 1  AND 14                   THEN '1-2 (DPD 1-14)'
        WHEN dpd.dpd_days BETWEEN 15 AND 56                   THEN '3-8 (DPD 15-56)'
        WHEN dpd.dpd_days BETWEEN 57 AND 91                   THEN '9-13 (DPD 57-91)'
        WHEN dpd.is_charged_off = 1 OR dpd.dpd_days > 91      THEN 'Charged Off'
        WHEN dpd.dpd_days = 0 OR dpd.dpd_days IS NULL         THEN 'Current'
        ELSE 'Unknown'
    END AS dpd_bucket_at_settlement,

    -- ── First delinquency (for Pre-CO vintage) ────────────────────────────────
    fd.first_delinquency_date,
    DATE_TRUNC('month',   fd.first_delinquency_date)          AS first_delinquency_month,
    YEAR(fd.first_delinquency_date) || '-Q'
        || QUARTER(fd.first_delinquency_date)                 AS first_delinquency_quarter,
    DATEDIFF('month', fd.first_delinquency_date,
             s.settlement_created_date)                       AS months_since_first_delinquency,

    -- ── [FIX-3] Balance — hardened fallback chain ────────────────────────────
    -- Priority: DACD > FMD point-in-time > CO-adjusted (payments subtracted)
    COALESCE(
        br.balance_at_settlement_dacd,
        br.outstanding_principal_at_date,
        br.co_adjusted_balance
    )                                                         AS balance_at_settlement,

    -- Expose the source of the balance for auditability
    CASE
        WHEN br.balance_at_settlement_dacd IS NOT NULL        THEN 'DACD'
        WHEN br.outstanding_principal_at_date IS NOT NULL     THEN 'FMD_PIT'
        WHEN br.co_adjusted_balance IS NOT NULL               THEN 'CO_ADJUSTED'
        ELSE 'MISSING'
    END AS balance_source_flag,

    cs.outstanding_principal   AS principal_at_settlement,
    cs.fees_due                AS fees_at_settlement,
    cs.discount_pending        AS discount_at_settlement,

    -- ── Balance tier ─────────────────────────────────────────────────────────
    CASE
        WHEN COALESCE(br.balance_at_settlement_dacd,
                      br.outstanding_principal_at_date,
                      br.co_adjusted_balance) < 5000       THEN 'Small (<$5K)'
        WHEN COALESCE(br.balance_at_settlement_dacd,
                      br.outstanding_principal_at_date,
                      br.co_adjusted_balance) < 25000      THEN 'Medium ($5K-$25K)'
        WHEN COALESCE(br.balance_at_settlement_dacd,
                      br.outstanding_principal_at_date,
                      br.co_adjusted_balance) < 100000     THEN 'Large ($25K-$100K)'
        ELSE 'Enterprise ($100K+)'
    END AS balance_tier,

    -- ── Placement type ────────────────────────────────────────────────────────
    COALESCE(cs.placement_type_at_settlement, 'Unknown')      AS placement_type_at_settlement,
    cs.recovery_suggested_state                               AS recovery_state_at_settlement,
    cs.recovery_suggested_substate                            AS recovery_substate_at_settlement,

    -- ── [FIX-1] Offer terms ───────────────────────────────────────────────────
    s.offered_settlement_amount,
    s.scheduled_payment_count                                 AS scheduled_installment_count,
    COALESCE(s.settlement_type_code, 'Unknown')               AS offered_payment_structure,

    -- Offer acceptance gap: how much more/less was actually paid vs offered
    sp.settlement_payment_amount - s.offered_settlement_amount AS settlement_vs_offer_variance,
    ROUND((sp.settlement_payment_amount - s.offered_settlement_amount)
          / NULLIF(s.offered_settlement_amount, 0) * 100, 2)  AS settlement_vs_offer_variance_pct,

    -- ── Payment metrics ───────────────────────────────────────────────────────
    COALESCE(sp.settlement_payment_amount, 0)                 AS settlement_payment_amount,
    COALESCE(sp.settlement_payment_count,  0)                 AS settlement_payment_count,
    sp.first_settlement_payment_date,
    sp.last_settlement_payment_date,
    DATEDIFF('day', s.settlement_created_date,
             sp.first_settlement_payment_date)                AS days_to_first_payment,

    -- [FIX-4] Actual payment structure observed (lump sum vs installment)
    COALESCE(sp.payment_structure_actual, 'No Payments')      AS payment_structure_actual,

    -- ── Recovery rate ─────────────────────────────────────────────────────────
    ROUND(COALESCE(sp.settlement_payment_amount, 0)
          / NULLIF(COALESCE(br.balance_at_settlement_dacd,
                            br.outstanding_principal_at_date,
                            br.co_adjusted_balance), 0) * 100, 2) AS recovery_rate_pct,

    -- Offer-based recovery: actual paid vs offered amount
    ROUND(COALESCE(sp.settlement_payment_amount, 0)
          / NULLIF(s.offered_settlement_amount, 0) * 100, 2)  AS recovery_rate_vs_offer_pct,

    -- ── Discount (haircut) ────────────────────────────────────────────────────
    COALESCE(br.balance_at_settlement_dacd,
             br.outstanding_principal_at_date,
             br.co_adjusted_balance)
        - COALESCE(sp.settlement_payment_amount, 0)           AS discount_amount,

    ROUND((COALESCE(br.balance_at_settlement_dacd,
                    br.outstanding_principal_at_date,
                    br.co_adjusted_balance)
           - COALESCE(sp.settlement_payment_amount, 0))
          / NULLIF(COALESCE(br.balance_at_settlement_dacd,
                            br.outstanding_principal_at_date,
                            br.co_adjusted_balance), 0) * 100, 2) AS discount_pct,

    -- ── [FIX-2] Re-settlement flags ───────────────────────────────────────────
    s.is_re_settlement,
    s.has_prior_funded_settlement,
    s.total_settlement_attempts,

    -- ── Customer attributes ───────────────────────────────────────────────────
    ca.industry,
    ca.customer_state                                         AS geography,
    ca.credit_limit,
    ca.channel,
    ca.partner,
    ca.tier,

    -- ── Risk grade & VantageScore ─────────────────────────────────────────────
    rg.og_bucket,
    rg.og_bucket_group,
    vs.vantage_score,
    CASE 
        WHEN vs.vantage_score < 600 THEN '1. <600 (Subprime)'
        WHEN vs.vantage_score BETWEEN 600 AND 650 THEN '2. 600-650 (Near Prime)'
        WHEN vs.vantage_score BETWEEN 651 AND 700 THEN '3. 651-700 (Prime)'
        WHEN vs.vantage_score BETWEEN 701 AND 750 THEN '4. 701-750 (Prime Plus)'
        WHEN vs.vantage_score > 750 THEN '5. 750+ (Super Prime)'
        ELSE '6. Unknown'
    END AS vantage_score_bucket,

    -- ── Acquisition cohort ────────────────────────────────────────────────────
    ac.first_approved_time,
    ac.acquisition_cohort,

    -- ── Time on platform ──────────────────────────────────────────────────────
    top.first_draw_date,
    DATEDIFF('month', top.first_draw_date,
             s.settlement_created_date)                       AS months_on_platform_at_settlement

FROM settlements s
LEFT JOIN charge_off_data                    co   ON s.fbbid = co.fbbid
LEFT JOIN customer_state_at_settlement       cs   ON s.fbbid = cs.fbbid
LEFT JOIN dpd_at_settlement                  dpd
    ON s.fbbid = dpd.fbbid
   AND dpd.edate = s.settlement_created_date
LEFT JOIN first_delinquency                  fd   ON s.fbbid = fd.fbbid
LEFT JOIN settlement_payments                sp   ON s.fbbid = sp.fbbid
LEFT JOIN customer_attributes                ca   ON s.fbbid = ca.fbbid
LEFT JOIN time_on_platform                   top  ON s.fbbid = top.fbbid
LEFT JOIN balance_resolution                 br   ON s.fbbid = br.fbbid
LEFT JOIN risk_grade_at_settlement           rg   ON s.fbbid = rg.fbbid
LEFT JOIN vantage_at_settlement              vs   ON s.fbbid = vs.fbbid
LEFT JOIN acquisition_cohort                 ac   ON s.fbbid = ac.fbbid;


-- =====================================================
-- VIEW 3: SETTLEMENT CONVERSION FUNNEL
-- =====================================================
-- [NEW-2] Full offer-to-funded funnel including offer acceptance stage.
-- Monthly cohort: offer created → accepted/active → funded/failed/cancelled.
-- Tracks: offer acceptance rate, active-to-fund rate, overall funnel efficiency.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_conversion_funnel AS

WITH
-- All settlement attempts (not just latest) for offer funnel
all_attempts AS (
    SELECT
        fbbid,
        settlement_id,
        settlement_status,
        settlement_created_date,
        settlement_end_time,
        offered_settlement_amount,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        dpd_bucket_at_settlement,
        days_in_settlement,
        balance_at_settlement,
        settlement_payment_amount,
        recovery_rate_pct,
        discount_pct,
        is_re_settlement,
        settlement_month
    FROM analytics.credit.v_settlement_portfolio_base
),

monthly_settlements AS (
    SELECT
        settlement_month,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        dpd_bucket_at_settlement,
        is_re_settlement,

        COUNT(DISTINCT fbbid)                                         AS total_settlements_created,

        -- [NEW-2] Offer stage breakdown
        COUNT(DISTINCT CASE WHEN settlement_status IN ('ACTIVE','FUNDED','FAILED','CANCELLED')
                            THEN fbbid END)                           AS offers_accepted,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                            THEN fbbid END)                           AS active_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'
                            THEN fbbid END)                           AS funded_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                            THEN fbbid END)                           AS failed_cancelled_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'CANCELLED'
                            THEN fbbid END)                           AS cancelled_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FAILED'
                            THEN fbbid END)                           AS failed_settlements,

        -- Balance & recovery
        SUM(balance_at_settlement)                                    AS total_balance_at_settlement,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN balance_at_settlement END)                      AS funded_balance,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                  AS total_recovered_funded,

        -- Timing
        AVG(days_in_settlement)                                       AS avg_days_in_settlement,
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN days_in_settlement END)                         AS avg_days_to_fund,
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN recovery_rate_pct END)                          AS avg_recovery_rate_funded,
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN discount_pct END)                               AS avg_discount_pct_funded

    FROM all_attempts
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    settlement_month,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    dpd_bucket_at_settlement,
    is_re_settlement,

    total_settlements_created,
    offers_accepted,
    active_settlements,
    funded_settlements,
    failed_cancelled_settlements,
    cancelled_settlements,
    failed_settlements,

    -- [NEW-2] Stage conversion rates
    ROUND(offers_accepted       * 100.0 / NULLIF(total_settlements_created, 0), 2)  AS offer_acceptance_rate_pct,
    ROUND(funded_settlements    * 100.0 / NULLIF(total_settlements_created, 0), 2)  AS overall_funding_rate_pct,
    ROUND(funded_settlements    * 100.0 / NULLIF(offers_accepted, 0),           2)  AS active_to_funded_rate_pct,
    ROUND(failed_cancelled_settlements * 100.0 / NULLIF(total_settlements_created, 0), 2) AS failure_rate_pct,
    ROUND(active_settlements    * 100.0 / NULLIF(total_settlements_created, 0), 2)  AS still_active_rate_pct,

    -- Balance & recovery metrics
    total_balance_at_settlement,
    funded_balance,
    total_recovered_funded,

    ROUND(total_recovered_funded / NULLIF(funded_balance,               0) * 100, 2) AS cohort_recovery_rate_pct,
    ROUND(total_recovered_funded / NULLIF(total_balance_at_settlement,  0) * 100, 2) AS funnel_efficiency_pct,

    ROUND(avg_days_in_settlement,    1) AS avg_days_in_settlement,
    ROUND(avg_days_to_fund,          1) AS avg_days_to_fund,
    ROUND(avg_recovery_rate_funded,  2) AS avg_recovery_rate_funded,
    ROUND(avg_discount_pct_funded,   2) AS avg_discount_pct_funded

FROM monthly_settlements
ORDER BY settlement_month DESC, settlement_type, placement_type_at_settlement;


-- =====================================================
-- VIEW 4: SETTLEMENT VELOCITY & PAYMENT BEHAVIOR
-- =====================================================
-- Time-to-fund distributions and payment pattern analysis.
-- [FIX-4] Added payment structure (lump sum vs installment) breakdown.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_velocity AS

WITH velocity_metrics AS (
    SELECT
        settlement_month,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        mob_at_settlement,
        payment_structure_actual,           -- [FIX-4]

        COUNT(DISTINCT fbbid)                                                     AS funded_count,

        AVG(days_in_settlement)                                                   AS avg_days_to_fund,
        MEDIAN(days_in_settlement)                                                AS median_days_to_fund,
        MIN(days_in_settlement)                                                   AS min_days_to_fund,
        MAX(days_in_settlement)                                                   AS max_days_to_fund,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_in_settlement)          AS p25_days_to_fund,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_in_settlement)          AS p75_days_to_fund,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_in_settlement)          AS p90_days_to_fund,

        AVG(days_to_first_payment)                                                AS avg_days_to_first_payment,
        AVG(settlement_payment_count)                                             AS avg_payment_count,

        -- [FIX-4] Lump sum vs installment split
        COUNT(DISTINCT CASE WHEN payment_structure_actual = 'Lump Sum'
                            THEN fbbid END)                                       AS lump_sum_count,
        COUNT(DISTINCT CASE WHEN payment_structure_actual = 'Installment'
                            THEN fbbid END)                                       AS installment_count,

        AVG(CASE WHEN settlement_payment_count > 0
                 THEN settlement_payment_amount / settlement_payment_count
            END)                                                                  AS avg_payment_size,

        SUM(settlement_payment_amount)                                            AS total_recovered,
        SUM(balance_at_settlement)                                                AS total_funded_balance

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_status = 'FUNDED'
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    settlement_month,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    mob_at_settlement,
    payment_structure_actual,

    funded_count,

    ROUND(avg_days_to_fund,          1)  AS avg_days_to_fund,
    ROUND(median_days_to_fund,       1)  AS median_days_to_fund,
    ROUND(p25_days_to_fund,          1)  AS p25_days_to_fund,
    ROUND(p75_days_to_fund,          1)  AS p75_days_to_fund,
    ROUND(p90_days_to_fund,          1)  AS p90_days_to_fund,
    min_days_to_fund,
    max_days_to_fund,

    ROUND(avg_days_to_first_payment, 1)  AS avg_days_to_first_payment,
    ROUND(avg_payment_count,         1)  AS avg_payment_count,

    lump_sum_count,
    installment_count,
    ROUND(lump_sum_count    * 100.0 / NULLIF(funded_count, 0), 2) AS lump_sum_pct,
    ROUND(installment_count * 100.0 / NULLIF(funded_count, 0), 2) AS installment_pct,

    ROUND(avg_payment_size, 2)           AS avg_payment_size,

    total_recovered,
    total_funded_balance,
    ROUND(total_recovered / NULLIF(total_funded_balance, 0) * 100, 2) AS recovery_rate_pct,

    -- Velocity score: higher = faster to resolution
    ROUND(30.0 / NULLIF(avg_days_to_fund, 0), 2) AS velocity_score

FROM velocity_metrics
WHERE funded_count > 0
ORDER BY settlement_month DESC;


-- =====================================================
-- VIEW 5: RECOVERY RATE & DISCOUNT ANALYSIS
-- =====================================================
-- Settlement economics and haircut analysis.
-- Now includes offer-vs-actual variance metrics.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_recovery_discount AS

WITH discount_analysis AS (
    SELECT
        settlement_month,
        settlement_quarter,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        dpd_bucket_at_settlement,
        mob_at_settlement,
        payment_structure_actual,         -- [FIX-4]

        COUNT(DISTINCT fbbid)                                                      AS total_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END)      AS funded_settlements,

        SUM(balance_at_settlement)                                                 AS total_balance,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN balance_at_settlement END)                                   AS funded_balance,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                               AS total_recovered,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN discount_amount END)                                         AS total_discount,

        -- [FIX-1] Offer vs actual variance (funded only)
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_vs_offer_variance_pct END)                        AS avg_vs_offer_variance_pct,

        -- Recovery rate distributions
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END)     AS avg_recovery_rate,
        MEDIAN(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END)  AS median_recovery_rate,
        PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END
        )                                                                          AS p25_recovery_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END
        )                                                                          AS p75_recovery_rate,

        -- Discount distributions
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END)          AS avg_discount_pct,
        MEDIAN(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END)       AS median_discount_pct

    FROM analytics.credit.v_settlement_portfolio_base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT
    settlement_month,
    settlement_quarter,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    dpd_bucket_at_settlement,
    mob_at_settlement,
    payment_structure_actual,

    total_settlements,
    funded_settlements,
    ROUND(funded_settlements * 100.0 / NULLIF(total_settlements, 0), 2) AS funding_rate_pct,

    total_balance,
    funded_balance,
    total_recovered,
    total_discount,

    ROUND(total_recovered / NULLIF(funded_balance,  0) * 100, 2)        AS aggregate_recovery_rate_pct,
    ROUND(total_discount  / NULLIF(funded_balance,  0) * 100, 2)        AS aggregate_discount_pct,

    ROUND(avg_recovery_rate,    2)  AS avg_recovery_rate_pct,
    ROUND(median_recovery_rate, 2)  AS median_recovery_rate_pct,
    ROUND(p25_recovery_rate,    2)  AS p25_recovery_rate_pct,
    ROUND(p75_recovery_rate,    2)  AS p75_recovery_rate_pct,

    ROUND(avg_discount_pct,     2)  AS avg_discount_pct,
    ROUND(median_discount_pct,  2)  AS median_discount_pct,

    -- [FIX-1] Offer acceptance quality
    ROUND(avg_vs_offer_variance_pct, 2) AS avg_settlement_vs_offer_variance_pct,

    -- Unit economics
    ROUND(total_recovered / NULLIF(funded_settlements, 0), 2)           AS avg_recovery_per_settlement,
    ROUND(total_discount  / NULLIF(funded_settlements, 0), 2)           AS avg_discount_per_settlement

FROM discount_analysis
WHERE total_settlements > 0
ORDER BY settlement_month DESC;


-- =====================================================
-- VIEW 6: VINTAGE COHORT ANALYSIS — POST-CO
-- =====================================================
-- Settlement recovery curves by CO quarter and MOB.
-- Unchanged in structure; benefits from FIX-3 balance accuracy.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_vintage_postco AS

WITH vintage_cohorts AS (
    SELECT
        co_quarter,
        co_month,
        mob_at_settlement,
        settlement_month,
        balance_source_flag,              -- audit column from FIX-3

        COUNT(DISTINCT fbbid)                                                          AS settlements_created,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'  THEN fbbid END)         AS settlements_funded,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'  THEN fbbid END)         AS settlements_active,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                            THEN fbbid END)                                            AS settlements_failed,

        SUM(balance_at_settlement)                                                     AS total_balance_at_settlement,
        SUM(co_principal)                                                              AS total_co_principal,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                                   AS total_recovered,

        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct  END)        AS avg_recovery_rate,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct       END)        AS avg_discount_pct,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement  END)       AS avg_days_to_fund

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_type = 'Post-CO'
      AND co_quarter IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    co_quarter,
    co_month,
    mob_at_settlement,
    settlement_month,
    balance_source_flag,

    settlements_created,
    settlements_funded,
    settlements_active,
    settlements_failed,

    ROUND(settlements_funded * 100.0 / NULLIF(settlements_created, 0), 2) AS funding_rate_pct,

    total_balance_at_settlement,
    total_co_principal,
    total_recovered,

    ROUND(total_recovered / NULLIF(total_balance_at_settlement, 0) * 100, 2) AS recovery_rate_on_settlement_balance,
    ROUND(total_recovered / NULLIF(total_co_principal,           0) * 100, 2) AS recovery_rate_on_co_principal,

    ROUND(avg_recovery_rate,  2) AS avg_recovery_rate_pct,
    ROUND(avg_discount_pct,   2) AS avg_discount_pct,
    ROUND(avg_days_to_fund,   1) AS avg_days_to_fund,

    -- Cumulative vintage curves
    SUM(settlements_funded) OVER (
        PARTITION BY co_quarter ORDER BY mob_at_settlement
    )                                                                     AS cumulative_funded_by_mob,
    SUM(total_recovered)    OVER (
        PARTITION BY co_quarter ORDER BY mob_at_settlement
    )                                                                     AS cumulative_recovered_by_mob

FROM vintage_cohorts
ORDER BY co_quarter DESC, mob_at_settlement;


-- =====================================================
-- VIEW 7: VINTAGE COHORT ANALYSIS — PRE-CO
-- =====================================================
-- [NEW-1] Settlement recovery curves for Pre-CO settlements,
-- cohorted by the month a merchant first went delinquent and
-- the DPD bucket at which the settlement was offered.
-- Answers: Do early-intervention settlements (DPD 1-14) complete faster
-- and at higher rates than late-stage Pre-CO settlements (DPD 57-91)?

CREATE OR REPLACE VIEW analytics.credit.v_settlement_vintage_preco AS

WITH preco_cohorts AS (
    SELECT
        -- Vintage: month of first delinquency
        first_delinquency_quarter,
        first_delinquency_month,
        -- DPD bucket at settlement offer
        dpd_bucket_at_settlement,
        -- Months from first delinquency to settlement
        months_since_first_delinquency,
        settlement_month,

        COUNT(DISTINCT fbbid)                                                          AS settlements_created,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'  THEN fbbid END)         AS settlements_funded,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'  THEN fbbid END)         AS settlements_active,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                            THEN fbbid END)                                            AS settlements_failed,

        SUM(balance_at_settlement)                                                     AS total_balance_at_settlement,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                                   AS total_recovered,

        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct  END)        AS avg_recovery_rate,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct       END)        AS avg_discount_pct,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement  END)       AS avg_days_to_fund

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_type = 'Pre-CO'
      AND first_delinquency_quarter IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    first_delinquency_quarter,
    first_delinquency_month,
    dpd_bucket_at_settlement,
    months_since_first_delinquency,
    settlement_month,

    settlements_created,
    settlements_funded,
    settlements_active,
    settlements_failed,

    ROUND(settlements_funded * 100.0 / NULLIF(settlements_created, 0), 2) AS funding_rate_pct,

    total_balance_at_settlement,
    total_recovered,

    ROUND(total_recovered / NULLIF(total_balance_at_settlement, 0) * 100, 2) AS recovery_rate_pct,

    ROUND(avg_recovery_rate, 2) AS avg_recovery_rate_pct,
    ROUND(avg_discount_pct,  2) AS avg_discount_pct,
    ROUND(avg_days_to_fund,  1) AS avg_days_to_fund,

    -- Cumulative curves partitioned by delinquency vintage + DPD entry bucket
    SUM(settlements_funded) OVER (
        PARTITION BY first_delinquency_quarter, dpd_bucket_at_settlement
        ORDER BY months_since_first_delinquency
    )                                                                     AS cumulative_funded,
    SUM(total_recovered)    OVER (
        PARTITION BY first_delinquency_quarter, dpd_bucket_at_settlement
        ORDER BY months_since_first_delinquency
    )                                                                     AS cumulative_recovered

FROM preco_cohorts
ORDER BY first_delinquency_quarter DESC, dpd_bucket_at_settlement, months_since_first_delinquency;


-- =====================================================
-- VIEW 8: AGENT SETTLEMENT PERFORMANCE
-- =====================================================
-- [FIX-5] Attribution hardened:
--   - Window extended to 30 days (was 14) to capture slower negotiation cycles
--   - All agents who touched the account in window are captured (not just last)
--   - Primary agent = closest activity to settlement created date
--   - Assist agents = all other touching agents in window
--   - Metrics computed separately for primary vs assist attribution

CREATE OR REPLACE VIEW analytics.credit.v_settlement_agent_performance AS

WITH
-- All SF task activity with settlement-accepted disposition
agent_settlement_activity AS (
    SELECT
        CASE WHEN fundbox_id__c = 'Not Linked'
             THEN NULL
             ELSE TRY_TO_NUMBER(fundbox_id__c)
        END                        AS fbbid,
        assignee_name__c           AS agent_name,
        role_id_name__c            AS agent_role,
        DATE(createddate)          AS activity_date,
        calldisposition            AS disposition
    FROM external_data_sources.salesforce_nova.task
    WHERE LOWER(calldisposition) = 'settlement accepted'
      AND DATE(createddate) >= '2022-01-01'
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
),

-- [FIX-5] All agents who touched each settlement in 30-day window
-- Rank: 1 = primary (closest to settlement date), 2+ = assist
settlement_agent_touches AS (
    SELECT
        s.fbbid,
        s.settlement_id,
        a.agent_name,
        a.agent_role,
        a.activity_date                                                        AS settlement_accepted_date,
        DATEDIFF('day', a.activity_date, s.settlement_created_date)            AS days_before_settlement,
        ROW_NUMBER() OVER (
            PARTITION BY s.fbbid, s.settlement_id
            ORDER BY a.activity_date DESC         -- closest to settlement date = rank 1
        )                                                                      AS agent_rank,
        COUNT(*) OVER (
            PARTITION BY s.fbbid, s.settlement_id
        )                                                                      AS total_agents_on_settlement
    FROM analytics.credit.v_settlement_portfolio_base s
    LEFT JOIN agent_settlement_activity a
        ON s.fbbid = a.fbbid
       AND a.activity_date BETWEEN
               DATEADD('day', -30, s.settlement_created_date)   -- [FIX-5] extended to 30 days
           AND s.settlement_created_date
),

-- [FIX-5] Join attribution flags back to base data
settlement_with_agents AS (
    SELECT
        b.*,
        t.agent_name,
        t.agent_role,
        t.agent_rank,
        t.total_agents_on_settlement,
        CASE WHEN t.agent_rank = 1 THEN 'Primary' ELSE 'Assist' END AS attribution_type
    FROM analytics.credit.v_settlement_portfolio_base b
    LEFT JOIN settlement_agent_touches t ON b.fbbid = t.fbbid
    WHERE t.agent_name IS NOT NULL
),

-- Monthly aggregates per agent per attribution type
agent_monthly AS (
    SELECT
        agent_name,
        agent_role,
        attribution_type,                                     -- [FIX-5] Primary vs Assist
        DATE_TRUNC('month', settlement_created_date)          AS performance_month,

        COUNT(DISTINCT fbbid)                                                  AS settlements_attributed,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'
                            THEN fbbid END)                                    AS settlements_funded,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                            THEN fbbid END)                                    AS settlements_failed,

        SUM(balance_at_settlement)                                             AS total_balance_attributed,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                           AS total_recovered,

        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct      END) AS avg_discount_pct,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund

    FROM settlement_with_agents
    GROUP BY 1, 2, 3, 4
)

SELECT
    agent_name,
    agent_role,
    attribution_type,
    performance_month,

    settlements_attributed,
    settlements_funded,
    settlements_failed,

    ROUND(settlements_funded * 100.0 / NULLIF(settlements_attributed, 0), 2)   AS funding_rate_pct,

    total_balance_attributed,
    total_recovered,
    ROUND(total_recovered / NULLIF(total_balance_attributed, 0) * 100, 2)       AS recovery_efficiency_pct,

    ROUND(avg_recovery_rate,  2) AS avg_recovery_rate_pct,
    ROUND(avg_discount_pct,   2) AS avg_discount_pct,
    ROUND(avg_days_to_fund,   1) AS avg_days_to_fund,

    ROUND(total_recovered / NULLIF(settlements_funded, 0), 2)                   AS avg_recovery_per_funded_settlement,

    -- Rolling 3-month averages (primary attribution only is most useful for trending)
    AVG(settlements_funded) OVER (
        PARTITION BY agent_name, attribution_type
        ORDER BY performance_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                                           AS rolling_3m_avg_funded,
    AVG(total_recovered) OVER (
        PARTITION BY agent_name, attribution_type
        ORDER BY performance_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                                           AS rolling_3m_avg_recovered

FROM agent_monthly
ORDER BY performance_month DESC, attribution_type, total_recovered DESC;


-- =====================================================
-- VIEW 9: PORTFOLIO HEALTH & LIQUIDITY RISK
-- =====================================================
-- Operational snapshot of capital deployed in active settlements.
-- [FIX-3] Balance accuracy improvement flows through automatically.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_portfolio_health AS

WITH
current_portfolio AS (
    SELECT
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                            THEN fbbid END)                                    AS active_settlement_count,
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                 THEN balance_at_settlement END)                               AS active_settlement_balance,
        AVG(CASE WHEN settlement_status = 'ACTIVE'
                 THEN days_in_settlement END)                                  AS avg_days_active,

        -- Aging buckets (counts)
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                             AND days_in_settlement <= 30         THEN fbbid END) AS active_0_30_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                             AND days_in_settlement BETWEEN 31 AND 60 THEN fbbid END) AS active_31_60_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                             AND days_in_settlement BETWEEN 61 AND 90 THEN fbbid END) AS active_61_90_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                             AND days_in_settlement > 90          THEN fbbid END) AS active_90_plus_days,

        -- Aging buckets (balances)
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                  AND days_in_settlement <= 30              THEN balance_at_settlement END) AS balance_active_0_30,
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                  AND days_in_settlement BETWEEN 31 AND 60  THEN balance_at_settlement END) AS balance_active_31_60,
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                  AND days_in_settlement BETWEEN 61 AND 90  THEN balance_at_settlement END) AS balance_active_61_90,
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                  AND days_in_settlement > 90               THEN balance_at_settlement END) AS balance_active_90_plus,

        -- Last 30 days funded
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'
                             AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE)
                            THEN fbbid END)                                    AS funded_last_30d_count,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                  AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE)
                 THEN settlement_payment_amount END)                           AS recovered_last_30d,

        -- Last 30 days failed
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                             AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE)
                            THEN fbbid END)                                    AS failed_last_30d_count,
        SUM(CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                  AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE)
                 THEN balance_at_settlement END)                               AS failed_balance_last_30d,

        -- Re-settlement volume in portfolio
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'
                             AND is_re_settlement = TRUE     THEN fbbid END)   AS active_re_settlements,
        SUM(CASE WHEN settlement_status = 'ACTIVE'
                  AND is_re_settlement = TRUE
                 THEN balance_at_settlement END)                               AS active_re_settlement_balance

    FROM analytics.credit.v_settlement_portfolio_base
),

monthly_velocity AS (
    SELECT
        settlement_month,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END)  AS funded_count,
        SUM(CASE WHEN settlement_status = 'FUNDED'
                 THEN settlement_payment_amount END)                           AS recovered_amount,
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN days_in_settlement END)                                  AS avg_days_to_fund,
        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN recovery_rate_pct END)                                   AS avg_recovery_rate
    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_month >= DATEADD('month', -12, CURRENT_DATE)
    GROUP BY 1
),

velocity_trends AS (
    SELECT
        AVG(funded_count)       AS avg_monthly_funded,
        AVG(recovered_amount)   AS avg_monthly_recovery,
        AVG(avg_days_to_fund)   AS overall_avg_days_to_fund,
        AVG(avg_recovery_rate)  AS overall_avg_recovery_rate,
        STDDEV(recovered_amount) AS stddev_monthly_recovery
    FROM monthly_velocity
)

SELECT
    CURRENT_DATE AS report_date,

    -- Active portfolio
    cp.active_settlement_count,
    cp.active_settlement_balance           AS capital_deployed_in_settlements,
    ROUND(cp.avg_days_active, 1)           AS avg_days_in_active_settlement,

    -- Aging counts
    cp.active_0_30_days,
    cp.active_31_60_days,
    cp.active_61_90_days,
    cp.active_90_plus_days,

    -- Aging balances
    cp.balance_active_0_30,
    cp.balance_active_31_60,
    cp.balance_active_61_90,
    cp.balance_active_90_plus,

    -- Weighted aging risk score (0-300; higher = more capital in stale settlements)
    ROUND((COALESCE(cp.balance_active_31_60,  0) * 1
         + COALESCE(cp.balance_active_61_90,  0) * 2
         + COALESCE(cp.balance_active_90_plus,0) * 3)
          / NULLIF(cp.active_settlement_balance, 0) * 100, 2) AS aging_risk_score,

    -- Recent 30-day performance
    cp.funded_last_30d_count,
    cp.recovered_last_30d                  AS capital_recovered_last_30d,
    cp.failed_last_30d_count,
    cp.failed_balance_last_30d,

    -- [FIX-2] Re-settlement exposure
    cp.active_re_settlements,
    cp.active_re_settlement_balance,
    ROUND(cp.active_re_settlement_balance
          / NULLIF(cp.active_settlement_balance, 0) * 100, 2)  AS re_settlement_pct_of_active_balance,

    -- Velocity metrics (trailing 12 months)
    ROUND(vt.avg_monthly_funded,       0)  AS avg_monthly_funded_settlements,
    ROUND(vt.avg_monthly_recovery,     2)  AS avg_monthly_recovery_amount,
    ROUND(vt.overall_avg_days_to_fund, 1)  AS overall_avg_days_to_fund,
    ROUND(vt.overall_avg_recovery_rate,2)  AS overall_avg_recovery_rate_pct,

    -- Liquidity risk indicators
    ROUND(cp.active_settlement_balance
          / NULLIF(vt.avg_monthly_recovery, 0), 1)             AS months_to_clear_active_portfolio,
    ROUND(COALESCE(cp.balance_active_90_plus, 0)
          / NULLIF(cp.active_settlement_balance, 0) * 100, 2)  AS pct_balance_over_90_days,

    -- Recovery volatility
    ROUND(vt.stddev_monthly_recovery
          / NULLIF(vt.avg_monthly_recovery, 0) * 100, 2)       AS recovery_volatility_pct

FROM current_portfolio cp
CROSS JOIN velocity_trends vt;


-- =====================================================
-- VIEW 10: INDUSTRY & GEOGRAPHY SEGMENTATION
-- =====================================================
-- Settlement performance by merchant segment.
-- Now includes re-settlement rate per segment.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_segmentation AS

SELECT
    COALESCE(industry,   'Unknown') AS industry,
    COALESCE(geography,  'Unknown') AS geography,
    settlement_type,
    balance_tier,

    COUNT(DISTINCT fbbid)                                                          AS total_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'  THEN fbbid END)         AS funded_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE'  THEN fbbid END)         AS active_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                        THEN fbbid END)                                            AS failed_settlements,

    ROUND(COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END)
          * 100.0 / NULLIF(COUNT(DISTINCT fbbid), 0), 2)                           AS funding_rate_pct,

    -- [FIX-2] Re-settlement exposure per segment
    COUNT(DISTINCT CASE WHEN is_re_settlement = TRUE THEN fbbid END)               AS re_settlement_count,
    ROUND(COUNT(DISTINCT CASE WHEN is_re_settlement = TRUE THEN fbbid END)
          * 100.0 / NULLIF(COUNT(DISTINCT fbbid), 0), 2)                           AS re_settlement_rate_pct,

    SUM(balance_at_settlement)                                                     AS total_balance,
    SUM(CASE WHEN settlement_status = 'FUNDED'
             THEN settlement_payment_amount END)                                   AS total_recovered,

    ROUND(SUM(CASE WHEN settlement_status = 'FUNDED'
                   THEN settlement_payment_amount END)
          / NULLIF(SUM(CASE WHEN settlement_status = 'FUNDED'
                            THEN balance_at_settlement END), 0) * 100, 2)          AS recovery_rate_pct,

    ROUND(AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END), 2)    AS avg_discount_pct,
    ROUND(AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END),
          1)                                                                        AS avg_days_to_fund,
    ROUND(AVG(balance_at_settlement), 2)                                           AS avg_balance_at_settlement

FROM analytics.credit.v_settlement_portfolio_base
GROUP BY 1, 2, 3, 4
HAVING COUNT(DISTINCT fbbid) >= 5
ORDER BY total_recovered DESC;


-- =====================================================
-- VIEW 11: SETTLEMENT SURVIVAL CURVE
-- =====================================================
-- [NEW-3] Projects expected outcomes for ACTIVE settlements
-- based on historical resolution rates of comparable settlements.
-- Comparable = same settlement_type, balance_tier, dpd_bucket,
--              and similar days_in_settlement bracket.
-- Use this to forecast: of active settlements today, how many are
-- expected to fund vs fail, and when?

CREATE OR REPLACE VIEW analytics.credit.v_settlement_survival_curve AS

WITH
-- Historical resolution rates for funded settlements by cohort attributes + age bucket
historical_rates AS (
    SELECT
        settlement_type,
        balance_tier,
        dpd_bucket_at_settlement,
        -- Age bucket at resolution (10-day windows)
        FLOOR(days_in_settlement / 10) * 10                    AS age_bucket_days,

        COUNT(DISTINCT fbbid)                                  AS cohort_size,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED'
                            THEN fbbid END)                    AS funded_count,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED')
                            THEN fbbid END)                    AS failed_count,

        ROUND(COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END)
              * 100.0 / NULLIF(COUNT(DISTINCT fbbid), 0), 2)   AS historical_funding_rate,
        ROUND(COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED','CANCELLED') THEN fbbid END)
              * 100.0 / NULLIF(COUNT(DISTINCT fbbid), 0), 2)   AS historical_failure_rate,

        AVG(CASE WHEN settlement_status = 'FUNDED'
                 THEN recovery_rate_pct END)                   AS avg_historical_recovery_rate

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_status IN ('FUNDED','FAILED','CANCELLED')    -- resolved only
      AND settlement_created_date >= '2022-01-01'
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(DISTINCT fbbid) >= 10  -- minimum cohort size for stable rates
),

-- Currently active settlements with their current age
active_now AS (
    SELECT
        fbbid,
        settlement_id,
        settlement_type,
        balance_tier,
        dpd_bucket_at_settlement,
        days_in_settlement                                     AS current_age_days,
        FLOOR(days_in_settlement / 10) * 10                   AS age_bucket_days,
        balance_at_settlement,
        settlement_created_date,
        is_re_settlement
    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_status = 'ACTIVE'
)

SELECT
    a.fbbid,
    a.settlement_type,
    a.balance_tier,
    a.dpd_bucket_at_settlement,
    a.current_age_days,
    a.balance_at_settlement,
    a.settlement_created_date,
    a.is_re_settlement,

    -- Historical benchmarks for this cohort + age
    h.cohort_size                                              AS benchmark_cohort_size,
    ROUND(h.historical_funding_rate, 2)                        AS expected_funding_rate_pct,
    ROUND(h.historical_failure_rate, 2)                        AS expected_failure_rate_pct,
    ROUND(h.avg_historical_recovery_rate, 2)                   AS expected_recovery_rate_pct,

    -- Projected recovery dollar amount
    ROUND(a.balance_at_settlement
          * COALESCE(h.historical_funding_rate, 0) / 100
          * COALESCE(h.avg_historical_recovery_rate, 0) / 100, 2) AS projected_recovery_amount,

    -- Flag settlements with no benchmark (insufficient historical data)
    CASE WHEN h.cohort_size IS NULL THEN TRUE ELSE FALSE END   AS no_benchmark_available,

    -- Risk tier based on age and expected outcome
    CASE
        WHEN a.current_age_days > 90
             AND COALESCE(h.historical_funding_rate, 0) < 30  THEN 'High Risk'
        WHEN a.current_age_days BETWEEN 60 AND 90
             AND COALESCE(h.historical_funding_rate, 0) < 50  THEN 'Medium Risk'
        ELSE 'Normal'
    END                                                        AS survival_risk_tier

FROM active_now a
LEFT JOIN historical_rates h
    ON  a.settlement_type          = h.settlement_type
    AND a.balance_tier             = h.balance_tier
    AND a.dpd_bucket_at_settlement = h.dpd_bucket_at_settlement
    AND a.age_bucket_days          = h.age_bucket_days
ORDER BY a.current_age_days DESC, a.balance_at_settlement DESC;


-- =====================================================
-- VIEW 12: PAYMENT WATERFALL & FAILURE STAGE ANALYSIS
-- =====================================================
-- [NEW-4] For installment settlements that failed, identifies
-- WHICH payment in the sequence triggered the failure.
-- Answers: Are failures front-loaded (payment 1 defaults) or
-- late-stage (merchant pays most installments then stops)?
-- Also tracks payments received before failure to measure
-- partial recovery on non-completed settlements.

CREATE OR REPLACE VIEW analytics.credit.v_settlement_payment_waterfall AS

WITH
-- Enumerate every payment within a settlement window
settlement_payments_ranked AS (
    SELECT
        p.fbbid,
        s.settlement_id,
        s.settlement_status,
        s.settlement_created_date,
        s.settlement_end_time,
        s.settlement_type,
        s.balance_tier,
        s.dpd_bucket_at_settlement,
        s.offered_settlement_amount,
        s.scheduled_installment_count,
        s.balance_at_settlement,
        DATE(p.payment_event_time)                              AS payment_date,
        TO_DOUBLE(p.payment_components_json:PAYMENT_AMOUNT)     AS payment_amount,
        ROW_NUMBER() OVER (
            PARTITION BY p.fbbid, s.settlement_id
            ORDER BY p.payment_event_time ASC
        )                                                       AS payment_number,
        COUNT(*) OVER (
            PARTITION BY p.fbbid, s.settlement_id
        )                                                       AS total_payments_made
    FROM bi.finance.payments_model p
    INNER JOIN analytics.credit.v_settlement_portfolio_base s
        ON p.fbbid = s.fbbid
    WHERE p.payment_status = 'FUND'
      AND p.parent_payment_id IS NOT NULL
      AND DATE(p.payment_event_time)
          BETWEEN s.settlement_created_date
              AND COALESCE(s.settlement_end_time, CURRENT_DATE)
),

-- For failed settlements: how many payments cleared before failure?
failed_partial_recovery AS (
    SELECT
        fbbid,
        settlement_id,
        settlement_status,
        settlement_type,
        balance_tier,
        dpd_bucket_at_settlement,
        offered_settlement_amount,
        scheduled_installment_count,
        balance_at_settlement,
        total_payments_made                                     AS payments_before_failure,
        SUM(payment_amount)                                     AS partial_recovery_amount,

        -- Failure stage classification
        CASE
            WHEN total_payments_made = 0                        THEN 'No Payment Made'
            WHEN scheduled_installment_count IS NOT NULL
             AND total_payments_made = 1
             AND total_payments_made < scheduled_installment_count THEN 'First Payment Only'
            WHEN scheduled_installment_count IS NOT NULL
             AND total_payments_made
                 < FLOOR(scheduled_installment_count * 0.5)     THEN 'Early Stage (<50% installments)'
            WHEN scheduled_installment_count IS NOT NULL
             AND total_payments_made
                 BETWEEN FLOOR(scheduled_installment_count * 0.5)
                     AND scheduled_installment_count - 1        THEN 'Late Stage (≥50% installments)'
            ELSE 'Unclassified'
        END                                                     AS failure_stage,

        ROUND(SUM(payment_amount)
              / NULLIF(balance_at_settlement, 0) * 100, 2)      AS partial_recovery_rate_pct

    FROM settlement_payments_ranked
    WHERE settlement_status IN ('FAILED','CANCELLED')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

-- Aggregate failure stage distribution
failure_stage_summary AS (
    SELECT
        settlement_type,
        balance_tier,
        dpd_bucket_at_settlement,
        failure_stage,

        COUNT(DISTINCT fbbid)                                   AS failed_settlement_count,
        SUM(balance_at_settlement)                              AS total_balance_at_failure,
        SUM(partial_recovery_amount)                            AS total_partial_recovery,
        AVG(partial_recovery_rate_pct)                          AS avg_partial_recovery_rate,
        AVG(payments_before_failure)                            AS avg_payments_before_failure

    FROM failed_partial_recovery
    GROUP BY 1, 2, 3, 4
)

SELECT
    settlement_type,
    balance_tier,
    dpd_bucket_at_settlement,
    failure_stage,

    failed_settlement_count,
    ROUND(failed_settlement_count * 100.0
          / NULLIF(SUM(failed_settlement_count) OVER (
                PARTITION BY settlement_type, balance_tier, dpd_bucket_at_settlement
            ), 0), 2)                                           AS pct_of_failures_in_stage,

    total_balance_at_failure,
    total_partial_recovery,
    ROUND(total_partial_recovery
          / NULLIF(total_balance_at_failure, 0) * 100, 2)       AS aggregate_partial_recovery_pct,
    ROUND(avg_partial_recovery_rate,   2)                       AS avg_partial_recovery_rate_pct,
    ROUND(avg_payments_before_failure, 1)                       AS avg_payments_before_failure

FROM failure_stage_summary
ORDER BY settlement_type, balance_tier, dpd_bucket_at_settlement,
    -- Order failure stages logically
    CASE failure_stage
        WHEN 'No Payment Made'              THEN 1
        WHEN 'First Payment Only'           THEN 2
        WHEN 'Early Stage (<50% installments)' THEN 3
        WHEN 'Late Stage (≥50% installments)'  THEN 4
        ELSE 5
    END;


-- =====================================================
-- TABLE 13: RAW SETTLEMENT DATA (FBBID-LEVEL)
-- =====================================================
-- Flat table with ALL settlements (multiple rows per fbbid if multiple settlements)
-- For ad-hoc analysis, exports, and data exploration
-- Includes all enriched fields from the base view plus history context

CREATE OR REPLACE TABLE analytics.credit.settlement_raw_data AS

SELECT
    -- ══════════════════════════════════════════════════════════════════════════
    -- IDENTIFIERS
    -- ══════════════════════════════════════════════════════════════════════════
    h.fbbid,
    h.settlement_id,
    h.settlement_seq_asc                                      AS settlement_sequence,
    h.total_settlement_attempts,
    h.is_re_settlement,
    h.has_prior_funded_settlement,
    h.is_latest_settlement,

    -- ══════════════════════════════════════════════════════════════════════════
    -- SETTLEMENT STATUS & TIMELINE
    -- ══════════════════════════════════════════════════════════════════════════
    h.settlement_status,
    h.settlement_created_date,
    h.settlement_end_time,
    h.corrected_created_date,
    DATEDIFF('day', h.settlement_created_date, 
             COALESCE(h.settlement_end_time, CURRENT_DATE))   AS days_in_settlement,
    CASE
        WHEN h.settlement_status = 'FUNDED'                   THEN 'Completed'
        WHEN h.settlement_status = 'ACTIVE'                   THEN 'In Progress'
        WHEN h.settlement_status IN ('FAILED','CANCELLED')    THEN 'Failed/Cancelled'
        ELSE 'Other'
    END                                                       AS settlement_lifecycle_stage,

    -- ══════════════════════════════════════════════════════════════════════════
    -- TIME DIMENSIONS
    -- ══════════════════════════════════════════════════════════════════════════
    DATE_TRUNC('month', h.settlement_created_date)            AS settlement_month,
    DATE_TRUNC('quarter', h.settlement_created_date)          AS settlement_quarter,
    YEAR(h.settlement_created_date)                           AS settlement_year,
    DATE_TRUNC('week', h.settlement_created_date)             AS settlement_week,

    -- ══════════════════════════════════════════════════════════════════════════
    -- OFFER TERMS [FIX-1]
    -- ══════════════════════════════════════════════════════════════════════════
    h.offered_settlement_amount,
    h.scheduled_payment_count                                 AS scheduled_installment_count,
    h.settlement_type_code                                    AS offered_payment_structure,

    -- ══════════════════════════════════════════════════════════════════════════
    -- PRE-CO VS POST-CO CLASSIFICATION
    -- ══════════════════════════════════════════════════════════════════════════
    CASE
        WHEN co.charge_off_date IS NULL                       THEN 'Pre-CO'
        WHEN h.settlement_created_date < co.charge_off_date   THEN 'Pre-CO'
        ELSE 'Post-CO'
    END                                                       AS settlement_type,

    -- ══════════════════════════════════════════════════════════════════════════
    -- CHARGE-OFF CONTEXT (POST-CO)
    -- ══════════════════════════════════════════════════════════════════════════
    co.charge_off_date,
    LAST_DAY(co.charge_off_date)                              AS co_month,
    YEAR(co.charge_off_date) || '-Q' || QUARTER(co.charge_off_date) AS co_quarter,
    co.co_principal,
    DATEDIFF('day', co.charge_off_date, h.settlement_created_date) AS days_since_co,
    FLOOR(DATEDIFF('day', co.charge_off_date, h.settlement_created_date) / 30) AS mob_at_settlement,

    -- ══════════════════════════════════════════════════════════════════════════
    -- DPD CONTEXT (PRE-CO)
    -- ══════════════════════════════════════════════════════════════════════════
    dpd.dpd_days                                               AS dpd_at_settlement,
    dpd.is_charged_off                                         AS was_charged_off_at_settlement,
    CASE
        WHEN dpd.dpd_days BETWEEN 1 AND 14                     THEN '1-2 (DPD 1-14)'
        WHEN dpd.dpd_days BETWEEN 15 AND 56                    THEN '3-8 (DPD 15-56)'
        WHEN dpd.dpd_days BETWEEN 57 AND 91                    THEN '9-13 (DPD 57-91)'
        WHEN dpd.is_charged_off = 1 OR dpd.dpd_days > 91       THEN 'Charged Off'
        WHEN dpd.dpd_days = 0 OR dpd.dpd_days IS NULL          THEN 'Current'
        ELSE 'Unknown'
    END                                                        AS dpd_bucket_at_settlement,

    -- ══════════════════════════════════════════════════════════════════════════
    -- FIRST DELINQUENCY (PRE-CO VINTAGE)
    -- ══════════════════════════════════════════════════════════════════════════
    fd.first_delinquency_date,
    DATE_TRUNC('month', fd.first_delinquency_date)             AS first_delinquency_month,
    YEAR(fd.first_delinquency_date) || '-Q' || QUARTER(fd.first_delinquency_date) AS first_delinquency_quarter,
    DATEDIFF('month', fd.first_delinquency_date, h.settlement_created_date) AS months_since_first_delinquency,

    -- ══════════════════════════════════════════════════════════════════════════
    -- BALANCE [FIX-3 - HARDENED FALLBACK]
    -- ══════════════════════════════════════════════════════════════════════════
    COALESCE(
        cs.balance_at_settlement_dacd,
        dpd.outstanding_principal_at_date,
        CASE
            WHEN co.charge_off_date IS NOT NULL
             AND h.settlement_created_date >= co.charge_off_date
            THEN GREATEST(co.co_principal - COALESCE(pco.payments_post_co_pre_settlement, 0), 0)
            ELSE NULL
        END
    )                                                          AS balance_at_settlement,
    
    CASE
        WHEN cs.balance_at_settlement_dacd IS NOT NULL         THEN 'DACD'
        WHEN dpd.outstanding_principal_at_date IS NOT NULL     THEN 'FMD_PIT'
        WHEN co.co_principal IS NOT NULL                       THEN 'CO_ADJUSTED'
        ELSE 'MISSING'
    END                                                        AS balance_source_flag,
    
    cs.outstanding_principal                                   AS principal_at_settlement,
    cs.fees_due                                                AS fees_at_settlement,
    cs.discount_pending                                        AS discount_at_settlement,

    CASE
        WHEN COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date) < 5000 THEN 'Small (<$5K)'
        WHEN COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date) < 25000 THEN 'Medium ($5K-$25K)'
        WHEN COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date) < 100000 THEN 'Large ($25K-$100K)'
        ELSE 'Enterprise ($100K+)'
    END                                                        AS balance_tier,

    -- ══════════════════════════════════════════════════════════════════════════
    -- PLACEMENT & RECOVERY STATE
    -- ══════════════════════════════════════════════════════════════════════════
    COALESCE(cs.placement_type_at_settlement, 'Unknown')       AS placement_type_at_settlement,
    cs.recovery_suggested_state                                AS recovery_state_at_settlement,
    cs.recovery_suggested_substate                             AS recovery_substate_at_settlement,

    -- ══════════════════════════════════════════════════════════════════════════
    -- PAYMENT METRICS
    -- ══════════════════════════════════════════════════════════════════════════
    COALESCE(sp.settlement_payment_amount, 0)                  AS settlement_payment_amount,
    COALESCE(sp.settlement_payment_count, 0)                   AS settlement_payment_count,
    sp.first_settlement_payment_date,
    sp.last_settlement_payment_date,
    DATEDIFF('day', h.settlement_created_date, sp.first_settlement_payment_date) AS days_to_first_payment,
    COALESCE(sp.payment_structure_actual, 'No Payments')       AS payment_structure_actual,

    -- ══════════════════════════════════════════════════════════════════════════
    -- RECOVERY METRICS
    -- ══════════════════════════════════════════════════════════════════════════
    ROUND(COALESCE(sp.settlement_payment_amount, 0)
          / NULLIF(COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date), 0) * 100, 2) AS recovery_rate_pct,
    
    ROUND(COALESCE(sp.settlement_payment_amount, 0)
          / NULLIF(h.offered_settlement_amount, 0) * 100, 2)   AS recovery_rate_vs_offer_pct,

    COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date, 0)
        - COALESCE(sp.settlement_payment_amount, 0)            AS discount_amount,
    
    ROUND((COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date, 0)
           - COALESCE(sp.settlement_payment_amount, 0))
          / NULLIF(COALESCE(cs.balance_at_settlement_dacd, dpd.outstanding_principal_at_date), 0) * 100, 2) AS discount_pct,

    sp.settlement_payment_amount - h.offered_settlement_amount AS settlement_vs_offer_variance,
    ROUND((sp.settlement_payment_amount - h.offered_settlement_amount)
          / NULLIF(h.offered_settlement_amount, 0) * 100, 2)   AS settlement_vs_offer_variance_pct,

    -- ══════════════════════════════════════════════════════════════════════════
    -- CUSTOMER ATTRIBUTES
    -- ══════════════════════════════════════════════════════════════════════════
    ca.industry,
    ca.customer_state                                          AS geography,
    ca.credit_limit,
    ca.channel,
    ca.partner,
    ca.tier,

    -- ══════════════════════════════════════════════════════════════════════════
    -- RISK GRADE & CREDIT SCORE
    -- ══════════════════════════════════════════════════════════════════════════
    rg.og_bucket,
    rg.og_bucket_group,
    vs.vantage_score,
    CASE 
        WHEN vs.vantage_score < 600 THEN '1. <600 (Subprime)'
        WHEN vs.vantage_score BETWEEN 600 AND 650 THEN '2. 600-650 (Near Prime)'
        WHEN vs.vantage_score BETWEEN 651 AND 700 THEN '3. 651-700 (Prime)'
        WHEN vs.vantage_score BETWEEN 701 AND 750 THEN '4. 701-750 (Prime Plus)'
        WHEN vs.vantage_score > 750 THEN '5. 750+ (Super Prime)'
        ELSE '6. Unknown'
    END                                                        AS vantage_score_bucket,

    -- ══════════════════════════════════════════════════════════════════════════
    -- ACQUISITION COHORT & TIME ON PLATFORM
    -- ══════════════════════════════════════════════════════════════════════════
    ac.first_approved_time,
    ac.acquisition_cohort,
    top.first_draw_date,
    DATEDIFF('month', top.first_draw_date, h.settlement_created_date) AS months_on_platform_at_settlement,

    -- ══════════════════════════════════════════════════════════════════════════
    -- METADATA
    -- ══════════════════════════════════════════════════════════════════════════
    CURRENT_TIMESTAMP()                                        AS etl_loaded_at

FROM analytics.credit.v_settlement_history_base h

-- Charge-off data
LEFT JOIN (
    SELECT fbbid, MIN(charge_off_date) AS charge_off_date, SUM(outstanding_principal_due) AS co_principal
    FROM bi.finance.finance_metrics_daily
    WHERE is_charged_off = 1 AND product_type <> 'Flexpay'
      AND original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fbbid
) co ON h.fbbid = co.fbbid

-- Payments between CO and settlement
LEFT JOIN (
    SELECT p.fbbid, s.settlement_created_date,
           SUM(TO_DOUBLE(p.payment_components_json:PAYMENT_AMOUNT)) AS payments_post_co_pre_settlement
    FROM bi.finance.payments_model p
    INNER JOIN analytics.credit.v_settlement_history_base s ON p.fbbid = s.fbbid
    INNER JOIN (
        SELECT fbbid, MIN(charge_off_date) AS charge_off_date
        FROM bi.finance.finance_metrics_daily WHERE is_charged_off = 1
        GROUP BY fbbid
    ) co ON p.fbbid = co.fbbid
    WHERE p.payment_status = 'FUND' AND p.parent_payment_id IS NOT NULL
      AND DATE(p.payment_event_time) > co.charge_off_date
      AND DATE(p.payment_event_time) < s.settlement_created_date
    GROUP BY p.fbbid, s.settlement_created_date
) pco ON h.fbbid = pco.fbbid AND h.settlement_created_date = pco.settlement_created_date

-- Customer state at settlement
LEFT JOIN (
    SELECT s.fbbid, s.settlement_created_date,
           dacd.recovery_suggested_state, dacd.recovery_suggested_substate,
           dacd.outstanding_principal, dacd.fees_due, dacd.discount_pending,
           (dacd.outstanding_principal + dacd.fees_due - COALESCE(dacd.discount_pending, 0)) AS balance_at_settlement_dacd,
           CASE
               WHEN dacd.recovery_suggested_state IN ('ILR','LR','ER','FB_TL','CB_DLQ','HEAL','TR_ILR','EOL','PRELIT','LPD','MCA_HE')
                    OR dacd.recovery_suggested_state IS NULL THEN 'Internal'
               WHEN dacd.recovery_suggested_state IN ('ELR','PROLIT','TR_LR') THEN 'External'
               ELSE 'Unknown'
           END AS placement_type_at_settlement
    FROM analytics.credit.v_settlement_history_base s
    LEFT JOIN bi.public.daily_approved_customers_data dacd ON s.fbbid = dacd.fbbid AND dacd.edate = s.settlement_created_date
) cs ON h.fbbid = cs.fbbid AND h.settlement_created_date = cs.settlement_created_date

-- DPD at settlement
LEFT JOIN (
    SELECT fbbid, edate, MAX(COALESCE(dpd_days, 0)) AS dpd_days, MAX(is_charged_off) AS is_charged_off,
           SUM(outstanding_principal_due) AS outstanding_principal_at_date
    FROM bi.finance.finance_metrics_daily
    WHERE product_type <> 'Flexpay' AND original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fbbid, edate
) dpd ON h.fbbid = dpd.fbbid AND dpd.edate = h.settlement_created_date

-- First delinquency
LEFT JOIN (
    SELECT fbbid, MIN(edate) AS first_delinquency_date
    FROM bi.finance.finance_metrics_daily
    WHERE dpd_days >= 1 AND product_type <> 'Flexpay' AND original_payment_plan_description NOT LIKE '%Term Loan%'
    GROUP BY fbbid
) fd ON h.fbbid = fd.fbbid

-- Settlement payments
LEFT JOIN (
    SELECT p.fbbid, s.settlement_created_date,
           SUM(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE)
                    THEN TO_DOUBLE(p.payment_components_json:PAYMENT_AMOUNT) ELSE 0 END) AS settlement_payment_amount,
           COUNT(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE) THEN 1 END) AS settlement_payment_count,
           MIN(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) >= s.settlement_created_date THEN DATE(p.payment_event_time) END) AS first_settlement_payment_date,
           MAX(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE) THEN DATE(p.payment_event_time) END) AS last_settlement_payment_date,
           CASE WHEN COUNT(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE) THEN 1 END) = 1 THEN 'Lump Sum'
                WHEN COUNT(CASE WHEN p.payment_status = 'FUND' AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE) THEN 1 END) > 1 THEN 'Installment'
                ELSE 'No Payments' END AS payment_structure_actual
    FROM bi.finance.payments_model p
    INNER JOIN analytics.credit.v_settlement_history_base s ON p.fbbid = s.fbbid
    WHERE p.parent_payment_id IS NOT NULL
    GROUP BY p.fbbid, s.settlement_created_date, s.settlement_end_time
) sp ON h.fbbid = sp.fbbid AND h.settlement_created_date = sp.settlement_created_date

-- Customer attributes
LEFT JOIN (
    SELECT DISTINCT fbbid,
           FIRST_VALUE(industry) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS industry,
           FIRST_VALUE(state) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS customer_state,
           FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS credit_limit,
           FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS channel,
           FIRST_VALUE(partner) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS partner,
           FIRST_VALUE(tier) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS tier
    FROM bi.public.daily_approved_customers_data WHERE fbbid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate DESC) = 1
) ca ON h.fbbid = ca.fbbid

-- Time on platform
LEFT JOIN (
    SELECT fbbid, MIN(edate) AS first_draw_date
    FROM bi.finance.finance_metrics_daily WHERE outstanding_principal_due > 0
    GROUP BY fbbid
) top ON h.fbbid = top.fbbid

-- Acquisition cohort
LEFT JOIN (
    SELECT fbbid, first_approved_time,
           CASE 
               WHEN first_approved_time::DATE < '2020-01-01' THEN '1. Pre-2020'
               WHEN first_approved_time::DATE BETWEEN '2020-01-01' AND '2021-12-31' THEN '2. 2020-2021'
               WHEN first_approved_time::DATE BETWEEN '2022-01-01' AND '2023-12-31' THEN '3. 2022-2023'
               WHEN first_approved_time::DATE BETWEEN '2024-01-01' AND '2024-12-31' THEN '4. 2024'
               WHEN first_approved_time::DATE >= '2025-01-01' THEN '5. 2025+'
               ELSE '6. Unknown'
           END AS acquisition_cohort
    FROM bi.public.customers_data WHERE fbbid IS NOT NULL
) ac ON h.fbbid = ac.fbbid

-- Risk grade
LEFT JOIN (
    SELECT s.fbbid, s.settlement_created_date, og.OG_BUCKET AS og_bucket,
           CASE WHEN og.OG_BUCKET BETWEEN 1 AND 4 THEN '1. Low Risk (1-4)'
                WHEN og.OG_BUCKET BETWEEN 5 AND 7 THEN '2. Medium-Low (5-7)'
                WHEN og.OG_BUCKET BETWEEN 8 AND 10 THEN '3. Medium (8-10)'
                WHEN og.OG_BUCKET BETWEEN 11 AND 12 THEN '4. Medium-High (11-12)'
                WHEN og.OG_BUCKET BETWEEN 13 AND 15 THEN '5. High Risk (13-15)'
                ELSE '6. Unknown' END AS og_bucket_group
    FROM analytics.credit.v_settlement_history_base s
    LEFT JOIN ANALYTICS.CREDIT.OG_MODEL_SCORES_RETROSCORED_V1_1 og
        ON s.fbbid = og.fbbid AND og.edate = DATEADD('day', -1, s.settlement_created_date)
) rg ON h.fbbid = rg.fbbid AND h.settlement_created_date = rg.settlement_created_date

-- Vantage score
LEFT JOIN (
    SELECT s.fbbid, s.settlement_created_date,
           CASE WHEN DATEDIFF('day', dacd.credit_score_json:"VantageScore 4.0":"created_time"::TIMESTAMP, dacd.edate) <= 90
                THEN dacd.credit_score_json:"VantageScore 4.0":"score"::INT ELSE NULL END AS vantage_score
    FROM analytics.credit.v_settlement_history_base s
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON s.fbbid = dacd.fbbid AND dacd.edate = DATEADD('day', -1, s.settlement_created_date)
) vs ON h.fbbid = vs.fbbid AND h.settlement_created_date = vs.settlement_created_date;


-- =====================================================
-- EXECUTIVE SUMMARY QUERIES
-- =====================================================

-- Query 1: Overall Portfolio Snapshot
/*
SELECT
    settlement_type,
    settlement_lifecycle_stage,
    balance_source_flag,                      -- audit: how reliable is the balance?
    COUNT(DISTINCT fbbid)                     AS settlement_count,
    SUM(balance_at_settlement)                AS total_balance,
    SUM(settlement_payment_amount)            AS total_recovered,
    ROUND(SUM(settlement_payment_amount)
          / NULLIF(SUM(balance_at_settlement), 0) * 100, 2) AS recovery_rate_pct,
    ROUND(AVG(discount_pct), 2)               AS avg_discount_pct,
    ROUND(AVG(days_in_settlement), 1)         AS avg_days_in_settlement
FROM analytics.credit.v_settlement_portfolio_base
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
*/

-- Query 2: Monthly Trend Summary (Pre-CO and Post-CO)
/*
SELECT
    settlement_month,
    settlement_type,
    COUNT(DISTINCT fbbid)                     AS settlements_created,
    COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded,
    SUM(CASE WHEN settlement_status = 'FUNDED'
             THEN settlement_payment_amount END)               AS recovered,
    ROUND(COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END)
          * 100.0 / NULLIF(COUNT(DISTINCT fbbid), 0), 2)      AS funding_rate_pct,
    COUNT(DISTINCT CASE WHEN is_re_settlement = TRUE
                        THEN fbbid END)                        AS re_settlements
FROM analytics.credit.v_settlement_portfolio_base
WHERE settlement_month >= DATEADD('month', -12, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
*/

-- Query 3: Portfolio Health Dashboard
/*
SELECT * FROM analytics.credit.v_settlement_portfolio_health;
*/

-- Query 4: Top Performing Agents — Primary Attribution, Last 3 Months
/*
SELECT
    agent_name,
    agent_role,
    SUM(settlements_funded)                   AS total_funded,
    SUM(total_recovered)                      AS total_recovered,
    ROUND(AVG(funding_rate_pct),       2)     AS avg_funding_rate_pct,
    ROUND(AVG(avg_recovery_rate_pct),  2)     AS avg_recovery_rate_pct,
    ROUND(AVG(avg_discount_pct),       2)     AS avg_discount_given_pct
FROM analytics.credit.v_settlement_agent_performance
WHERE performance_month >= DATEADD('month', -3, CURRENT_DATE)
  AND attribution_type = 'Primary'
GROUP BY 1, 2
HAVING SUM(settlements_funded) >= 5
ORDER BY total_recovered DESC
LIMIT 20;
*/

-- Query 5: Post-CO Vintage Performance Curve
/*
SELECT
    co_quarter,
    mob_at_settlement,
    SUM(settlements_funded)                   AS funded,
    SUM(total_recovered)                      AS recovered,
    MAX(cumulative_recovered_by_mob)          AS cumulative_recovered
FROM analytics.credit.v_settlement_vintage_postco
WHERE co_quarter >= '2022-Q1'
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- Query 6: Pre-CO Vintage — DPD bucket comparison
/*
SELECT
    first_delinquency_quarter,
    dpd_bucket_at_settlement,
    SUM(settlements_created)                  AS created,
    SUM(settlements_funded)                   AS funded,
    ROUND(SUM(settlements_funded) * 100.0
          / NULLIF(SUM(settlements_created), 0), 2) AS funding_rate_pct,
    ROUND(AVG(avg_recovery_rate_pct), 2)      AS avg_recovery_rate_pct,
    MAX(cumulative_recovered)                 AS cumulative_recovered
FROM analytics.credit.v_settlement_vintage_preco
WHERE first_delinquency_quarter >= '2022-Q1'
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- Query 7: Active Settlement Survival Risk
/*
SELECT
    survival_risk_tier,
    settlement_type,
    balance_tier,
    COUNT(DISTINCT fbbid)                     AS active_count,
    SUM(balance_at_settlement)                AS total_balance_at_risk,
    SUM(projected_recovery_amount)            AS total_projected_recovery,
    ROUND(AVG(expected_funding_rate_pct), 2)  AS avg_expected_funding_rate,
    COUNT(DISTINCT CASE WHEN no_benchmark_available = TRUE
                        THEN fbbid END)       AS no_benchmark_count
FROM analytics.credit.v_settlement_survival_curve
GROUP BY 1, 2, 3
ORDER BY
    CASE survival_risk_tier WHEN 'High Risk' THEN 1
                             WHEN 'Medium Risk' THEN 2
                             ELSE 3 END,
    total_balance_at_risk DESC;
*/

-- Query 8: Payment Failure Stage Distribution
/*
SELECT
    settlement_type,
    failure_stage,
    SUM(failed_settlement_count)              AS failed_count,
    ROUND(AVG(pct_of_failures_in_stage), 2)   AS avg_pct_in_stage,
    SUM(total_partial_recovery)               AS partial_recovery_recovered,
    ROUND(AVG(aggregate_partial_recovery_pct),2) AS avg_partial_recovery_pct
FROM analytics.credit.v_settlement_payment_waterfall
GROUP BY 1, 2
ORDER BY 1,
    CASE failure_stage
        WHEN 'No Payment Made'                 THEN 1
        WHEN 'First Payment Only'              THEN 2
        WHEN 'Early Stage (<50% installments)' THEN 3
        WHEN 'Late Stage (≥50% installments)'  THEN 4
        ELSE 5
    END;
*/

-- Query 9: Raw Data Export (for Excel / ad-hoc analysis)
/*
SELECT * FROM analytics.credit.settlement_raw_data
WHERE settlement_created_date >= '2024-01-01'
ORDER BY fbbid, settlement_sequence;
*/
