-- =====================================================
-- SETTLEMENT MONTHLY ANALYSIS — FIXED VERSION
-- =====================================================
-- Fixes applied vs original:
--
--  FIX 1 | fbbid VARIANT → VARCHAR cast on every join/filter
--         | fbbid is VARIANT in cjk_v_backy_settlements and settlement_monthly_base.
--         | All joins that match fbbid across tables now use fbbid::VARCHAR.
--
--  FIX 2 | Scoring table: correct column + correct identifier_type
--         | Column is IDENTIFIER_ID (TEXT), not "fbbid".
--         | identifier_type = 'FUNDBOX_BUSINESS' (confirmed from live table).
--         | Join: b.fbbid::VARCHAR = rs.identifier_id
--
--  FIX 3 | DPD_AT_SETTLEMENT stored as VARIANT → cast to INT at base table
--         | Source dpd_days is VARIANT; explicit ::INT cast applied in dpd_data CTE
--         | and in the final SELECT so downstream numeric comparisons work.
--
--  FIX 4 | Payment JSON extraction: TRY_TO_DOUBLE + ::VARCHAR for safety
--         | Prevents silent NULLs when PAYMENT_COMPONENTS_JSON key is missing.
--
--  FIX 5 | payment_structure and mob_bucket derived columns added
--         | These were referenced in enrichment queries but never computed.
-- =====================================================


-- =====================================================
-- TABLE 1: SETTLEMENT BASE DATA  (FIXED)
-- =====================================================

CREATE OR REPLACE TABLE analytics.credit.settlement_monthly_base AS

WITH

-- ── FIX 1: fbbid is VARIANT — cast to VARCHAR throughout ──────────────────
settlements_all AS (
    SELECT
        fbbid::VARCHAR                AS fbbid,          -- FIX 1: VARIANT → VARCHAR
        settlement_created_date,
        settlement_end_time,
        current_status                AS settlement_status,
        event_time,
        ROW_NUMBER() OVER (
            PARTITION BY fbbid::VARCHAR, settlement_created_date
            ORDER BY event_time DESC
        )                             AS rn
    FROM analytics.credit.cjk_v_backy_settlements
    WHERE settlement_created_date >= '2022-01-01'
),

settlements_latest AS (
    SELECT
        fbbid,
        settlement_created_date,
        settlement_end_time,
        settlement_status,
        event_time
    FROM settlements_all
    WHERE rn = 1
),

-- Charge-off data
charge_off AS (
    SELECT
        fbbid::VARCHAR                AS fbbid,          -- FIX 1
        MIN(charge_off_date)          AS charge_off_date,
        SUM(outstanding_principal_due) AS co_principal
    FROM bi.finance.finance_metrics_daily
    WHERE is_charged_off = 1
      AND product_type <> 'Flexpay'
    GROUP BY fbbid::VARCHAR
),

-- Customer state at settlement (point-in-time)
customer_state AS (
    SELECT
        s.fbbid,
        s.settlement_created_date,
        dacd.outstanding_principal,
        dacd.recovery_suggested_state,
        CASE
            WHEN dacd.recovery_suggested_state IN (
                'ILR','LR','ER','FB_TL','CB_DLQ','HEAL',
                'TR_ILR','EOL','PRELIT','LPD','MCA_HE'
            ) OR dacd.recovery_suggested_state IS NULL THEN 'Internal'
            WHEN dacd.recovery_suggested_state IN ('ELR','PROLIT','TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_type
    FROM settlements_latest s
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON dacd.fbbid::VARCHAR = s.fbbid              -- FIX 1
       AND dacd.edate          = s.settlement_created_date
),

-- ── FIX 3: dpd_days is VARIANT — cast to INT explicitly ───────────────────
dpd_data AS (
    SELECT
        fbbid::VARCHAR                            AS fbbid,   -- FIX 1
        edate,
        MAX(COALESCE(dpd_days::INT, 0))           AS dpd_days, -- FIX 3
        MAX(is_charged_off)                       AS is_charged_off,
        SUM(outstanding_principal_due)            AS outstanding_principal,
        SUM(outstanding_balance)                  AS total_balance   -- Added: Total Balance (Principal + Interest + Fees)
    FROM bi.finance.finance_metrics_daily
    WHERE product_type <> 'Flexpay'
    GROUP BY fbbid::VARCHAR, edate
),

-- ── FIX 4: TRY_TO_DOUBLE + ::VARCHAR for safe JSON extraction ─────────────
settlement_payments AS (
    SELECT
        s.fbbid,
        s.settlement_created_date,
        SUM(
            CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date
                     AND COALESCE(s.settlement_end_time::DATE, CURRENT_DATE)
                -- FIX 4: TRY_TO_DOUBLE prevents crash on missing / malformed JSON key
                THEN TRY_TO_DOUBLE(p.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT::VARCHAR)
                ELSE 0
            END
        )                                         AS payment_amount,
        COUNT(
            CASE
                WHEN p.payment_status = 'FUND'
                 AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date
                     AND COALESCE(s.settlement_end_time::DATE, CURRENT_DATE)
                THEN 1
            END
        )                                         AS payment_count
    FROM settlements_latest s
    LEFT JOIN bi.finance.payments_model p
        ON p.fbbid::VARCHAR = s.fbbid            -- FIX 1
       AND p.parent_payment_id IS NOT NULL
       AND DATE(p.payment_event_time) BETWEEN s.settlement_created_date
           AND COALESCE(s.settlement_end_time::DATE, CURRENT_DATE)
    GROUP BY s.fbbid, s.settlement_created_date
),

-- Industry from NAICS
customer_industry AS (
    SELECT
        fbbid::VARCHAR AS fbbid,                 -- FIX 1
        industry_naics_code,
        CASE
            WHEN LEFT(industry_naics_code, 2) = '23'                         THEN 'Construction'
            WHEN LEFT(industry_naics_code, 2) IN ('42','44','45')
              OR LEFT(industry_naics_code, 1) = '4'                          THEN 'Retail & Wholesale Trade'
            WHEN LEFT(industry_naics_code, 2) IN ('48','49')                 THEN 'Transportation & Warehousing'
            WHEN LEFT(industry_naics_code, 2) = '56'                         THEN 'ASWR'
            WHEN LEFT(industry_naics_code, 2) IN ('53','72')                 THEN 'Real Estate, Rental & Hospitality'
            WHEN LEFT(industry_naics_code, 1) = '5'                          THEN 'Professional Services'
            ELSE 'Others'
        END AS industry_type
    FROM bi.public.customers_data
    WHERE fbbid IS NOT NULL
)

SELECT
    -- Identifiers
    s.fbbid,
    s.settlement_status,
    s.settlement_created_date,
    s.settlement_end_time,

    -- Time dimensions
    DATE_TRUNC('month',   s.settlement_created_date) AS settlement_month,
    DATE_TRUNC('quarter', s.settlement_created_date) AS settlement_quarter,
    YEAR(s.settlement_created_date)                  AS settlement_year,

    -- Lifecycle label
    CASE
        WHEN s.settlement_status = 'FUNDED'                  THEN 'Funded'
        WHEN s.settlement_status = 'ACTIVE'                  THEN 'Active'
        WHEN s.settlement_status IN ('FAILED','CANCELLED')   THEN 'Failed/Cancelled'
        ELSE 'Other'
    END AS lifecycle_stage,

    DATEDIFF('day', s.settlement_created_date,
             COALESCE(s.settlement_end_time::DATE, CURRENT_DATE)) AS days_in_settlement,

    -- Pre-CO vs Post-CO
    CASE
        WHEN co.charge_off_date IS NULL                          THEN 'Pre-CO'
        WHEN s.settlement_created_date < co.charge_off_date     THEN 'Pre-CO'
        ELSE 'Post-CO'
    END AS settlement_type,

    -- Charge-off context
    co.charge_off_date,
    co.co_principal,
    CASE
        WHEN co.charge_off_date IS NOT NULL
         AND s.settlement_created_date >= co.charge_off_date
        THEN FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30)
        ELSE NULL
    END AS mob_at_settlement,

    -- ── FIX 5: mob_bucket derived column ─────────────────────────────────
    CASE
        WHEN co.charge_off_date IS NULL
          OR s.settlement_created_date < co.charge_off_date     THEN 'N/A (Pre-CO)'
        WHEN FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30) = 0
                                                                 THEN 'MOB 0'
        WHEN FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30)
             BETWEEN 1 AND 3                                     THEN 'MOB 1-3'
        WHEN FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30)
             BETWEEN 4 AND 6                                     THEN 'MOB 4-6'
        WHEN FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30)
             BETWEEN 7 AND 12                                    THEN 'MOB 7-12'
        ELSE 'MOB 13+'
    END AS mob_bucket,

    -- ── FIX 3: dpd_at_settlement as INT ──────────────────────────────────
    dpd.dpd_days::INT AS dpd_at_settlement,               -- FIX 3
    CASE
        WHEN dpd.dpd_days::INT BETWEEN 1  AND 14           THEN 'DPD 1-14'
        WHEN dpd.dpd_days::INT BETWEEN 15 AND 56           THEN 'DPD 15-56'
        WHEN dpd.dpd_days::INT BETWEEN 57 AND 91           THEN 'DPD 57-91'
        WHEN dpd.is_charged_off = 1
          OR dpd.dpd_days::INT > 91                        THEN 'Charged Off'
        WHEN dpd.dpd_days::INT = 0
          OR dpd.dpd_days IS NULL                          THEN 'Current'
        ELSE 'Unknown'
    END AS dpd_bucket,

    -- Balance
    COALESCE(cs.outstanding_principal,
             dpd.outstanding_principal,
             co.co_principal)                              AS principal_due,
    COALESCE(dpd.total_balance,
             cs.outstanding_principal,
             dpd.outstanding_principal,
             co.co_principal)                              AS total_balance,
    COALESCE(cs.outstanding_principal,
             dpd.outstanding_principal,
             co.co_principal)                              AS balance_at_settlement,
    CASE
        WHEN COALESCE(cs.outstanding_principal, dpd.outstanding_principal, co.co_principal) < 5000
                                                           THEN 'Small (<$5K)'
        WHEN COALESCE(cs.outstanding_principal, dpd.outstanding_principal, co.co_principal) < 25000
                                                           THEN 'Medium ($5K-$25K)'
        WHEN COALESCE(cs.outstanding_principal, dpd.outstanding_principal, co.co_principal) < 100000
                                                           THEN 'Large ($25K-$100K)'
        ELSE 'Enterprise ($100K+)'
    END AS balance_tier,

    -- Placement
    COALESCE(cs.placement_type, 'Unknown')                AS placement_type,
    cs.recovery_suggested_state,

    -- Payments
    COALESCE(sp.payment_amount, 0)                        AS payment_amount,
    COALESCE(sp.payment_count,  0)                        AS payment_count,

    -- ── FIX 5: payment_structure derived column ───────────────────────────
    CASE
        WHEN COALESCE(sp.payment_count, 0) = 0            THEN 'No Payment'
        WHEN COALESCE(sp.payment_count, 0) = 1            THEN 'Lump Sum'
        WHEN COALESCE(sp.payment_count, 0) BETWEEN 2 AND 4 THEN 'Short Installment'
        ELSE 'Long Installment'
    END AS payment_structure,

    -- Recovery rate
    ROUND(
        COALESCE(sp.payment_amount, 0)
        / NULLIF(COALESCE(cs.outstanding_principal, dpd.outstanding_principal, co.co_principal), 0)
        * 100, 2
    ) AS recovery_rate_pct,

    -- Discount (haircut = balance forgiven)
    COALESCE(cs.outstanding_principal, dpd.outstanding_principal, co.co_principal)
    - COALESCE(sp.payment_amount, 0)                      AS discount_amount,

    -- Customer attributes (point-in-time at settlement)
    ci.industry_type                                      AS industry,
    dacd_pit.state                                        AS geography,
    f_pit.channel,
    f_pit.partner

FROM settlements_latest s

LEFT JOIN charge_off co
    ON co.fbbid = s.fbbid

LEFT JOIN customer_state cs
    ON cs.fbbid = s.fbbid
   AND cs.settlement_created_date = s.settlement_created_date

-- FIX 3: dpd join uses VARCHAR-cast fbbid from dpd_data CTE
LEFT JOIN dpd_data dpd
    ON dpd.fbbid = s.fbbid
   AND dpd.edate = s.settlement_created_date

LEFT JOIN settlement_payments sp
    ON sp.fbbid = s.fbbid
   AND sp.settlement_created_date = s.settlement_created_date

LEFT JOIN customer_industry ci
    ON ci.fbbid = s.fbbid

-- Point-in-time: state at settlement creation
LEFT JOIN bi.public.daily_approved_customers_data dacd_pit
    ON dacd_pit.fbbid::VARCHAR = s.fbbid           -- FIX 1
   AND dacd_pit.edate = s.settlement_created_date

-- Point-in-time: channel/partner at settlement creation
LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f_pit
    ON f_pit.fbbid::VARCHAR = s.fbbid             -- FIX 1
   AND f_pit.edate = s.settlement_created_date;


-- =====================================================
-- TABLE 2: SETTLEMENT fbbid RISK ENRICHMENT  (FIXED)
-- =====================================================
-- Joins Collections SMM scorecard to the settlement base.
--
--  FIX 2 applied:
--    • Column is IDENTIFIER_ID (not "fbbid")
--    • identifier_type = 'FUNDBOX_BUSINESS' (confirmed live)
--    • Join: s.fbbid = rs.identifier_id  (both VARCHAR after FIX 1)
--    • Latest score per merchant via QUALIFY ROW_NUMBER() on created_time DESC

CREATE OR REPLACE TABLE analytics.credit.settlement_fbbid_risk AS

WITH risk_scores AS (
    SELECT
        -- FIX 2: correct column name and identifier_type filter
        identifier_id                                    AS fbbid,
        score                                            AS collections_raw_score,
        score * 100                                      AS collections_score_x100,
        CASE
            WHEN score * 100 >  0  AND score * 100 <= 30 THEN 'A'
            WHEN score * 100 > 30  AND score * 100 <= 40 THEN 'B'
            WHEN score * 100 > 40  AND score * 100 <= 55 THEN 'C'
            WHEN score * 100 > 55  AND score * 100 <= 70 THEN 'D'
            WHEN score * 100 > 70  AND score * 100 <= 85 THEN 'E'
            WHEN score * 100 > 85                        THEN 'F'
            ELSE 'Unscored'
        END AS collections_risk_grade,
        created_time
    FROM CDC_V2.SCORING.scoring_model_scores
    WHERE model_name      = 'smm-collections-SV-v2-scorecard-production'
      AND identifier_type = 'FUNDBOX_BUSINESS'          -- FIX 2: confirmed live value
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY identifier_id
        ORDER BY created_time DESC                       -- latest score per merchant
    ) = 1
)

SELECT
    -- All base columns
    s.fbbid,
    s.settlement_status,
    s.settlement_created_date,
    s.settlement_end_time,
    s.settlement_month,
    s.settlement_quarter,
    s.settlement_year,
    s.lifecycle_stage,
    s.days_in_settlement,
    s.settlement_type,
    s.charge_off_date,
    s.co_principal,
    s.mob_at_settlement,
    s.mob_bucket,                                        -- FIX 5
    s.dpd_at_settlement,                                 -- FIX 3: now INT
    s.dpd_bucket,
    s.principal_due,                                     -- Added: Principal Due
    s.total_balance,                                     -- Added: Total Balance
    s.balance_at_settlement,
    s.balance_tier,
    s.placement_type,
    s.recovery_suggested_state,
    s.payment_amount,
    s.payment_count,
    s.payment_structure,                                 -- FIX 5
    s.recovery_rate_pct,
    s.discount_amount,
    s.industry,
    s.geography,
    s.channel,
    s.partner,

    -- Risk grade enrichment (FIX 2)
    rs.collections_raw_score,
    rs.collections_score_x100,
    COALESCE(rs.collections_risk_grade, 'Unscored')     AS collections_risk_grade

FROM analytics.credit.settlement_monthly_base s
LEFT JOIN risk_scores rs
    ON rs.fbbid = s.fbbid;                               -- FIX 2: both VARCHAR, clean join



Select * from analytics.credit.settlement_fbbid_risk
