-- ============================================================
-- BREATHERS PERFORMANCE DATASET
-- Scope: 2022-01-01 onwards | Non-Flexpay only
-- Outputs: All breather events with DPD context + W13/W26 outcomes + fees collected
--          (breather_fee from payments_model, attributed to inter-breather date windows)
--          + account context (partner, industry, revenue/FICO buckets, sub-product at T-1)
--          + total_breathers_per_account (count of breathers in this file's population per FBBID)
-- Use: segment "who gets breathers" via account_* columns; performance via dpd_* / is_co_*;
--       GROUP BY FBBID or account dimensions for rollups.
-- ============================================================

WITH

-- ── STEP 1a: Core breather events (inner) ────────────────────
breather_events_core AS (
    SELECT
        b.FBBID,
        b.EVENT_ID                                                              AS breather_id,
        b.FROM_DATE                                                             AS breather_start,
        b.TO_DATE                                                               AS breather_end,
        b.EVENT_JSON:breather_days::INT                                         AS duration_days,
        b.EVENT_JSON:breather_reason::STRING                                    AS reason,

        -- Fee in DOLLARS (source is cents)
        ROUND(b.EVENT_JSON:breather_fees_in_cents::FLOAT / 1000, 2)             AS fee_invoiced_usd,
        ROUND(b.EVENT_JSON:breather_principal_amount::FLOAT, 2)                AS principal_paused,

        -- Breather sequence per account
        ROW_NUMBER() OVER (PARTITION BY b.FBBID ORDER BY b.FROM_DATE)          AS breather_num,

        -- Days between previous breather end and this start
        -- Filter days_gap >= 0 downstream for inter-breather analysis
        DATEDIFF('day',
            LAG(b.TO_DATE) OVER (PARTITION BY b.FBBID ORDER BY b.FROM_DATE),
            b.FROM_DATE)                                                        AS days_since_last_breather,

        -- Total breathers (same scope as this query) per customer
        COUNT(*) OVER (PARTITION BY b.FBBID)                                    AS total_breathers_per_account

    FROM BI.FINANCE.DIM_BREATHER_EVENT b
    WHERE b.FROM_DATE IS NOT NULL          -- hard rule: always filter nulls
      AND b.FROM_DATE >= '2022-01-01'      -- pre-2022 has bulk re-issue artefacts
),

-- ── STEP 1b: Adjacent breather boundaries + fee attribution window ──
-- Breather-fee payments (payments_model) are mapped to the breather row whose window
-- contains the payment date: from breather_start (first on account) or day after
-- prior breather_end, through day before next breather_start, else CURRENT_DATE().
breather_events AS (
    SELECT
        w.*,
        CASE
            WHEN w.prev_breather_end IS NULL THEN w.breather_start
            ELSE DATEADD('day', 1, w.prev_breather_end)
        END                                                                     AS fee_collected_window_start,
        GREATEST(
            CASE
                WHEN w.prev_breather_end IS NULL THEN w.breather_start
                ELSE DATEADD('day', 1, w.prev_breather_end)
            END,
            COALESCE(DATEADD('day', -1, w.next_breather_start), CURRENT_DATE())
        )                                                                         AS fee_collected_window_end
    FROM (
        SELECT
            c.*,
            LAG(c.breather_end) OVER (PARTITION BY c.FBBID ORDER BY c.breather_start)   AS prev_breather_end,
            LEAD(c.breather_start) OVER (PARTITION BY c.FBBID ORDER BY c.breather_start) AS next_breather_start
        FROM breather_events_core c
    ) w
),

-- ── STEP 2: FINANCE_METRICS_DAILY deduplicated by FBBID + EDATE ──
-- Required because FMD can have multiple rows per day per account
fmd_daily AS (
    SELECT
        d.FBBID,
        d.EDATE,
        MAX(d.DPD_DAYS)                     AS dpd_days,
        MAX(d.IS_CHARGED_OFF)               AS is_charged_off,
        SUM(d.OUTSTANDING_PRINCIPAL_DUE)    AS outstanding_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY d
    WHERE d.PRODUCT_TYPE <> 'Flexpay'
    GROUP BY d.FBBID, d.EDATE
),

-- ── STEP 3: Breather fee cash collected (payments_model, inter-breather windows) ──
-- Sum PAYMENT_COMPONENTS_JSON:BREATHER_FEE for FUND debits whose payment date falls in
-- [fee_collected_window_start, fee_collected_window_end] for that breather row.
pm_fund_deduped AS (
    SELECT
        p.FBBID,
        DATE(p.PAYMENT_EVENT_TIME)                                            AS payment_event_date,
        COALESCE(
            TRY_TO_DOUBLE(TO_VARCHAR(p.PAYMENT_COMPONENTS_JSON:BREATHER_FEE)),
            TRY_TO_DOUBLE(TO_VARCHAR(p.PAYMENT_COMPONENTS_JSON:breather_fee)),
            0
        )                                                                     AS breather_fee_amount
    FROM BI.FINANCE.PAYMENTS_MODEL p
    WHERE p.DIRECTION = 'D'
      AND p.PAYMENT_STATUS = 'FUND'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.PAYMENT_ID ORDER BY p.PAYMENT_EVENT_TIME ASC) = 1
),

fees_collected AS (
    SELECT
        b.FBBID,
        b.breather_id,
        SUM(pm.breather_fee_amount)                                           AS fee_collected_usd
    FROM breather_events b
    INNER JOIN pm_fund_deduped pm
        ON  pm.FBBID = b.FBBID
        AND pm.payment_event_date BETWEEN b.fee_collected_window_start AND b.fee_collected_window_end
    WHERE pm.breather_fee_amount <> 0
    GROUP BY b.FBBID, b.breather_id
),

-- ── STEP 4: Daily approved snapshot day before breather (dedupe if multiple rows) ──
dacd_at_breather_start AS (
    SELECT
        e.FBBID,
        e.breather_id,
        dacd.SUB_PRODUCT                                                       AS sub_product_at_start,
        dacd.STATE                                                             AS state_at_start,
        ROW_NUMBER() OVER (
            PARTITION BY e.FBBID, e.breather_id
            ORDER BY COALESCE(dacd.CREDIT_LIMIT, 0) DESC NULLS LAST
        )                                                                      AS rn_dacd
    FROM breather_events e
    LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd
        ON  e.FBBID = dacd.FBBID
        AND dacd.EDATE = DATEADD('day', -1, e.breather_start)
),

dacd_at_breather_start_1 AS (
    SELECT FBBID, breather_id, sub_product_at_start, state_at_start
    FROM dacd_at_breather_start
    WHERE rn_dacd = 1
)

-- ── FINAL: Join everything together ──────────────────────────
SELECT

    -- Identity
    e.FBBID,
    e.breather_id,

    -- Account context (who gets breathers — segment on these in downstream BI)
    cd.PARTNER_ATTRIBUTION                                                   AS partner_attribution,
    CASE
        WHEN LOWER(cd.PARTNER_ATTRIBUTION) IN ('intuit') THEN 'Intuit'
        WHEN LOWER(cd.PARTNER_ATTRIBUTION) IN ('non-partner')
             OR cd.PARTNER_ATTRIBUTION IS NULL THEN 'Direct'
        ELSE 'Partner / Other'
    END                                                                      AS partner_group_coarse,
    cd.PREQUAL_ANNUAL_REVENUE_RANGE                                          AS prequal_annual_revenue_range,
    CASE
        WHEN cd.PREQUAL_ANNUAL_REVENUE_RANGE IN (NULL, '0K-100K')              THEN '<100K'
        WHEN cd.PREQUAL_ANNUAL_REVENUE_RANGE IN ('100K-200K', '200K-300K', '300K-600K')
                                                                                THEN '100K-600K'
        WHEN cd.PREQUAL_ANNUAL_REVENUE_RANGE IN ('600K-1M', '>1M')            THEN '>600K'
        ELSE 'Unknown / Other'
    END                                                                      AS revenue_bucket_coarse,
    cd.FICO_ONBOARDING                                                       AS fico_onboarding,
    CASE
        WHEN cd.FICO_ONBOARDING BETWEEN 300 AND 500                            THEN '300-500'
        WHEN cd.FICO_ONBOARDING BETWEEN 501 AND 600                            THEN '501-600'
        WHEN cd.FICO_ONBOARDING BETWEEN 601 AND 700                            THEN '601-700'
        WHEN cd.FICO_ONBOARDING > 700                                          THEN '700+'
        ELSE 'Unknown / Missing'
    END                                                                      AS fico_bucket_coarse,
    cd.INDUSTRY_NAICS_CODE                                                   AS industry_naics_code,
    CASE
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 2) = '23'                    THEN 'Construction'
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 2) IN ('42', '44', '45')
             OR LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 1) = '4'                  THEN 'Retail & Wholesale'
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 2) IN ('48', '49')           THEN 'Transportation & Warehousing'
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 2) = '56'                    THEN 'Administrative & Support'
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 2) IN ('53', '72')          THEN 'Real Estate & Hospitality'
        WHEN LEFT(cd.INDUSTRY_NAICS_CODE::STRING, 1) = '5'                     THEN 'Professional Services'
        WHEN cd.INDUSTRY_NAICS_CODE IS NOT NULL                                THEN 'Other / Unknown NAICS'
        ELSE 'Missing'
    END                                                                      AS industry_bucket_coarse,
    dac1.sub_product_at_start,
    dac1.state_at_start,

    e.total_breathers_per_account,

    -- Event details
    e.breather_start,
    e.breather_end,
    e.duration_days,
    e.reason,
    e.fee_invoiced_usd,
    e.principal_paused,
    e.breather_num,
    e.days_since_last_breather,

    e.prev_breather_end,
    e.next_breather_start,
    e.fee_collected_window_start,
    e.fee_collected_window_end,

    -- ── DPD day before breather started ──────────────────────
    COALESCE(db.dpd_days, 0)                AS dpd_day_before_start,
    db.outstanding_principal                AS outstanding_principal_before,
    CASE
        WHEN COALESCE(db.dpd_days, 0) = 0  THEN 'DPD 0'
        WHEN db.dpd_days BETWEEN 1  AND 14  THEN 'DPD 1-2'
        WHEN db.dpd_days BETWEEN 15 AND 56  THEN 'DPD 3-8'
        WHEN db.dpd_days BETWEEN 57 AND 91  THEN 'DPD 9-13'
        WHEN db.dpd_days > 91               THEN 'DPD 14+'
        ELSE 'Unknown'
    END                                     AS dpd_bucket_before_start,

    -- ── Performance at Week 13 (91 days post breather_end) ───
    w13.dpd_days                            AS dpd_days_w13,
    w13.is_charged_off                      AS is_co_w13,
    CASE
        WHEN w13.is_charged_off = 1                 THEN 'Charged Off'
        WHEN COALESCE(w13.dpd_days, 0) = 0         THEN 'DPD 0'
        WHEN w13.dpd_days BETWEEN 1  AND 14         THEN 'DPD 1-2'
        WHEN w13.dpd_days BETWEEN 15 AND 56         THEN 'DPD 3-8'
        WHEN w13.dpd_days BETWEEN 57 AND 91         THEN 'DPD 9-13'
        WHEN w13.dpd_days > 91                      THEN 'DPD 14+'
        ELSE 'No data'
    END                                     AS dpd_bucket_w13,

    -- ── Performance at Week 26 (182 days post breather_end) ──
    w26.dpd_days                            AS dpd_days_w26,
    w26.is_charged_off                      AS is_co_w26,
    CASE
        WHEN w26.is_charged_off = 1                 THEN 'Charged Off'
        WHEN COALESCE(w26.dpd_days, 0) = 0         THEN 'DPD 0'
        WHEN w26.dpd_days BETWEEN 1  AND 14         THEN 'DPD 1-2'
        WHEN w26.dpd_days BETWEEN 15 AND 56         THEN 'DPD 3-8'
        WHEN w26.dpd_days BETWEEN 57 AND 91         THEN 'DPD 9-13'
        WHEN w26.dpd_days > 91                      THEN 'DPD 14+'
        ELSE 'No data'
    END                                     AS dpd_bucket_w26,

    -- ── Fee cash collected ────────────────────────────────────
    COALESCE(fc.fee_collected_usd, 0)       AS fee_collected_usd,
    CASE
        WHEN e.fee_invoiced_usd > 0
        THEN ROUND(COALESCE(fc.fee_collected_usd, 0) / e.fee_invoiced_usd, 4)
        ELSE NULL
    END                                     AS fee_collection_rate

FROM breather_events e

LEFT JOIN BI.PUBLIC.CUSTOMERS_DATA cd
    ON e.FBBID = cd.FBBID

LEFT JOIN dacd_at_breather_start_1 dac1
    ON  e.FBBID       = dac1.FBBID
    AND e.breather_id = dac1.breather_id

-- DPD the day BEFORE breather started (T-1)
LEFT JOIN fmd_daily db
    ON  e.FBBID  = db.FBBID
    AND db.EDATE = DATEADD('day', -1, e.breather_start)

-- DPD at Week 13 (91 days after breather ended)
LEFT JOIN fmd_daily w13
    ON  e.FBBID   = w13.FBBID
    AND w13.EDATE = DATEADD('day', 91, e.breather_end)

-- DPD at Week 26 (182 days after breather ended)
LEFT JOIN fmd_daily w26
    ON  e.FBBID   = w26.FBBID
    AND w26.EDATE = DATEADD('day', 182, e.breather_end)

-- Breather fees from payments_model, summed over [window after prev breather, day before next]
LEFT JOIN fees_collected fc
    ON  fc.FBBID       = e.FBBID
    AND fc.breather_id = e.breather_id

ORDER BY e.FBBID, e.breather_start
;