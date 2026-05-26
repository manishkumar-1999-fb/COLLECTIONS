/*
=============================================================================
  ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
  
  Purpose : Single materialized table powering the MUW Risk Factors
            Team Lead Dashboard (Modules A, B, C, D).

  Grain   : One row per MUW review record. No daily fan-out.
            Dashboard aggregates at review_month level.

  Sources : All from source tables — no dependency on downstream views.
    EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C       reviews, risk factors, UW IDs
    EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER          UW names + TL hierarchy
    BI.PUBLIC.CUSTOMERS_DATA                            first approved CL, revenue
    BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA             Vantage at review, pre/post CL, NAICS
    BI.FINANCE.FINANCE_METRICS_DAILY                    DPD outcome (UDR35)
    ANALYTICS.CREDIT.SECOND_LOOK_ACCOUNTS               auto grade vs UW grade (Module D)
    BI.CUSTOMERS.LEADS_DATA                             Pre-Approval CL and revenue
    INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2           Vantage, OG bucket, partner
    CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING industry label

  Key design notes:
    - All reviews kept (not just latest). rnk_latest=1 flags most recent per fbbid.
      Module A uses rnk_latest=1 for snapshots; Modules B/C use all rows for trends.
    - UW attribution: APPROVER__C primary, LASTMODIFIEDBYID fallback.
    - Integration Master bot (0054T000001PvTrQAK) excluded from both.
    - TL_NAME: manager's manager; if manager has no manager in SF, manager = TL.
      If UW is a team lead / manager (has reports in SF or static list), TL = UW.
    - PARTNER sourced from INDUS only (not in DAILY_APPROVED_CUSTOMERS_DATA).
    - Vantage: DACD ONBOARDING_FETCHED_CREDIT_SCORE_JSON at review date (4.0, else 3.0),
      then INDUS, then LOAN__C.vantage_score_last_90_days__c.
    - T4W = last 4 complete Wed-to-Wed weeks (excludes current in-flight week).
    - UW_TYPE = dashboard filter (Onboarding / Ongoing / HVC / AUW Monitoring / Pre-Approval).
    - PROGRAM_GROUP = Reactive vs Proactive (separate from UW_TYPE).
    - UDR35 = first DPD>=35 date that falls on or after review_complete_date.
    - SECOND_LOOK_ACCOUNTS joined on fbbid — fallback for SL-only fields only.
    - IS_MATURE_6MO = review >= 6 months ago (DQ charts — avoids right-censoring).

  Known data coverage constraints:
    - rf_residual: only populated from Feb 2026 (~76% fill from Mar 2026).
    - rf_quality_of_revenue: reliable from Sep 2024 onwards.
    - 12-month lookbacks in Module B will be sparse for these two factors.

  Refresh cadence:
    Daily, after FINANCE_METRICS_DAILY and INDUS refreshes complete.
    Then run TL_Dashboard_Weekly_Aggregates.sql for WEEKLY_TREND / WEEKLY_DQ.
    Scheduled task template at bottom of file.
=============================================================================
*/

CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD AS

WITH

/* -------------------------------------------------------------------------
   STEP 1 — UW & TL HIERARCHY
   Salesforce manager chain: UW → manager → TL (manager's manager).
   When manager has no managerid (manager is TL in org), TL = manager.
   Integration Master bot excluded.
------------------------------------------------------------------------- */
uw_hierarchy AS (
    SELECT
        u.id                AS uw_id,
        u.name              AS uw_name,
        u.isactive          AS uw_is_active,
        m.id                AS manager_id,
        m.name              AS manager_name,
        COALESCE(mm.id, m.id)   AS tl_id,
        COALESCE(mm.name, m.name) AS tl_name
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER u
    LEFT JOIN EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER m
        ON u.managerid = m.id
    LEFT JOIN EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER mm
        ON m.managerid = mm.id
    WHERE u.id != '0054T000001PvTrQAK'
      AND (m.id IS NULL OR m.id != '0054T000001PvTrQAK')
),

/* Team leads / managers with no TL above them in SF (self-TL when UW is a leader) */
team_leaders AS (
    SELECT user_id, user_name
    FROM (
        SELECT m.id AS user_id, m.name AS user_name
        FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER m
        WHERE m.id != '0054T000001PvTrQAK'
          AND EXISTS (
              SELECT 1
              FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER r
              WHERE r.managerid = m.id
                AND r.id != '0054T000001PvTrQAK'
          )
        UNION
        SELECT column1 AS user_id, column2 AS user_name
        FROM VALUES
            ('0054T0000010694QAA', 'Neil Patel'),
            ('0054T0000010699QAA', 'Christopher Dykes'),
            ('005Rd000003Lu2HIAS', 'Greg Maitles'),
            ('005Rd000005K1LFIA0', 'Dmitry Altshuler'),
            ('005Rd000004bZSjIAM', 'Jerry Christian')
    )
),

/* -------------------------------------------------------------------------
   STEP 2 — BASE MUW REVIEWS from LOAN__C
   One row per review. All 6 programs included.
------------------------------------------------------------------------- */
loan_base AS (
    SELECT
        fundbox_id__c                                                               AS fbbid,
        id                                                                          AS loan_id,
        recordtypeid,
        CASE recordtypeid
            WHEN '012Rd000000AcjxIAC' THEN 'OB AUW'
            WHEN '012Rd000000jbbJIAQ' THEN 'Pre-Approval'
            WHEN '0124T000000DSMTQA4' THEN 'Second Look'
            WHEN '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
            WHEN '012Rd000001B2txIAC' THEN 'OG AUW'
            WHEN '012Rd000002EppVIAS' THEN 'HVC'
            ELSE 'Other'
        END                                                                         AS program_name,
        CASE recordtypeid
            WHEN '012Rd000000AcjxIAC' THEN 'Reactive'
            WHEN '0124T000000DSMTQA4' THEN 'Reactive'
            WHEN '012Rd000000jbbJIAQ' THEN 'Reactive'
            WHEN '012Rd000001B2txIAC' THEN 'Proactive'
            WHEN '012Rd000002EppVIAS' THEN 'Proactive'
            WHEN '012Rd000002Dp5CIAS' THEN 'Proactive'
            ELSE 'Other'
        END                                                                         AS program_group,

        -- Review date (coalesce pre-doc date for OB AUW)
        COALESCE(
            review_complete_time__c::DATE,
            auw_pre_doc_review_complete_time__c::DATE
        )                                                                           AS review_complete_date,
        createddate::DATE                                                           AS created_date,

        -- Decision fields
        status__c,
        risk_level__c,
        approved_uw_credit_limit__c,
        auw_pre_doc_approved_limit__c,
        auw_post_doc_approved_limit__c,
        auw_pre_doc_review_status__c,
        auw_post_doc_review_status__c,
        auw_pre_doc_review_start_time__c,
        first_suggested_credit_limit__c,

        -- UW attribution: APPROVER__C primary, LASTMODIFIEDBYID fallback
        -- Integration Master bot excluded from both
        CASE
            WHEN approver__c IS NOT NULL
             AND approver__c != ''
             AND approver__c != '0054T000001PvTrQAK'
                THEN approver__c
            WHEN lastmodifiedbyid IS NOT NULL
             AND lastmodifiedbyid != '0054T000001PvTrQAK'
                THEN lastmodifiedbyid
            ELSE NULL
        END                                                                         AS uw_id_resolved,
        underwriter__c                                                              AS underwriter_id,
        approver__c                                                                 AS approver_id,
        lastmodifiedbyid,

        -- Second Look grades on the review record (preferred for Module D / lookback)
        NULLIF(automated_risk_grade__c, '')                                         AS sf_automated_risk_grade,
        underwriting_risk_grade__c                                                  AS sf_uw_risk_grade,
        TRY_TO_DOUBLE(vantage_score_last_90_days__c)                               AS vantage_at_review_sf,

        -- Dashboard UW Type filter (spec — distinct from Reactive/Proactive program_group)
        CASE recordtypeid
            WHEN '012Rd000000AcjxIAC' THEN 'Onboarding'
            WHEN '0124T000000DSMTQA4' THEN 'Onboarding'
            WHEN '012Rd000001B2txIAC' THEN 'Ongoing'
            WHEN '012Rd000002EppVIAS' THEN 'HVC'
            WHEN '012Rd000002Dp5CIAS' THEN 'AUW Monitoring'
            WHEN '012Rd000000jbbJIAQ' THEN 'Pre-Approval'
            ELSE 'Other'
        END                                                                         AS uw_type,

        -- 6 Risk factor ratings (raw text)
        RISK_FACTOR_BANK_BALANCE__C                                                 AS rf_bank_balance,
        RISK_FACTOR_QUALITY_OF_REVENUE__C                                           AS rf_quality_of_revenue,
        RISK_FACTOR_PERSONAL_CREDIT__C                                              AS rf_personal_credit,
        RISK_FACTOR_INDUSTRY__C                                                     AS rf_industry,
        RISK_FACTOR_EXISTING_DEBT_SERVICE__C                                        AS rf_debt_service,
        RISK_FACTOR_RESIDUAL__C                                                     AS rf_residual,

        -- Numeric encoding for avg rating trend (Module B.1)
        CASE RISK_FACTOR_BANK_BALANCE__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
        END                                                                         AS rf_bank_balance_num,
        CASE RISK_FACTOR_QUALITY_OF_REVENUE__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
        END                                                                         AS rf_quality_of_revenue_num,
        CASE RISK_FACTOR_PERSONAL_CREDIT__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
        END                                                                         AS rf_personal_credit_num,
        CASE RISK_FACTOR_INDUSTRY__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
            -- N/A maps to NULL intentionally
        END                                                                         AS rf_industry_num,
        CASE RISK_FACTOR_EXISTING_DEBT_SERVICE__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
        END                                                                         AS rf_debt_service_num,
        CASE RISK_FACTOR_RESIDUAL__C
            WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 ELSE NULL
        END                                                                         AS rf_residual_num,

        -- Latest review flag per customer (1 = most recent)
        ROW_NUMBER() OVER (
            PARTITION BY fundbox_id__c
            ORDER BY COALESCE(review_complete_time__c, auw_pre_doc_review_complete_time__c) DESC
        )                                                                           AS rnk_latest

    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.LOAN__C
    WHERE recordtypeid IN (
        '012Rd000000AcjxIAC',
        '012Rd000000jbbJIAQ',
        '0124T000000DSMTQA4',
        '012Rd000002Dp5CIAS',
        '012Rd000001B2txIAC',
        '012Rd000002EppVIAS'
    )
    AND (
        review_complete_time__c IS NOT NULL
        OR auw_pre_doc_review_complete_time__c IS NOT NULL
    )
    AND fundbox_id__c NOT IN (999999999999999910, 9987800100888, 4478137, 3923148, 2464129)
    AND fundbox_id__c IS NOT NULL
),

/* -------------------------------------------------------------------------
   STEP 3 — CUSTOMER MASTER DATA
------------------------------------------------------------------------- */
customer_master AS (
    SELECT
        fbbid,
        first_approved_time::DATE                                                   AS first_approved_date,
        first_approved_credit_limit,
        first_draw_time::DATE                                                       AS first_draw_date,
        first_draw_amount,
        COALESCE(
            first_account_size_accounting_software,
            first_account_size_fi,
            0
        ) * 12                                                                      AS customer_annual_revenue_raw
    FROM BI.PUBLIC.CUSTOMERS_DATA
    WHERE fbbid IS NOT NULL
),

/* -------------------------------------------------------------------------
   STEP 4 — PRE-APPROVAL DATA from leads_data
------------------------------------------------------------------------- */
leads AS (
    SELECT
        fbbid,
        partner_name                AS pa_partner,
        calculated_annual_revenue   AS pa_annual_revenue,
        pre_approval_amount         AS pa_automated_cl,
        auw_approved_limit          AS pa_approved_cl
    FROM BI.CUSTOMERS.LEADS_DATA
    WHERE fbbid IS NOT NULL
),

/* -------------------------------------------------------------------------
   STEP 5 — VANTAGE at review date from DACD (4.0 preferred, else 3.0)
------------------------------------------------------------------------- */
dacd_vantage_at_review AS (
    SELECT
        fbbid,
        edate,
        vantage_4,
        vantage_3,
        COALESCE(vantage_4, vantage_3)                              AS vantage_score
    FROM (
        SELECT
            fbbid,
            edate,
            PARSE_JSON(PARSE_JSON(ONBOARDING_FETCHED_CREDIT_SCORE_JSON::VARCHAR):"VantageScore 4.0":"score")::INT
                                                                                AS vantage_4,
            PARSE_JSON(PARSE_JSON(ONBOARDING_FETCHED_CREDIT_SCORE_JSON::VARCHAR):"VantageScore 3.0":"score")::INT
                                                                                AS vantage_3
        FROM BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA
    )
),

/* -------------------------------------------------------------------------
   STEP 6 — OG BUCKET + PARTNER from INDUS at review date
   NOTE: PARTNER is only available in INDUS, not in DAILY_APPROVED_CUSTOMERS_DATA.
------------------------------------------------------------------------- */
indus_data AS (
    SELECT
        fbbid,
        edate,
        vantage,
        og_bucket_group,
        CASE
            WHEN og_bucket_group ILIKE '%1-4%'
              OR og_bucket_group IN ('OG1','OG2','OG3','OG4')   THEN 'OG 1-4'
            WHEN og_bucket_group ILIKE '%5-7%'
              OR og_bucket_group IN ('OG5','OG6','OG7')         THEN 'OG 5-7'
            WHEN og_bucket_group ILIKE '%8-10%'
              OR og_bucket_group IN ('OG8','OG9','OG10')        THEN 'OG 8-10'
            WHEN og_bucket_group ILIKE '%11-12%'
              OR og_bucket_group IN ('OG11','OG12')             THEN 'OG 11-12'
            WHEN og_bucket_group ILIKE '%13+%'                    THEN 'OG 13+'
            WHEN og_bucket_group ILIKE '%No Bucket%'
              OR og_bucket_group IS NULL                        THEN 'No Bucket'
            ELSE og_bucket_group
        END                         AS og_bucket_group_banded,
        ob_bucket_group_retro,
        customer_annual_revenue_group,
        partner
    FROM INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_V2
),

/* -------------------------------------------------------------------------
   STEP 7 — DPD OUTCOME (UDR35)
   First date each customer hit DPD >= 35.
   Dashboard joins this to review_complete_date to check if DQ occurred
   after the review (post-review outcome).
------------------------------------------------------------------------- */
first_dpd35 AS (
    SELECT
        fbbid,
        MIN(edate) AS first_udr35_date
    FROM (
        SELECT
            fbbid,
            edate,
            CASE
                WHEN dpd_days IS NULL AND is_charged_off::INT = 0 THEN 0
                WHEN dpd_days IS NULL AND is_charged_off::INT = 1 THEN 98
                ELSE dpd_days::INT
            END AS dpd_corrected
        FROM BI.FINANCE.FINANCE_METRICS_DAILY
        WHERE product_type          != 'Flexpay'
          AND loan_operational_status != 'CNCL'
    )
    WHERE dpd_corrected >= 35
    GROUP BY 1
),

/* -------------------------------------------------------------------------
   STEP 8 — SECOND LOOK ACCOUNTS (Module D)
   Deduped to one row per fbbid (most recent review).
------------------------------------------------------------------------- */
second_look_deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY fbbid
                ORDER BY first_risk_review_time DESC
            ) AS sl_rnk
        FROM ANALYTICS.CREDIT.SECOND_LOOK_ACCOUNTS
    )
    WHERE sl_rnk = 1
),

/* -------------------------------------------------------------------------
   STEP 9 — NAICS INDUSTRY LABELS
------------------------------------------------------------------------- */
naics AS (
    SELECT naics_code, naics_title
    FROM CDC_V2.AUDIT_LOG_HIST.NAICS_CODES_TO_TITLES_MAPPING
),

/* Wed-to-Wed week anchor (1900-01-03 = Wednesday) — matches dashboard spec T4W */
period_anchor AS (
    SELECT
        DATEADD(
            'day',
            -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), CURRENT_DATE), 7),
            CURRENT_DATE
        )                                                       AS current_wed_week_start
),

/* -------------------------------------------------------------------------
   STEP 10 — FULL ASSEMBLY
------------------------------------------------------------------------- */
assembled AS (
    SELECT
        -- Identity
        lb.fbbid,
        lb.loan_id,
        lb.recordtypeid,
        lb.program_name,
        lb.program_group,
        lb.uw_type,
        lb.review_complete_date,
        DATE_TRUNC('month', lb.review_complete_date)    AS review_month,
        DATEADD(
            'day',
            -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), lb.review_complete_date), 7),
            lb.review_complete_date
        )                                               AS review_week_start,
        TO_CHAR(
            DATEADD(
                'day',
                -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), lb.review_complete_date), 7),
                lb.review_complete_date
            ),
            'YYYY-MM-DD'
        )                                               AS review_week_label,
        lb.created_date,
        lb.rnk_latest,

        -- Period flags (dashboard IN_T4W / IN_3MO / IN_6MO / IN_12MO)
        CASE
            WHEN DATEADD(
                    'day',
                    -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), lb.review_complete_date), 7),
                    lb.review_complete_date
                 ) >= DATEADD('week', -4, pa.current_wed_week_start)
             AND DATEADD(
                    'day',
                    -MOD(DATEDIFF('day', TO_DATE('1900-01-03'), lb.review_complete_date), 7),
                    lb.review_complete_date
                 ) <  pa.current_wed_week_start
            THEN 1 ELSE 0
        END                                             AS is_in_t4w,
        CASE
            WHEN lb.review_complete_date >= DATEADD('month', -3, CURRENT_DATE)
            THEN 1 ELSE 0
        END                                             AS is_in_3mo,
        CASE
            WHEN lb.review_complete_date >= DATEADD('month', -6, CURRENT_DATE)
            THEN 1 ELSE 0
        END                                             AS is_in_6mo,
        CASE
            WHEN lb.review_complete_date >= DATEADD('month', -12, CURRENT_DATE)
            THEN 1 ELSE 0
        END                                             AS is_in_12mo,
        CASE
            WHEN lb.review_complete_date <= DATEADD('month', -6, CURRENT_DATE)
            THEN 1 ELSE 0
        END                                             AS is_mature_6mo,

        -- Module B denominators / filters
        CASE
            WHEN lb.status__c ILIKE 'Complete%' THEN 1 ELSE 0
        END                                             AS is_complete,
        CASE
            WHEN cm.first_draw_amount IS NOT NULL THEN 1 ELSE 0
        END                                             AS is_ftd,

        -- UW Attribution
        lb.uw_id_resolved,
        lb.underwriter_id,
        lb.approver_id,
        lb.lastmodifiedbyid,
        uh.uw_name,
        uh.uw_is_active,
        uh.manager_id,
        uh.manager_name,
        COALESCE(
            uh.tl_id,
            CASE
                WHEN tl_uw.user_id IS NOT NULL OR tl_und.user_id IS NOT NULL
                    THEN COALESCE(lb.uw_id_resolved, lb.underwriter_id, tl_uw.user_id, tl_und.user_id)
            END
        )                                               AS tl_id,
        COALESCE(
            uh.tl_name,
            CASE
                WHEN tl_uw.user_id IS NOT NULL
                    THEN COALESCE(uh.uw_name, tl_uw.user_name)
                WHEN tl_und.user_id IS NOT NULL
                    THEN COALESCE(uw_und.name, tl_und.user_name)
            END
        )                                               AS tl_name,

        -- Decision
        lb.status__c,
        lb.risk_level__c,

        -- Automated CL (pre-decision)
        CASE lb.recordtypeid
            WHEN '012Rd000000AcjxIAC' THEN cm.first_approved_credit_limit
            WHEN '0124T000000DSMTQA4' THEN sl.automated_cl
            WHEN '012Rd000000jbbJIAQ' THEN ld.pa_automated_cl
            WHEN '012Rd000001B2txIAC' THEN pre.credit_limit
            WHEN '012Rd000002EppVIAS' THEN pre.credit_limit
            WHEN '012Rd000002Dp5CIAS' THEN pre.credit_limit
            ELSE NULL
        END                                             AS automated_cl,

        -- Approved CL (post-decision)
        CASE lb.recordtypeid
            WHEN '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
            WHEN '012Rd000000jbbJIAQ' THEN ld.pa_approved_cl
            WHEN '012Rd000001B2txIAC' THEN post.credit_limit
            WHEN '012Rd000002EppVIAS' THEN post.credit_limit
            WHEN '012Rd000002Dp5CIAS' THEN
                CASE WHEN lb.status__c IN ('Close Account','RMR/Disable')
                     THEN 0 ELSE post.credit_limit END
            ELSE COALESCE(lb.approved_uw_credit_limit__c, 0)
        END                                             AS approved_cl,

        -- Decision type (6-bucket standardised)
        CASE
            WHEN lb.recordtypeid = '012Rd000002Dp5CIAS' THEN
                CASE
                    WHEN lb.status__c = 'Complete - Current CL'          THEN 'No change'
                    WHEN lb.status__c = 'Reduce CL'                      THEN 'Decrease'
                    WHEN lb.status__c IN ('Close Account','RMR/Disable') THEN 'Close/RMR/Disable'
                    ELSE lb.status__c
                END
            ELSE
                CASE
                    WHEN CASE lb.recordtypeid
                             WHEN '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_approved_cl
                             WHEN '012Rd000001B2txIAC' THEN post.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN post.credit_limit
                             ELSE COALESCE(lb.approved_uw_credit_limit__c, 0)
                         END
                         >
                         CASE lb.recordtypeid
                             WHEN '012Rd000000AcjxIAC' THEN cm.first_approved_credit_limit
                             WHEN '0124T000000DSMTQA4' THEN sl.automated_cl
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_automated_cl
                             WHEN '012Rd000001B2txIAC' THEN pre.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN pre.credit_limit
                             ELSE NULL
                         END                                              THEN 'Increase'
                    WHEN CASE lb.recordtypeid
                             WHEN '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_approved_cl
                             WHEN '012Rd000001B2txIAC' THEN post.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN post.credit_limit
                             ELSE COALESCE(lb.approved_uw_credit_limit__c, 0)
                         END = 0                                          THEN 'Rejected'
                    WHEN CASE lb.recordtypeid
                             WHEN '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_approved_cl
                             WHEN '012Rd000001B2txIAC' THEN post.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN post.credit_limit
                             ELSE COALESCE(lb.approved_uw_credit_limit__c, 0)
                         END
                         <
                         CASE lb.recordtypeid
                             WHEN '012Rd000000AcjxIAC' THEN cm.first_approved_credit_limit
                             WHEN '0124T000000DSMTQA4' THEN sl.automated_cl
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_automated_cl
                             WHEN '012Rd000001B2txIAC' THEN pre.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN pre.credit_limit
                             ELSE NULL
                         END                                              THEN 'Decrease'
                    WHEN CASE lb.recordtypeid
                             WHEN '0124T000000DSMTQA4' THEN sl.first_approved_credit_limit
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_approved_cl
                             WHEN '012Rd000001B2txIAC' THEN post.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN post.credit_limit
                             ELSE COALESCE(lb.approved_uw_credit_limit__c, 0)
                         END
                         =
                         CASE lb.recordtypeid
                             WHEN '012Rd000000AcjxIAC' THEN cm.first_approved_credit_limit
                             WHEN '0124T000000DSMTQA4' THEN sl.automated_cl
                             WHEN '012Rd000000jbbJIAQ' THEN ld.pa_automated_cl
                             WHEN '012Rd000001B2txIAC' THEN pre.credit_limit
                             WHEN '012Rd000002EppVIAS' THEN pre.credit_limit
                             ELSE NULL
                         END                                              THEN 'No change'
                    ELSE 'Other'
                END
        END                                             AS decision_type,

        -- Risk factors (raw)
        lb.rf_bank_balance,
        lb.rf_quality_of_revenue,
        lb.rf_personal_credit,
        lb.rf_industry,
        lb.rf_debt_service,
        lb.rf_residual,

        -- Risk factors (numeric 1-3 for avg trend)
        lb.rf_bank_balance_num,
        lb.rf_quality_of_revenue_num,
        lb.rf_personal_credit_num,
        lb.rf_industry_num,
        lb.rf_debt_service_num,
        lb.rf_residual_num,

        -- Fill flags (for coverage warnings on low-data factors)
        CASE WHEN lb.rf_bank_balance       IS NOT NULL AND lb.rf_bank_balance       != '' THEN 1 ELSE 0 END AS rf_bank_balance_filled,
        CASE WHEN lb.rf_quality_of_revenue IS NOT NULL AND lb.rf_quality_of_revenue != '' THEN 1 ELSE 0 END AS rf_quality_of_revenue_filled,
        CASE WHEN lb.rf_personal_credit    IS NOT NULL AND lb.rf_personal_credit    != '' THEN 1 ELSE 0 END AS rf_personal_credit_filled,
        CASE WHEN lb.rf_industry           IS NOT NULL AND lb.rf_industry           != '' THEN 1 ELSE 0 END AS rf_industry_filled,
        CASE WHEN lb.rf_debt_service       IS NOT NULL AND lb.rf_debt_service       != '' THEN 1 ELSE 0 END AS rf_debt_service_filled,
        CASE WHEN lb.rf_residual           IS NOT NULL AND lb.rf_residual           != '' THEN 1 ELSE 0 END AS rf_residual_filled,

        -- DPD outcome: UDR35 = 1 if customer hit DPD>=35 on or after review date
        CASE
            WHEN dpd.first_udr35_date IS NOT NULL
             AND dpd.first_udr35_date >= lb.review_complete_date
            THEN 1 ELSE 0
        END                                             AS udr35,
        dpd.first_udr35_date,
        DATEDIFF('day', lb.review_complete_date, dpd.first_udr35_date) AS days_to_first_dpd35,

        -- Vantage at review: DACD 4.0 → 3.0 → INDUS → Salesforce
        COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf)              AS vantage_at_review,
        CASE
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) >= 800 THEN '800-850'
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) >= 750 THEN '750-800'
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) >= 700 THEN '700-750'
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) >= 650 THEN '650-700'
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) >= 600 THEN '600-650'
            WHEN COALESCE(dv.vantage_score, ind.vantage, lb.vantage_at_review_sf) <  600 THEN '<600'
            ELSE NULL
        END                                             AS vantage_bucket,
        dv.vantage_score                                AS vantage_dacd,
        dv.vantage_4                                    AS vantage_dacd_4,
        dv.vantage_3                                    AS vantage_dacd_3,
        ind.vantage                                     AS vantage_indus,
        lb.vantage_at_review_sf                         AS vantage_sf,
        ind.og_bucket_group,
        ind.og_bucket_group_banded,
        ind.ob_bucket_group_retro,
        ind.customer_annual_revenue_group,

        -- Revenue (raw + banded)
        CASE lb.recordtypeid
            WHEN '012Rd000000jbbJIAQ' THEN ld.pa_annual_revenue
            ELSE cm.customer_annual_revenue_raw
        END                                             AS annual_revenue_raw,
        CASE
            WHEN (CASE lb.recordtypeid
                    WHEN '012Rd000000jbbJIAQ' THEN ld.pa_annual_revenue
                    ELSE cm.customer_annual_revenue_raw END) >= 1500000 THEN '>$1.5M'
            WHEN (CASE lb.recordtypeid
                    WHEN '012Rd000000jbbJIAQ' THEN ld.pa_annual_revenue
                    ELSE cm.customer_annual_revenue_raw END) >= 500000  THEN '$500K-$1.5M'
            ELSE '$0-$500K'
        END                                             AS revenue_band,

        -- Partner (INDUS only — not available in daily_approved)
        CASE lb.recordtypeid
            WHEN '012Rd000000jbbJIAQ' THEN ld.pa_partner
            ELSE ind.partner
        END                                             AS partner,

        -- Customer info
        cm.first_approved_date,
        cm.first_draw_date,
        cm.first_draw_amount,

        -- Industry (NAICS at review date)
        dar.industry_naics_code_edate                   AS naics_code_at_review,
        n.naics_title                                   AS industry_label,

        -- Module D — Override matrix (Second Look; review-level grade, then SL account fallback)
        COALESCE(lb.sf_automated_risk_grade, sl.automated_risk_grade)               AS automated_risk_grade,
        COALESCE(lb.sf_uw_risk_grade, sl.underwriting_risk_grade__c)                  AS uw_risk_grade,
        sl.first_rejected_time                          AS sl_first_rejected_time,
        sl.first_risk_review_time                       AS sl_first_risk_review_time,
        sl.cl_delta                                     AS sl_cl_delta,
        CASE
            WHEN lb.recordtypeid = '0124T000000DSMTQA4' THEN 1
            ELSE COALESCE(sl.second_look_account, 0)
        END                                             AS is_second_look,
        CASE
            WHEN lb.recordtypeid = '0124T000000DSMTQA4'
             AND COALESCE(lb.sf_uw_risk_grade, sl.underwriting_risk_grade__c) IS NOT NULL
             AND sl.first_rejected_time IS NOT NULL
            THEN 1 ELSE 0
        END                                             AS is_muw_decided_sl,
        lb.review_complete_date                         AS override_lookback_date,

        -- Metadata
        CURRENT_TIMESTAMP()                             AS table_refreshed_at

    FROM loan_base lb
    CROSS JOIN period_anchor pa

    LEFT JOIN uw_hierarchy uh
        ON lb.uw_id_resolved = uh.uw_id
    LEFT JOIN team_leaders tl_uw
        ON lb.uw_id_resolved = tl_uw.user_id
    LEFT JOIN team_leaders tl_und
        ON lb.underwriter_id = tl_und.user_id
    LEFT JOIN EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER uw_und
        ON lb.underwriter_id = uw_und.id

    LEFT JOIN customer_master cm
        ON lb.fbbid = cm.fbbid

    -- Pre-decision CL: day before review completed (OG programs only)
    LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA pre
        ON lb.fbbid = pre.fbbid
        AND lb.review_complete_date = DATEADD('day', 1, pre.edate)
        AND lb.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')

    -- Post-decision CL: day after review completed (OG programs only)
    LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA post
        ON lb.fbbid = post.fbbid
        AND lb.review_complete_date = DATEADD('day', -1, post.edate)
        AND lb.recordtypeid IN ('012Rd000001B2txIAC','012Rd000002EppVIAS','012Rd000002Dp5CIAS')

    -- Pre-Approval specific fields
    LEFT JOIN leads ld
        ON lb.fbbid = ld.fbbid
        AND lb.recordtypeid = '012Rd000000jbbJIAQ'

    -- Second Look / Module D (deduped to latest per fbbid)
    LEFT JOIN second_look_deduped sl
        ON lb.fbbid = sl.fbbid

    -- Vantage (DACD) + OG bucket + partner (INDUS) at review date
    LEFT JOIN dacd_vantage_at_review dv
        ON lb.fbbid = dv.fbbid
        AND dv.edate = lb.review_complete_date
    LEFT JOIN indus_data ind
        ON lb.fbbid = ind.fbbid
        AND ind.edate = lb.review_complete_date

    -- DPD outcome
    LEFT JOIN first_dpd35 dpd
        ON lb.fbbid = dpd.fbbid

    -- NAICS code at review date
    LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dar
        ON lb.fbbid = dar.fbbid
        AND dar.edate = lb.review_complete_date

    LEFT JOIN naics n
        ON dar.industry_naics_code_edate = n.naics_code
)

SELECT * FROM assembled;


/* =========================================================================
   SCHEDULED TASK
   Run daily after FINANCE_METRICS_DAILY and INDUS refreshes complete.

   CREATE OR REPLACE TASK TABLEAU.CREDIT.run_muw_risk_factors_dashboard
       WAREHOUSE = KEY_METRICS_WH
       SCHEDULE  = 'USING CRON 30 04 * * * Asia/Kolkata'
   AS
   CREATE OR REPLACE TABLE ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD AS
   -- (paste full SELECT above);
========================================================================= */


/* =========================================================================
   VALIDATION QUERIES — run after CREATE to sanity check the table
========================================================================= */

-- 1. Row count by program
-- SELECT program_name,
--        COUNT(*) AS total_rows,
--        COUNT(DISTINCT fbbid) AS distinct_customers,
--        MIN(review_month) AS earliest,
--        MAX(review_month) AS latest
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- GROUP BY 1 ORDER BY 1;

-- 2. Risk factor fill rates by month (confirm Sep-24 ramp-up for quality_of_revenue)
-- SELECT review_month,
--        ROUND(AVG(rf_bank_balance_filled)*100,1)       AS bank_bal_pct,
--        ROUND(AVG(rf_quality_of_revenue_filled)*100,1) AS qual_rev_pct,
--        ROUND(AVG(rf_residual_filled)*100,1)           AS residual_pct
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- GROUP BY 1 ORDER BY 1;

-- 3. Module A — Vantage distribution (latest review per customer)
-- SELECT vantage_bucket, uw_type, COUNT(DISTINCT fbbid) AS fbbids
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- WHERE rnk_latest = 1 AND is_in_12mo = 1 AND vantage_bucket IS NOT NULL
-- GROUP BY 1,2 ORDER BY 1,2;

-- 3b. T4W week coverage (expect 4 Wed-start weeks)
-- SELECT review_week_start, COUNT(*) FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- WHERE is_in_t4w = 1 GROUP BY 1 ORDER BY 1;

-- 3c. Vantage fill — DACD 4.0 / 3.0 / INDUS / SF
-- SELECT ROUND(100*AVG(CASE WHEN vantage_at_review IS NOT NULL THEN 1 ELSE 0 END),1) AS pct,
--        ROUND(100*AVG(CASE WHEN vantage_dacd IS NOT NULL THEN 1 ELSE 0 END),1) AS dacd_pct,
--        ROUND(100*AVG(CASE WHEN vantage_dacd_4 IS NOT NULL THEN 1 ELSE 0 END),1) AS dacd_4_pct,
--        ROUND(100*AVG(CASE WHEN vantage_dacd_3 IS NOT NULL THEN 1 ELSE 0 END),1) AS dacd_3_pct,
--        ROUND(100*AVG(CASE WHEN vantage_indus IS NOT NULL THEN 1 ELSE 0 END),1) AS indus_pct,
--        ROUND(100*AVG(CASE WHEN vantage_sf IS NOT NULL THEN 1 ELSE 0 END),1) AS sf_pct
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD WHERE program_name = 'Pre-Approval';

-- 4. Module B.1 — Avg risk factor rating by month
-- SELECT review_month,
--        ROUND(AVG(rf_bank_balance_num),2)       AS avg_bank_balance,
--        ROUND(AVG(rf_quality_of_revenue_num),2) AS avg_quality_of_revenue,
--        ROUND(AVG(rf_personal_credit_num),2)    AS avg_personal_credit,
--        ROUND(AVG(rf_industry_num),2)           AS avg_industry,
--        ROUND(AVG(rf_debt_service_num),2)       AS avg_debt_service,
--        ROUND(AVG(rf_residual_num),2)           AS avg_residual
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- WHERE review_month >= DATEADD('month',-12,DATE_TRUNC('month',CURRENT_DATE))
-- GROUP BY 1 ORDER BY 1;

-- 5. Module B.2 — DQ% by bank balance rating bucket
-- SELECT review_month, rf_bank_balance AS rating,
--        COUNT(*) AS total,
--        SUM(udr35) AS dq_count,
--        ROUND(SUM(udr35)*100.0/NULLIF(COUNT(*),0),2) AS dq_pct
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- WHERE rf_bank_balance IS NOT NULL
--   AND review_month >= DATEADD('month',-12,DATE_TRUNC('month',CURRENT_DATE))
-- GROUP BY 1,2 ORDER BY 1,2;

-- 6. Module D — Override matrix (use review_complete_date for lookback, not sl_first_risk_review_time)
-- SELECT automated_risk_grade, uw_risk_grade, COUNT(*) AS cnt
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- WHERE program_name = 'Second Look'
--   AND automated_risk_grade IS NOT NULL
--   AND uw_risk_grade IS NOT NULL
--   AND is_in_3mo = 1   -- or is_in_t4w / is_in_6mo / is_in_12mo
-- GROUP BY 1,2 ORDER BY 1,2;

-- 7. TL/UW hierarchy check (self-TL for manager-level UWs baked into build)
-- SELECT
--     CASE WHEN tl_name IS NULL THEN '(blank)' ELSE tl_name END AS tl_name,
--     uw_name,
--     COUNT(*) AS reviews
-- FROM ANALYTICS.CREDIT.MUW_RISK_FACTORS_DASHBOARD
-- GROUP BY 1, 2
-- ORDER BY 3 DESC;