/*
================================================================================
  PB Capital — Vendor Placement Eligibility Check  v2
  Author  : Collections Analytics
  Date    : 2026-03-20

  CHANGES FROM v1
  ---------------
  - Custom plan check replaced: DACD IS_CUSTOM_PLAN removed.
    Now uses bi.finance.DIM_PAYMENT_PLAN with calculated end_date.
    Only flags account if end_date >= CURRENT_DATE() (truly active plan).
  - CJK Backy: uses analytics.credit.cjk_v_backy_settlements

  EXCLUSION CHECKS
  ----------------
  1.  Different external vendor active         RECOVERY_SUGGESTED_SUBSTATE != PB_CAP*
  2.  EOL (End of Life)                        RECOVERY_SUGGESTED_STATE = 'EOL'
  3.  In litigation / pre-lit / TR_LR          RECOVERY_SUGGESTED_STATE IN (PROLIT, PRELIT, TR_LR)
  4.  Debt sold (SCJ)                          RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD'
  5.  Active settlement — CFS                  cfs_is_in_settlement=1 OR cfs_status IN (ACTIVE, CREATED)
  6.  Active settlement — CJK Backy            cjk_status = 'ACTIVE'
  7.  Active fraud tag (Captain)               fraud_tag_status = 'Active Fraud Tag'
  8.  IS_FRAUD flag (DACD)                     IS_FRAUD = 1
  9.  Bankruptcy active / filed                IS_BANKRUPTCY=1 (excl. dismissed/discharged-resume)
  10. Bankruptcy discharged NFA                BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY'
  11. Active custom payment plan               DIM_PAYMENT_PLAN end_date >= CURRENT_DATE()
  12. Post CO payment plan tag (Captain)       tag_category_id = 71, is_deleted = FALSE
  13. Permanently restricted                   IS_PERMANENTLY_RESTRICTED = 1
  14. Active dispute                           DISPUTED_PRINCIPAL > 0 OR DISPUTED_FEES > 0
================================================================================
*/

Create or replace table tableau.credit.P_B_Placement_042026 as 

WITH

/*────────────────────────────────────────────────────────────────────────────
  1. ACTIVE CUSTOM PLANS — from DIM_PAYMENT_PLAN
     end_date = start_date + duration_days
     Only included if end_date >= CURRENT_DATE (plan still running)
────────────────────────────────────────────────────────────────────────────*/
active_custom_plans AS (
    SELECT
        fbbid,
        MAX(payment_plan_start_date)        AS plan_start_date,
        MAX(DATEADD(day,
            CASE
                WHEN time_units = 'MONTH' THEN duration * 30
                WHEN time_units = 'WEEK'  THEN duration * 7
                WHEN time_units = 'DAY'   THEN duration
                ELSE 0
            END,
            payment_plan_start_date))       AS plan_end_date,
        MAX(CASE
                WHEN time_units = 'MONTH' THEN duration * 30
                WHEN time_units = 'WEEK'  THEN duration * 7
                WHEN time_units = 'DAY'   THEN duration
                ELSE 0
            END)                            AS duration_days
    FROM bi.finance.DIM_PAYMENT_PLAN
    WHERE is_custom_plan = 1
      AND duration IS NOT NULL
    GROUP BY 1
    HAVING MAX(DATEADD(day,
        CASE
            WHEN time_units = 'MONTH' THEN duration * 30
            WHEN time_units = 'WEEK'  THEN duration * 7
            WHEN time_units = 'DAY'   THEN duration
            ELSE 0
        END,
        payment_plan_start_date)) >= CURRENT_DATE()
),

/*────────────────────────────────────────────────────────────────────────────
  2. DACD — Live account flags (today's snapshot)
────────────────────────────────────────────────────────────────────────────*/
dacd AS (
    SELECT
        fbbid,
        CREDIT_STATUS,
        IS_FRAUD,
        IS_BANKRUPTCY,
        BANKRUPTCY_STATUS,
        BANKRUPTCY_CHAPTER,
        BK_FILING_DATE,
        IS_CHARGEOFF,
        DPD_DAYS,
        OUTSTANDING_PRINCIPAL               AS current_os_principal,
        FEES_DUE                            AS current_fees_due,
        RECOVERY_SUGGESTED_STATE,
        RECOVERY_SUGGESTED_SUBSTATE,
        IS_IN_BREATHER,
        IS_PERMANENTLY_RESTRICTED,
        DISPUTED_PRINCIPAL,
        DISPUTED_FEES,
        CBR_SUPPRESSION_FLAG,
        BBR_SUPPRESSION_FLAG,
        BANKRUPTCY_CATEGORY,
        IS_SETTLEMENT                       AS dacd_is_in_settlement
    FROM bi.public.daily_approved_customers_data
    WHERE edate = CURRENT_DATE()
),

/*────────────────────────────────────────────────────────────────────────────
  3. CFS SETTLEMENT — Pivot from customer_finance_statuses_scd_v (LAST_ROW=1)
────────────────────────────────────────────────────────────────────────────*/
cfs_settlement AS (
    SELECT
        fbbid,
        MAX(CASE WHEN status_name = 'SETTLEMENT_STATUS'
            THEN TRIM(status_value::VARCHAR, '"') END)              AS cfs_status,
        MAX(CASE WHEN status_name = 'IS_IN_DISCOUNTED_SETTLEMENT'
            THEN status_value::INTEGER END)                         AS cfs_is_in_settlement,
        MAX(CASE WHEN status_name = 'DATE_OF_SETTLEMENT_ARRANGEMENT'
            THEN TRY_TO_DATE(status_value::VARCHAR) END)            AS cfs_created_date,
        MAX(CASE WHEN status_name = 'FINAL_SETTLEMENT_AMOUNT'
            THEN TRY_TO_DOUBLE(status_value::VARCHAR) END)          AS cfs_offer_amount,
        MAX(CASE WHEN status_name = 'SETTLEMENT_PERCENT'
            THEN TRY_TO_DOUBLE(status_value::VARCHAR) END)          AS cfs_pct,
        MAX(CASE WHEN status_name = 'SETTLEMENT_NUMBER_OF_PAYMENTS'
            THEN TRY_CAST(status_value::VARCHAR AS INTEGER) END)    AS cfs_num_payments,
        MAX(CASE WHEN status_name = 'SETTLEMENT_AGENT_NAME'
            THEN TRIM(status_value::VARCHAR, '"') END)              AS cfs_agent
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name IN (
        'SETTLEMENT_STATUS', 'IS_IN_DISCOUNTED_SETTLEMENT',
        'DATE_OF_SETTLEMENT_ARRANGEMENT', 'FINAL_SETTLEMENT_AMOUNT',
        'SETTLEMENT_PERCENT', 'SETTLEMENT_NUMBER_OF_PAYMENTS', 'SETTLEMENT_AGENT_NAME'
    )
    AND last_row = 1
    GROUP BY 1
),

/*────────────────────────────────────────────────────────────────────────────
  4. CFS SETTLEMENT END DATE — Derived from full history
────────────────────────────────────────────────────────────────────────────*/
cfs_dates AS (
    SELECT
        fbbid,
        MIN(from_date)                                              AS cfs_first_seen,
        MAX(CASE
            WHEN TRIM(status_value::VARCHAR, '"') IN ('FUNDED','FAILED','CANCELLED')
            THEN from_date END)                                     AS cfs_end_date
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name = 'SETTLEMENT_STATUS'
    GROUP BY 1
),

/*────────────────────────────────────────────────────────────────────────────
  5. CJK BACKY SETTLEMENTS — Latest settlement per fbbid
     analytics.credit.cjk_v_backy_settlements
     Columns: FBBID, REQUEST_ID, CORRECTED_CREATED_TIME, SETTLEMENT_END_TIME,
              SETTLEMENT_CREATED_DATE, SETTLEMENT_RNK, EVENT_TIME,
              CURRENT_STATUS, AGENT, N_PAYMENTS_MADE, TOTAL_AMOUNT_PAID
────────────────────────────────────────────────────────────────────────────*/
cjk_backy AS (
    SELECT
        FBBID::INTEGER                      AS fbbid,
        CURRENT_STATUS                      AS cjk_status,
        SETTLEMENT_CREATED_DATE             AS cjk_created_date,
        SETTLEMENT_END_TIME::DATE           AS cjk_end_date,
        N_PAYMENTS_MADE                     AS cjk_payments_made,
        TOTAL_AMOUNT_PAID                   AS cjk_total_paid,
        AGENT                               AS cjk_agent,
        SETTLEMENT_RNK
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbbid
        ORDER BY settlement_rnk DESC, event_time DESC
    ) = 1
),

/*────────────────────────────────────────────────────────────────────────────
  6. FRAUD TAGS — Latest Captain fraud tag per fbbid (category 1 = Fraud)
────────────────────────────────────────────────────────────────────────────*/
fraud_tags AS (
    SELECT
        t.fbbid,
        topt.name                           AS fraud_type,
        t.tag_event_time::DATE              AS fraud_tag_date,
        t.system_user                       AS fraud_tagged_by,
        t.comment                           AS fraud_comment,
        t.is_deleted                        AS fraud_tag_deleted,
        CASE
            WHEN t.is_deleted = FALSE THEN 'Active Fraud Tag'
            WHEN t.is_deleted = TRUE  THEN 'Fraud Tag (Cleared)'
        END                                 AS fraud_tag_status
    FROM cdc.feature_flags_hist.tags_fundbox_businesses t
    LEFT JOIN (
        SELECT DISTINCT id, name FROM cdc.feature_flags_hist.tag_options
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ble_time DESC) = 1
    ) topt ON t.tag_option_id = topt.id
    WHERE t.tag_category_id = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.fbbid ORDER BY t.tag_event_time DESC
    ) = 1
),

/*────────────────────────────────────────────────────────────────────────────
  7. POST CO PAYMENT PLAN TAG — Captain tag category 71 (active only)
────────────────────────────────────────────────────────────────────────────*/
co_plan_tag AS (
    SELECT
        t.fbbid,
        topt.name                           AS co_plan_status,
        t.tag_event_time::DATE              AS co_plan_date
    FROM cdc.feature_flags_hist.tags_fundbox_businesses t
    LEFT JOIN (
        SELECT DISTINCT id, name FROM cdc.feature_flags_hist.tag_options
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ble_time DESC) = 1
    ) topt ON t.tag_option_id = topt.id
    WHERE t.tag_category_id = 71
      AND t.is_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.fbbid ORDER BY t.tag_event_time DESC
    ) = 1
),

/*────────────────────────────────────────────────────────────────────────────
  8. BANKRUPTCY TAG — Captain tag category 28 (active only)
────────────────────────────────────────────────────────────────────────────*/
bk_tag AS (
    SELECT
        t.fbbid,
        topt.name                           AS bk_chapter_tag,
        t.tag_event_time::DATE              AS bk_tag_date
    FROM cdc.feature_flags_hist.tags_fundbox_businesses t
    LEFT JOIN (
        SELECT DISTINCT id, name FROM cdc.feature_flags_hist.tag_options
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ble_time DESC) = 1
    ) topt ON t.tag_option_id = topt.id
    WHERE t.tag_category_id = 28
      AND t.is_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.fbbid ORDER BY t.tag_event_time DESC
    ) = 1
)

/*================================================================================
  FINAL OUTPUT
================================================================================*/
SELECT

    /*── PLACEMENT INFO ──────────────────────────────────────────────────────*/
    p."Fbbid"                                                       AS fbbid,
    p."Business Name"                                               AS business_name,
    p."Person Name"                                                 AS person_name,
    p."Business Addr1 State"                                        AS state,
    p."placement type"                                              AS placement_type,
    p."Product type"                                                AS product_type,
    p."Total balance"                                               AS total_balance,
    p."OS principal"                                                AS os_principal,
    p."Non-principal balance"                                       AS non_principal_balance,
    p."legal_eligible"                                              AS legal_eligible,
    p."Lien filed"                                                  AS lien_filed,
    p."CHARGE_OFF_DATE"                                             AS charge_off_date,
    p."Date placed"                                                 AS pb_placement_date,
    p."PG_SIGNEE_FIRST_NAME"                                        AS pg_first_name,
    p."PG_SIGNEE_LAST_NAME"                                         AS pg_last_name,

    /*── LIVE FLAGS (DACD) ───────────────────────────────────────────────────*/
    d.CREDIT_STATUS,
    d.IS_FRAUD,
    d.IS_BANKRUPTCY,
    d.BANKRUPTCY_STATUS,
    d.BANKRUPTCY_CHAPTER,
    d.BK_FILING_DATE,
    d.IS_CHARGEOFF,
    d.DPD_DAYS,
    d.current_os_principal,
    d.current_fees_due,
    d.RECOVERY_SUGGESTED_STATE,
    d.RECOVERY_SUGGESTED_SUBSTATE,
    d.IS_IN_BREATHER,
    d.IS_PERMANENTLY_RESTRICTED,
    d.DISPUTED_PRINCIPAL,
    d.DISPUTED_FEES,
    d.CBR_SUPPRESSION_FLAG,
    d.BBR_SUPPRESSION_FLAG,

    /*── CUSTOM PLAN (DIM_PAYMENT_PLAN) ─────────────────────────────────────*/
    CASE WHEN cp_dim.fbbid IS NOT NULL THEN 1 ELSE 0 END            AS has_active_custom_plan,
    cp_dim.plan_start_date                                          AS custom_plan_start_date,
    cp_dim.plan_end_date                                            AS custom_plan_end_date,
    cp_dim.duration_days                                            AS custom_plan_duration_days,

    /*── FRAUD TAGS (Captain) ────────────────────────────────────────────────*/
    COALESCE(f.fraud_tag_status, 'No Fraud Tag')                    AS fraud_tag_status,
    f.fraud_type,
    f.fraud_tag_date,
    f.fraud_tagged_by,
    f.fraud_comment,

    /*── CFS SETTLEMENT ──────────────────────────────────────────────────────*/
    COALESCE(cs.cfs_status, 'No Settlement')                        AS cfs_status,
    cd.cfs_first_seen                                               AS cfs_created_date,
    cd.cfs_end_date,
    cs.cfs_is_in_settlement,
    cs.cfs_offer_amount,
    cs.cfs_pct                                                      AS cfs_pct_principal,
    cs.cfs_num_payments,
    cs.cfs_agent,

    /*── CJK BACKY SETTLEMENT ────────────────────────────────────────────────*/
    COALESCE(b.cjk_status, 'No Record')                             AS cjk_status,
    b.cjk_created_date,
    b.cjk_end_date,
    b.cjk_payments_made,
    b.cjk_total_paid,
    b.cjk_agent,

    /*── OTHER CAPTAIN TAGS ──────────────────────────────────────────────────*/
    cp_tag.co_plan_status,
    cp_tag.co_plan_date,
    bk.bk_chapter_tag,
    bk.bk_tag_date,

    /*────────────────────────────────────────────────────────────────────────
      INDIVIDUAL EXCLUSION FLAGS  (1 = issue found, review before sending)
    ────────────────────────────────────────────────────────────────────────*/
    CASE WHEN d.RECOVERY_SUGGESTED_STATE = 'EOL'
         THEN 1 ELSE 0 END                                          AS flag_eol,

    CASE WHEN d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR')
         THEN 1 ELSE 0 END                                          AS flag_litigation,

    CASE WHEN d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD'
         THEN 1 ELSE 0 END                                          AS flag_debt_sold,

    CASE WHEN d.RECOVERY_SUGGESTED_STATE = 'ELR'
          AND d.RECOVERY_SUGGESTED_SUBSTATE NOT IN (
              'PB_CAP_PR','PB_CAPITAL','PB_CAP_SEC','')
         THEN 1 ELSE 0 END                                          AS flag_diff_vendor,

    CASE WHEN cs.cfs_is_in_settlement = 1
          OR cs.cfs_status IN ('ACTIVE','CREATED','FUNDED')
         THEN 1 ELSE 0 END                                          AS flag_sett_cfs,

    CASE WHEN b.cjk_status in ('ACTIVE','FUNDED')
         THEN 1 ELSE 0 END                                          AS flag_sett_cjk,

    CASE WHEN f.fraud_tag_status = 'Active Fraud Tag'
         THEN 1 ELSE 0 END                                          AS flag_fraud_tag,

    CASE WHEN d.IS_FRAUD = 1
         THEN 1 ELSE 0 END                                          AS flag_is_fraud,

    CASE WHEN d.IS_BANKRUPTCY = 1
          AND d.BANKRUPTCY_STATUS NOT IN (
              'DISMISSED_RESUME_COLLECTIONS',
              'DISCHARGED_RESUME_COLLECTIONS')
         THEN 1 ELSE 0 END                                          AS flag_bk_active,

    CASE WHEN d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY'
         THEN 1 ELSE 0 END                                          AS flag_bk_discharged_nfa,

    CASE WHEN cp_dim.fbbid IS NOT NULL
         THEN 1 ELSE 0 END                                          AS flag_custom_plan,

    CASE WHEN cp_tag.fbbid IS NOT NULL
         THEN 1 ELSE 0 END                                          AS flag_co_plan_tag,

    CASE WHEN d.IS_PERMANENTLY_RESTRICTED = 1
         THEN 1 ELSE 0 END                                          AS flag_restricted,

    CASE WHEN d.DISPUTED_PRINCIPAL > 0 OR d.DISPUTED_FEES > 0
         THEN 1 ELSE 0 END                                          AS flag_dispute,

    /*────────────────────────────────────────────────────────────────────────
      MASTER FLAG — 1 = DO NOT SEND, 0 = ELIGIBLE
    ────────────────────────────────────────────────────────────────────────*/
    CASE WHEN
            d.RECOVERY_SUGGESTED_STATE = 'EOL'
         OR d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR')
         OR d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD'
         OR d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY'
         OR (d.IS_BANKRUPTCY = 1
             AND d.BANKRUPTCY_STATUS NOT IN (
                 'DISMISSED_RESUME_COLLECTIONS','DISCHARGED_RESUME_COLLECTIONS'))
         OR (d.RECOVERY_SUGGESTED_STATE = 'ELR'
             AND d.RECOVERY_SUGGESTED_SUBSTATE NOT IN (
                 'PB_CAP_PR','PB_CAPITAL','PB_CAP_SEC',''))
         OR d.IS_PERMANENTLY_RESTRICTED = 1
         OR d.IS_FRAUD = 1
         OR f.fraud_tag_status = 'Active Fraud Tag'
         OR cs.cfs_is_in_settlement = 1
         OR cs.cfs_status IN ('ACTIVE','CREATED')
         OR b.cjk_status = 'ACTIVE'
         OR cp_dim.fbbid IS NOT NULL
         OR cp_tag.fbbid IS NOT NULL
         OR d.DISPUTED_PRINCIPAL > 0
         OR d.DISPUTED_FEES > 0
        THEN 1 ELSE 0
    END                                                             AS flag_exclude,

    /*── EXCLUSION REASON (first matching, priority order) ───────────────────*/
    CASE
        WHEN d.RECOVERY_SUGGESTED_STATE = 'EOL'
            THEN 'EOL — End of Life'
        WHEN d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR')
            THEN 'In Litigation / Pre-Lit'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD'
            THEN 'Debt Sold (SCJ)'
        WHEN d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY'
            THEN 'BK Discharged — No Further Activity'
        WHEN d.IS_BANKRUPTCY = 1
          AND d.BANKRUPTCY_STATUS NOT IN (
              'DISMISSED_RESUME_COLLECTIONS','DISCHARGED_RESUME_COLLECTIONS')
            THEN CONCAT('Bankruptcy — ', COALESCE(d.BANKRUPTCY_STATUS, 'Active'))
        WHEN d.RECOVERY_SUGGESTED_STATE = 'ELR'
          AND d.RECOVERY_SUGGESTED_SUBSTATE NOT IN (
              'PB_CAP_PR','PB_CAPITAL','PB_CAP_SEC','')
            THEN CONCAT('Different Vendor — ', d.RECOVERY_SUGGESTED_SUBSTATE)
        WHEN d.IS_PERMANENTLY_RESTRICTED = 1
            THEN 'Permanently Restricted'
        WHEN d.IS_FRAUD = 1 OR f.fraud_tag_status = 'Active Fraud Tag'
            THEN CONCAT('Fraud — ', COALESCE(f.fraud_type, 'IS_FRAUD Flag'))
        WHEN cs.cfs_is_in_settlement = 1 OR cs.cfs_status IN ('ACTIVE','CREATED')
            THEN CONCAT('Active Settlement (CFS) — ', COALESCE(cs.cfs_status, 'In Settlement'))
        WHEN b.cjk_status = 'ACTIVE'
            THEN 'Active Settlement (CJK Backy)'
        WHEN cp_dim.fbbid IS NOT NULL
            THEN CONCAT('Active Custom Plan — ends ', cp_dim.plan_end_date::VARCHAR)
        WHEN cp_tag.fbbid IS NOT NULL
            THEN CONCAT('Post CO Payment Plan — ', COALESCE(cp_tag.co_plan_status, ''))
        WHEN d.DISPUTED_PRINCIPAL > 0 OR d.DISPUTED_FEES > 0
            THEN 'Active Dispute'
        ELSE 'Eligible for Placement'
    END                                                             AS exclusion_reason

FROM analytics.credit.PB_placement_CO_0304 p
LEFT JOIN dacd                d       ON p."Fbbid" = d.fbbid
LEFT JOIN active_custom_plans cp_dim  ON p."Fbbid" = cp_dim.fbbid
LEFT JOIN cfs_settlement      cs      ON p."Fbbid" = cs.fbbid
LEFT JOIN cfs_dates           cd      ON p."Fbbid" = cd.fbbid
LEFT JOIN cjk_backy           b       ON p."Fbbid" = b.fbbid
LEFT JOIN fraud_tags          f       ON p."Fbbid" = f.fbbid
LEFT JOIN co_plan_tag         cp_tag  ON p."Fbbid" = cp_tag.fbbid
LEFT JOIN bk_tag              bk      ON p."Fbbid" = bk.fbbid

ORDER BY flag_exclude DESC, total_balance DESC
;

Select * from analytics.credit.PB_placement_CO_2402
where "Fbbid" in (
Select fbbid from tableau.credit.P_B_Placement_032026
where exclusion_reason = 'Eligible for Placement' 
and (cjk_status not in ('ACTIVE','FUNDED') and cfs_status not in ('ACTIVE','FUNDED'))
);




Select * from tableau.credit.P_B_Placement_042026


