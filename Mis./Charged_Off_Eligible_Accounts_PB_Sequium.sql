-- ============================================================================
-- Post Charge Off Accounts - As of April 1, 2026
-- Description: Get all Post Charge Off accounts matching Key Metrics logic
-- With all exclusion flags from P_B_placement.sql and asset sale fields
-- ============================================================================

WITH 
--------------------------------------------------------------------------------
-- Exclude cancelled loans
--------------------------------------------------------------------------------
first_table AS (
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

--------------------------------------------------------------------------------
-- Charge-off data from FMD
--------------------------------------------------------------------------------
co_data AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.charge_off_date,
        fmd.outstanding_principal_due * COALESCE(flu.loan_fx_rate, 1.0) AS os_91
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    LEFT JOIN INDUS.PUBLIC.FX_LOAN_UNIFIED flu ON flu.LOAN_KEY = fmd.LOAN_KEY
    WHERE fmd.edate = '2026-04-08'
      AND fmd.is_charged_off = 1
      AND fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
),

--------------------------------------------------------------------------------
-- Aggregate to fbbid level (keep most recent charge-off date)
--------------------------------------------------------------------------------
co_fbbid AS (
    SELECT 
        fbbid,
        MAX(charge_off_date) AS charge_off_date,
        SUM(os_91) AS total_os_91,
        CASE 
            WHEN MAX(charge_off_date) BETWEEN DATEADD(YEAR, -5, '2026-04-01') AND '2026-04-01' 
            THEN 1 ELSE 0 
        END AS is_within_last_5_yrs
    FROM co_data
    GROUP BY fbbid
),

--------------------------------------------------------------------------------
-- 1. ACTIVE CUSTOM PLANS — from DIM_PAYMENT_PLAN
--------------------------------------------------------------------------------
active_custom_plans AS (
    SELECT
        fbbid,
        MAX(payment_plan_start_date) AS plan_start_date,
        MAX(DATEADD(day,
            CASE
                WHEN time_units = 'MONTH' THEN duration * 30
                WHEN time_units = 'WEEK'  THEN duration * 7
                WHEN time_units = 'DAY'   THEN duration
                ELSE 0
            END,
            payment_plan_start_date)) AS plan_end_date,
        MAX(CASE
                WHEN time_units = 'MONTH' THEN duration * 30
                WHEN time_units = 'WEEK'  THEN duration * 7
                WHEN time_units = 'DAY'   THEN duration
                ELSE 0
            END) AS duration_days
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
        payment_plan_start_date)) >= '2026-04-01'
),

--------------------------------------------------------------------------------
-- 2. DACD — Live account flags
--------------------------------------------------------------------------------
dacd AS (
    SELECT
        fbbid,
        name,
        full_street_address,
        city,
        state,
        zip_code,
        mobile_number,
        email,
        entity_id,
        first_approved_time,
        first_draw_time,
        first_draw_amount,
        CREDIT_STATUS,
        IS_FRAUD,
        IS_BANKRUPTCY,
        BANKRUPTCY_STATUS,
        BANKRUPTCY_CHAPTER,
        BK_FILING_DATE,
        IS_CHARGEOFF,
        DPD_DAYS,
        OUTSTANDING_PRINCIPAL AS current_os_principal,
        FEES_DUE AS current_fees_due,
        DISCOUNT_PENDING,
        RECOVERY_SUGGESTED_STATE,
        RECOVERY_SUGGESTED_SUBSTATE,
        IS_IN_BREATHER,
        IS_PERMANENTLY_RESTRICTED,
        DISPUTED_PRINCIPAL,
        DISPUTED_FEES,
        CBR_SUPPRESSION_FLAG,
        BBR_SUPPRESSION_FLAG,
        BANKRUPTCY_CATEGORY,
        IS_SETTLEMENT AS dacd_is_in_settlement
    FROM bi.public.daily_approved_customers_data
    WHERE edate = '2026-04-01'
),

--------------------------------------------------------------------------------
-- 3. CFS SETTLEMENT — Pivot from customer_finance_statuses_scd_v
--------------------------------------------------------------------------------
cfs_settlement AS (
    SELECT
        fbbid,
        MAX(CASE WHEN status_name = 'SETTLEMENT_STATUS'
            THEN TRIM(status_value::VARCHAR, '"') END) AS cfs_status,
        MAX(CASE WHEN status_name = 'IS_IN_DISCOUNTED_SETTLEMENT'
            THEN status_value::INTEGER END) AS cfs_is_in_settlement,
        MAX(CASE WHEN status_name = 'DATE_OF_SETTLEMENT_ARRANGEMENT'
            THEN TRY_TO_DATE(status_value::VARCHAR) END) AS cfs_created_date,
        MAX(CASE WHEN status_name = 'FINAL_SETTLEMENT_AMOUNT'
            THEN TRY_TO_DOUBLE(status_value::VARCHAR) END) AS cfs_offer_amount,
        MAX(CASE WHEN status_name = 'SETTLEMENT_PERCENT'
            THEN TRY_TO_DOUBLE(status_value::VARCHAR) END) AS cfs_pct,
        MAX(CASE WHEN status_name = 'SETTLEMENT_NUMBER_OF_PAYMENTS'
            THEN TRY_CAST(status_value::VARCHAR AS INTEGER) END) AS cfs_num_payments,
        MAX(CASE WHEN status_name = 'SETTLEMENT_AGENT_NAME'
            THEN TRIM(status_value::VARCHAR, '"') END) AS cfs_agent
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name IN (
        'SETTLEMENT_STATUS', 'IS_IN_DISCOUNTED_SETTLEMENT',
        'DATE_OF_SETTLEMENT_ARRANGEMENT', 'FINAL_SETTLEMENT_AMOUNT',
        'SETTLEMENT_PERCENT', 'SETTLEMENT_NUMBER_OF_PAYMENTS', 'SETTLEMENT_AGENT_NAME'
    )
    AND last_row = 1
    GROUP BY 1
),

--------------------------------------------------------------------------------
-- 4. CJK BACKY SETTLEMENTS
--------------------------------------------------------------------------------
cjk_backy AS (
    SELECT
        FBBID::INTEGER AS fbbid,
        CURRENT_STATUS AS cjk_status,
        SETTLEMENT_CREATED_DATE AS cjk_created_date,
        SETTLEMENT_END_TIME::DATE AS cjk_end_date,
        N_PAYMENTS_MADE AS cjk_payments_made,
        TOTAL_AMOUNT_PAID AS cjk_total_paid,
        AGENT AS cjk_agent,
        SETTLEMENT_RNK
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbbid
        ORDER BY settlement_rnk DESC, event_time DESC
    ) = 1
),

--------------------------------------------------------------------------------
-- 5. FRAUD TAGS — Captain fraud tag (category 1 = Fraud)
--------------------------------------------------------------------------------
fraud_tags AS (
    SELECT
        t.fbbid,
        topt.name AS fraud_type,
        t.tag_event_time::DATE AS fraud_tag_date,
        t.system_user AS fraud_tagged_by,
        t.comment AS fraud_comment,
        t.is_deleted AS fraud_tag_deleted,
        CASE
            WHEN t.is_deleted = FALSE THEN 'Active Fraud Tag'
            WHEN t.is_deleted = TRUE  THEN 'Fraud Tag (Cleared)'
        END AS fraud_tag_status
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

--------------------------------------------------------------------------------
-- 6. POST CO PAYMENT PLAN TAG — Captain tag category 71
--------------------------------------------------------------------------------
co_plan_tag AS (
    SELECT
        t.fbbid,
        topt.name AS co_plan_status,
        t.tag_event_time::DATE AS co_plan_date
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

--------------------------------------------------------------------------------
-- 7. BANKRUPTCY TAG — Captain tag category 28
--------------------------------------------------------------------------------
bk_tag AS (
    SELECT
        t.fbbid,
        topt.name AS bk_chapter_tag,
        t.tag_event_time::DATE AS bk_tag_date
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
),

--------------------------------------------------------------------------------
-- Charge Off Date (keep most recent charge-off per fbbid)
--------------------------------------------------------------------------------
co_date_cte AS (
    SELECT 
        fbbid, 
        from_date AS charge_off_effective_date
    FROM bi.finance.customer_finance_statuses_scd_v
    WHERE status_name = 'IS_CHARGEOFF'
      AND status_value = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY from_date DESC) = 1
),

--------------------------------------------------------------------------------
-- Charge Off Balance at Time of Charge Off (most recent charge-off)
--------------------------------------------------------------------------------
co_balance AS (
    SELECT 
        cb.fbbid, 
        cb.from_date AS co_date,
        cb.status_value AS co_balance
    FROM bi.finance.customer_finance_statuses_scd_v cb
    INNER JOIN co_date_cte cod ON cb.fbbid = cod.fbbid
        AND cb.from_date BETWEEN cod.charge_off_effective_date AND DATEADD(day, 1, cod.charge_off_effective_date)
    WHERE cb.status_name = 'BALANCE_DUE_FUNDED'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cb.fbbid ORDER BY cb.from_date DESC) = 1
),

--------------------------------------------------------------------------------
-- Current Balances
--------------------------------------------------------------------------------
current_balances AS (
    SELECT 
        fbbid, 
        SUM(principal) AS cur_principal, 
        SUM(fees - discount_pending) AS cur_fees, 
        SUM(balance_calc) AS cur_balance
    FROM bi.finance.fact_balance
    WHERE balance_type = 'BALANCE DUE FUNDED'
      AND DATE(to_ble_time_calc) >= '2030-01-01'
    GROUP BY 1
),

--------------------------------------------------------------------------------
-- Last Payment Date
--------------------------------------------------------------------------------
last_payment AS (
    SELECT 
        fbbid, 
        MAX(payment_transmission_date) AS last_payment_date
    FROM bi.finance.payments_data
    WHERE payment_status = 'FUND'
      AND direction = 'D'
    GROUP BY 1
),

--------------------------------------------------------------------------------
-- Total Paid
--------------------------------------------------------------------------------
total_paid AS (
    SELECT 
        a.fbbid, 
        SUM(b.loan_paid) AS total_paid 
    FROM (
        SELECT fbbid, loan_key
        FROM bi.finance.loan_statuses_scd_v
        WHERE status_name = 'LOAN_OPERATIONAL_STATUS'
          AND last_row = 1
          AND status_value IN ('CHOF', 'CMPB')
    ) a
    JOIN (
        SELECT fbbid, loan_key, SUM(payment_total_amount) AS loan_paid
        FROM bi.finance.payments_data
        WHERE direction = 'D'
          AND payment_status = 'FUND'
        GROUP BY 1, 2
    ) b ON a.fbbid = b.fbbid AND a.loan_key = b.loan_key
    GROUP BY 1
),

--------------------------------------------------------------------------------
-- Delinquency Date
--------------------------------------------------------------------------------
delinquency_info AS (
    SELECT 
        fbbid, 
        streak_start_date AS delinquency_date
    FROM cdc.recovery.recovery_business
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY streak_start_date DESC) = 1
),

--------------------------------------------------------------------------------
-- Guarantor Information
--------------------------------------------------------------------------------
guarantor_info AS (
    SELECT 
        fbbid, 
        ssn_encrypted, 
        date_of_birth_encrypted, 
        id AS verification_attempt_id
    FROM cdc.audit_log.individual_verification_attempts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY created_time DESC) = 1
),

pg_signee AS (
    SELECT 
        fbbid, 
        INITCAP(auditlog_fn) AS pg_signee_first_name, 
        INITCAP(auditlog_ln) AS pg_signee_last_name
    FROM (
        SELECT 
            al.fbbid,
            LOWER(ale.first_name) AS auditlog_fn, 
            LOWER(ale.last_name) AS auditlog_ln,
            ROW_NUMBER() OVER (PARTITION BY al.fbbid ORDER BY al.created_time DESC) AS rn
        FROM cdc.audit_log.audit_logs al 
        JOIN cdc.audit_log.audit_log_extensions ale ON ale.id = al.audit_log_extension_id
        WHERE al.type IN (
            'personal_guarantee_agreement',
            'personal_guarantee_agreement_term_loan',
            'fbx_populated_personal_guarantee_agreement',
            'feb_populated_personal_guarantee_agreement',
            'feb_populated_personal_guarantee_agreement_term_lo'
        )
    )
    WHERE rn = 1
),

--------------------------------------------------------------------------------
-- PG Info from CUSTOMERS_RT_DATA
--------------------------------------------------------------------------------
pg_rt_data AS (
    SELECT 
        fbbid,
        PG_FIRST_NAME,
        PG_LAST_NAME
    FROM BI.PUBLIC.CUSTOMERS_RT_DATA
    WHERE PG_FIRST_NAME IS NOT NULL
),

--------------------------------------------------------------------------------
-- Bank Account Details
--------------------------------------------------------------------------------
bank_account_1 AS (
    SELECT 
        ba.fbbid,
        RIGHT(ba.ACCOUNT_NUMBER_MASKED, 4) AS bank_account_last_4_digits,
        ba.routing_number,
        ba.account_type,
        pm."NAME" AS bank_account_name
    FROM cdc.payments.BANK_ACCOUNT_DETAILS ba
    LEFT JOIN cdc.payments.PAYMENT_METHODS pm ON pm.bank_account_id = ba.id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ba.fbbid ORDER BY pm."NAME" DESC) = 1
),

bank_account_2 AS (
    SELECT 
        ba.fbbid,
        RIGHT(ba.ACCOUNT_NUMBER_MASKED, 4) AS bank_account_last_4_digits,
        ba.routing_number,
        ba.account_type,
        pm."NAME" AS bank_account_name
    FROM cdc.payments.BANK_ACCOUNT_DETAILS ba
    LEFT JOIN cdc.payments.PAYMENT_METHODS pm ON pm.bank_account_id = ba.id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ba.fbbid ORDER BY pm."NAME" DESC) = 2
),

--------------------------------------------------------------------------------
-- IP Address
--------------------------------------------------------------------------------
ip_address AS (
    SELECT DISTINCT 
        checked_ip, 
        entity_id
    FROM CDC.ENTITIES.WHITEPAGESPRO_IP_CHECKS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY last_modified_time DESC) = 1
),

--------------------------------------------------------------------------------
-- Successful Verification
--------------------------------------------------------------------------------
successful_verification AS (
    SELECT INDIVIDUAL_VERIFICATION_ATTEMPT_ID
    FROM cdc.audit_log.SUCCESSFUL_VERIFICATIONS
),

--------------------------------------------------------------------------------
-- Customer Info
--------------------------------------------------------------------------------
dim_customers AS (
    SELECT fbbid, ein, full_street_address, city, state, zip_code
    FROM bi.public.dim_customers
)

--------------------------------------------------------------------------------
-- Final Output
--------------------------------------------------------------------------------
SELECT 
    -- ==================== ACCOUNT IDENTIFIERS ====================
    c.fbbid AS LoanID,
    'Fundbox' AS Lender,
    '2026-04-01' AS SnapshotDate,
    
    -- ==================== PLACEMENT INFORMATION ====================
    CASE 
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('3RD_P_SOLD', 'ASPIRE_LAW', 'BK_BL', 'EVANS_MUL', 
             'LP_HARVEST', 'LP_WELTMAN', 'MRS_PRIM', 'MRS_SEC', 'PB_CAP_PR', 'PB_CAPITAL', 
             'SEQ_PRIM', 'SEQ_SEC') THEN 'External'
        ELSE 'Internal'
    END AS PlacementStatus,
    CASE 
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('3RD_P_SOLD') THEN 'SCJ'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('ASPIRE_LAW') THEN 'ASPIRE_LAW'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('BK_BL') THEN 'BK_BL'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('EVANS_MUL') THEN 'EVANS_MUL' 
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('LP_HARVEST') THEN 'Harvest'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('LP_WELTMAN') THEN 'Weltman'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('MRS_PRIM', 'MRS_SEC') THEN 'MRS'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('PB_CAP_PR', 'PB_CAPITAL') THEN 'PB_Capital'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE IN ('SEQ_PRIM', 'SEQ_SEC') THEN 'SEQ'
        ELSE NULL
    END AS CurrentVendor,
    d.RECOVERY_SUGGESTED_STATE AS RecoveryState,
    d.RECOVERY_SUGGESTED_SUBSTATE AS VendorSubstate,
    
    -- ==================== EXCLUSION FLAGS ====================
    CASE WHEN d.RECOVERY_SUGGESTED_STATE = 'EOL' THEN 1 ELSE 0 END AS flag_eol,
    CASE WHEN d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR') THEN 1 ELSE 0 END AS flag_litigation,
    CASE WHEN d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD' THEN 1 ELSE 0 END AS flag_debt_sold,
    CASE WHEN d.RECOVERY_SUGGESTED_STATE = 'ELR'
          AND d.RECOVERY_SUGGESTED_SUBSTATE NOT IN ('PB_CAP_PR','PB_CAPITAL','PB_CAP_SEC','')
         THEN 1 ELSE 0 END AS flag_diff_vendor,
    CASE WHEN cs.cfs_is_in_settlement = 1
          OR cs.cfs_status IN ('ACTIVE','CREATED','FUNDED')
         THEN 1 ELSE 0 END AS flag_sett_cfs,
    CASE WHEN cjk.cjk_status IN ('ACTIVE','FUNDED') THEN 1 ELSE 0 END AS flag_sett_cjk,
    CASE WHEN f.fraud_tag_status = 'Active Fraud Tag' THEN 1 ELSE 0 END AS flag_fraud_tag,
    CASE WHEN d.IS_FRAUD = 1 THEN 1 ELSE 0 END AS flag_is_fraud,
    CASE WHEN d.IS_BANKRUPTCY = 1
          AND d.BANKRUPTCY_STATUS NOT IN ('DISMISSED_RESUME_COLLECTIONS','DISCHARGED_RESUME_COLLECTIONS')
         THEN 1 ELSE 0 END AS flag_bk_active,
    CASE WHEN d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY' THEN 1 ELSE 0 END AS flag_bk_discharged_nfa,
    CASE WHEN cp_dim.fbbid IS NOT NULL THEN 1 ELSE 0 END AS flag_custom_plan,
    CASE WHEN cp_tag.fbbid IS NOT NULL THEN 1 ELSE 0 END AS flag_co_plan_tag,
    CASE WHEN d.IS_PERMANENTLY_RESTRICTED = 1 THEN 1 ELSE 0 END AS flag_restricted,
    CASE WHEN d.DISPUTED_PRINCIPAL > 0 OR d.DISPUTED_FEES > 0 THEN 1 ELSE 0 END AS flag_dispute,
    
    -- ==================== MASTER EXCLUSION FLAG ====================
    CASE WHEN
            d.RECOVERY_SUGGESTED_STATE = 'EOL'
         OR d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR')
         OR d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD'
         OR d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY'
         OR (d.IS_BANKRUPTCY = 1
             AND d.BANKRUPTCY_STATUS NOT IN ('DISMISSED_RESUME_COLLECTIONS','DISCHARGED_RESUME_COLLECTIONS'))
         OR d.IS_PERMANENTLY_RESTRICTED = 1
         OR d.IS_FRAUD = 1
         OR f.fraud_tag_status = 'Active Fraud Tag'
         OR cs.cfs_is_in_settlement = 1
         OR cs.cfs_status IN ('ACTIVE','CREATED')
         OR cjk.cjk_status = 'ACTIVE'
         OR cp_dim.fbbid IS NOT NULL
         OR cp_tag.fbbid IS NOT NULL
         OR d.DISPUTED_PRINCIPAL > 0
         OR d.DISPUTED_FEES > 0
        THEN 1 ELSE 0
    END AS flag_exclude,
    
    -- ==================== EXCLUSION REASON ====================
    CASE
        WHEN d.RECOVERY_SUGGESTED_STATE = 'EOL' THEN 'EOL'
        WHEN d.RECOVERY_SUGGESTED_STATE IN ('PROLIT','PRELIT','TR_LR') THEN 'Litigation'
        WHEN d.RECOVERY_SUGGESTED_SUBSTATE = '3RD_P_SOLD' THEN 'Debt Sold'
        WHEN d.BANKRUPTCY_STATUS = 'DISCHARGED_NO_FURTHER_ACTIVITY' THEN 'BK Discharged'
        WHEN d.IS_BANKRUPTCY = 1 AND d.BANKRUPTCY_STATUS NOT IN ('DISMISSED_RESUME_COLLECTIONS','DISCHARGED_RESUME_COLLECTIONS')
            THEN 'Bankruptcy'
        WHEN d.IS_FRAUD = 1 OR f.fraud_tag_status = 'Active Fraud Tag' THEN 'Fraud'
        WHEN cs.cfs_is_in_settlement = 1 OR cs.cfs_status IN ('ACTIVE','CREATED') THEN 'Settlement (CFS)'
        WHEN cjk.cjk_status = 'ACTIVE' THEN 'Settlement (CJK)'
        WHEN cp_dim.fbbid IS NOT NULL THEN 'Custom Plan'
        WHEN cp_tag.fbbid IS NOT NULL THEN 'Post CO Plan'
        WHEN d.DISPUTED_PRINCIPAL > 0 OR d.DISPUTED_FEES > 0 THEN 'Dispute'
        ELSE 'Eligible'
    END AS exclusion_reason,
    
    -- ==================== TIME-BASED FLAGS ====================
    c.is_within_last_5_yrs AS Is_Within_Last_5_Yrs,
    
    -- ==================== BALANCE INFORMATION ====================
    c.total_os_91 AS OS_Principal,
    d.current_os_principal AS CurrentOSPrincipal_DACD,
    d.current_fees_due AS CurrentFeesDue,
    d.DISCOUNT_PENDING AS DiscountPending,
    (d.current_os_principal + d.current_fees_due - COALESCE(d.DISCOUNT_PENDING, 0)) AS TransferBalance,
    cb.cur_principal AS CurrentPrincipalBalance,
    cb.cur_fees AS CurrentFeeBalance,
    cb.cur_balance AS CurrentTotalBalance,
    
    -- ==================== CHARGE OFF INFO ====================
    c.charge_off_date AS ChargeOffDate,
    cod.charge_off_effective_date AS ChargeOffEffectiveDate,
    cobal.co_balance AS ChargeOffBalance,
    ROUND((COALESCE(cobal.co_balance, 0) - COALESCE(cb.cur_balance, 0)), 2) AS TotalPaymentsSinceChargeOff,
    
    -- ==================== LIVE FLAGS (DACD) ====================
    d.CREDIT_STATUS,
    d.IS_FRAUD,
    d.IS_BANKRUPTCY,
    d.BANKRUPTCY_STATUS,
    d.BANKRUPTCY_CHAPTER,
    d.BK_FILING_DATE,
    d.IS_CHARGEOFF,
    d.DPD_DAYS,
    d.IS_IN_BREATHER,
    d.IS_PERMANENTLY_RESTRICTED,
    d.DISPUTED_PRINCIPAL,
    d.DISPUTED_FEES,
    d.CBR_SUPPRESSION_FLAG,
    d.BBR_SUPPRESSION_FLAG,
    
    -- ==================== CUSTOM PLAN INFO ====================
    CASE WHEN cp_dim.fbbid IS NOT NULL THEN 1 ELSE 0 END AS has_active_custom_plan,
    cp_dim.plan_start_date AS custom_plan_start_date,
    cp_dim.plan_end_date AS custom_plan_end_date,
    cp_dim.duration_days AS custom_plan_duration_days,
    
    -- ==================== FRAUD TAG INFO ====================
    COALESCE(f.fraud_tag_status, 'No Fraud Tag') AS fraud_tag_status,
    f.fraud_type,
    f.fraud_tag_date,
    
    -- ==================== SETTLEMENT INFO (CFS) ====================
    COALESCE(cs.cfs_status, 'No Settlement') AS cfs_status,
    cs.cfs_is_in_settlement,
    cs.cfs_offer_amount,
    cs.cfs_pct AS cfs_pct_principal,
    cs.cfs_num_payments,
    
    -- ==================== SETTLEMENT INFO (CJK BACKY) ====================
    COALESCE(cjk.cjk_status, 'No Record') AS cjk_status,
    cjk.cjk_created_date,
    cjk.cjk_end_date,
    cjk.cjk_payments_made,
    cjk.cjk_total_paid,
    
    -- ==================== OTHER CAPTAIN TAGS ====================
    cp_tag.co_plan_status,
    cp_tag.co_plan_date,
    bk.bk_chapter_tag,
    bk.bk_tag_date,
    
    -- ==================== BUSINESS INFORMATION ====================
    d.name AS BusinessLegalName,
    d.full_street_address AS BusinessAddress,
    d.city AS BusinessCity,
    d.state AS BusinessState,
    d.zip_code AS BusinessZip,
    dc.ein AS TaxEIN,
    
    -- ==================== CONTRACT/FUNDING INFO ====================
    DATE(d.first_approved_time) AS ContractDate,
    DATE(d.first_draw_time) AS FundDate,
    d.first_draw_amount AS FundAmount,
    
    -- ==================== PAYMENT HISTORY ====================
    lp.last_payment_date AS LastPaymentDate,
    tp.total_paid AS TotalAmountPaid,
    
    -- ==================== GUARANTOR INFORMATION ====================
    CASE WHEN pgs.pg_signee_first_name IS NOT NULL OR pgrt.PG_FIRST_NAME IS NOT NULL 
         THEN 1 ELSE 0 END AS has_pg_info,
    pgs.pg_signee_first_name AS PG_FirstName_AuditLog,
    pgs.pg_signee_last_name AS PG_LastName_AuditLog,
    pgrt.PG_FIRST_NAME AS PG_FirstName_RT,
    pgrt.PG_LAST_NAME AS PG_LastName_RT,
    COALESCE(pgs.pg_signee_first_name, pgrt.PG_FIRST_NAME) AS GuarantorFirstName,
    COALESCE(pgs.pg_signee_last_name, pgrt.PG_LAST_NAME) AS GuarantorLastName,
    gi.ssn_encrypted AS GuarantorSSN,
    gi.date_of_birth_encrypted AS GuarantorDOB,
    dc.full_street_address AS GuarantorAddress,
    dc.city AS GuarantorCity,
    dc.state AS GuarantorState,
    dc.zip_code AS GuarantorZip,
    
    -- ==================== DELINQUENCY INFO ====================
    di.delinquency_date AS DelinquencyDate,
    
    -- ==================== CONTACT INFO ====================
    d.mobile_number AS BusinessPrimaryPhone,
    d.email AS Email,
    ip.checked_ip AS IPAddress,
    
    -- ==================== BANK ACCOUNT 1 ====================
    ba1.bank_account_last_4_digits AS Last4DigitsBA1,
    ba1.routing_number AS RoutingNumberBA1,
    ba1.account_type AS AccountTypeBA1,
    ba1.bank_account_name AS AccountNameBA1,
    
    -- ==================== BANK ACCOUNT 2 ====================
    CASE WHEN ba1.bank_account_last_4_digits = ba2.bank_account_last_4_digits THEN NULL 
         ELSE ba2.bank_account_last_4_digits END AS Last4DigitsBA2,
    CASE WHEN ba1.bank_account_last_4_digits = ba2.bank_account_last_4_digits THEN NULL 
         ELSE ba2.routing_number END AS RoutingNumberBA2,
    CASE WHEN ba1.bank_account_last_4_digits = ba2.bank_account_last_4_digits THEN NULL 
         ELSE ba2.account_type END AS AccountTypeBA2,
    CASE WHEN ba1.bank_account_last_4_digits = ba2.bank_account_last_4_digits THEN NULL 
         ELSE ba2.bank_account_name END AS AccountNameBA2,
    
    -- ==================== VERIFICATION INFO ====================
    gi.verification_attempt_id AS VerificationAttemptID,
    CASE WHEN gi.verification_attempt_id = sv.INDIVIDUAL_VERIFICATION_ATTEMPT_ID THEN 1 ELSE 0 END AS SuccessfulVerificationMatch

FROM co_fbbid c

-- DACD data
LEFT JOIN dacd d ON c.fbbid = d.fbbid

-- Exclusion data sources
LEFT JOIN active_custom_plans cp_dim ON c.fbbid = cp_dim.fbbid
LEFT JOIN cfs_settlement cs ON c.fbbid = cs.fbbid
LEFT JOIN cjk_backy cjk ON c.fbbid = cjk.fbbid
LEFT JOIN fraud_tags f ON c.fbbid = f.fbbid
LEFT JOIN co_plan_tag cp_tag ON c.fbbid = cp_tag.fbbid
LEFT JOIN bk_tag bk ON c.fbbid = bk.fbbid

-- Customer info
LEFT JOIN dim_customers dc ON c.fbbid = dc.fbbid

-- Balance info
LEFT JOIN co_date_cte cod ON c.fbbid = cod.fbbid
LEFT JOIN co_balance cobal ON c.fbbid = cobal.fbbid
LEFT JOIN current_balances cb ON c.fbbid = cb.fbbid

-- Payment info
LEFT JOIN last_payment lp ON c.fbbid = lp.fbbid
LEFT JOIN total_paid tp ON c.fbbid = tp.fbbid

-- Delinquency info
LEFT JOIN delinquency_info di ON c.fbbid = di.fbbid

-- Guarantor info
LEFT JOIN guarantor_info gi ON c.fbbid = gi.fbbid
LEFT JOIN pg_signee pgs ON c.fbbid = pgs.fbbid
LEFT JOIN pg_rt_data pgrt ON c.fbbid = pgrt.fbbid

-- Bank accounts
LEFT JOIN bank_account_1 ba1 ON c.fbbid = ba1.fbbid
LEFT JOIN bank_account_2 ba2 ON c.fbbid = ba2.fbbid

-- IP address
LEFT JOIN ip_address ip ON d.entity_id = ip.entity_id

-- Verification
LEFT JOIN successful_verification sv ON gi.verification_attempt_id = sv.INDIVIDUAL_VERIFICATION_ATTEMPT_ID

ORDER BY c.charge_off_date DESC;
