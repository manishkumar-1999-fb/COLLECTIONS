-- =====================================================
-- Charged Off Account Lookup - 2021 Onwards
-- Shows: Internal vs Agency accounts, Recoveries, Outstanding balances, Agency time
-- =====================================================

CREATE OR REPLACE TABLE analytics.credit.co_lookup_2021_onwards AS

WITH status_history AS (
    SELECT 
        fbbid,
        edate,
        recovery_suggested_state,
        recovery_suggested_substate,
        outstanding_principal,
        fees_due,
        discount_pending,
        date(CHARGEOFF_TIME) AS charge_off_date,
        (outstanding_principal + fees_due - discount_pending) AS balance_at_snapshot,
        CASE 
            WHEN recovery_suggested_state = LAG(recovery_suggested_state) OVER (PARTITION BY fbbid ORDER BY edate ASC)
                 AND (
                      recovery_suggested_substate = LAG(recovery_suggested_substate) OVER (PARTITION BY fbbid ORDER BY edate ASC)
                      OR (recovery_suggested_substate IS NULL AND LAG(recovery_suggested_substate) OVER (PARTITION BY fbbid ORDER BY edate ASC) IS NULL)
                 )
            THEN 0 ELSE 1 
        END AS is_new_transition
    FROM bi.public.daily_approved_customers_data 
    WHERE date(CHARGEOFF_TIME) IS NOT NULL
      AND date(CHARGEOFF_TIME) >= '2021-01-01'
),

state_transitions AS (
    SELECT 
        fbbid,
        edate AS transfer_date,
        recovery_suggested_state,
        recovery_suggested_substate,
        outstanding_principal,
        fees_due,
        discount_pending,
        balance_at_snapshot,
        charge_off_date,
        LEAD(edate, 1, CURRENT_DATE) OVER (PARTITION BY fbbid ORDER BY edate) AS next_transfer_date,
        ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY edate ASC) AS state_sequence
    FROM status_history
    WHERE is_new_transition = 1 
      AND ((recovery_suggested_state = 'ELR' AND recovery_suggested_substate IS NOT NULL) 
           OR recovery_suggested_state <> 'ELR')
),

current_state AS (
    SELECT *
    FROM state_transitions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY transfer_date DESC) = 1
),

-- Get latest loan operational status to exclude cancelled loans (aligns with Collections_KM)
first_table AS (
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

-- Charge-off balances from FMD (aligns with Collections_KM co_weekly logic)
-- Excludes: Flexpay, Term Loans, Cancelled loans
charge_off_balances_raw AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.charge_off_date,
        fmd.outstanding_principal_due AS co_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 1
      AND fmd.charge_off_date >= '2021-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fmd.loan_key ORDER BY fmd.edate ASC) = 1
),

-- Aggregate to fbbid level (same as Collections_KM co_fbbid_weekly)
charge_off_balances AS (
    SELECT 
        fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(co_principal) AS co_principal,
        SUM(co_principal) AS co_balance  -- Using principal as balance for alignment
    FROM charge_off_balances_raw
    GROUP BY fbbid
),

-- Current outstanding principal from today's snapshot (aligns with Collections_KM logic)
-- Same filters: Excludes Flexpay, Term Loans, Cancelled loans
current_os_snapshot AS (
    SELECT 
        fmd.fbbid,
        SUM(fmd.outstanding_principal_due) AS current_outstanding_principal_today
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    WHERE fmd.edate = CURRENT_DATE - 1
      AND fmd.is_charged_off = 1
      AND fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
    GROUP BY fmd.fbbid
),

agency_history AS (
    SELECT 
        fbbid,
        recovery_suggested_substate AS agency_substate,
        MIN(transfer_date) AS first_agency_date,
        MAX(next_transfer_date) AS last_agency_date,
        DATEDIFF('day', MIN(transfer_date), 
                 CASE WHEN MAX(next_transfer_date) = '2090-01-01' THEN CURRENT_DATE 
                      ELSE MAX(next_transfer_date) END) AS days_with_agency
    FROM state_transitions
    WHERE recovery_suggested_state = 'ELR'
    GROUP BY fbbid, recovery_suggested_substate
),

agency_history_ordered AS (
    SELECT 
        fbbid,
        agency_substate,
        first_agency_date,
        last_agency_date,
        days_with_agency,
        ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY first_agency_date) AS agency_order
    FROM agency_history
),

total_agency_time AS (
    SELECT 
        fbbid,
        COUNT(DISTINCT agency_substate) AS num_agencies_placed,
        SUM(days_with_agency) AS total_days_with_agencies,
        LISTAGG(agency_substate, ', ') WITHIN GROUP (ORDER BY agency_order) AS agencies_history,
        MIN(first_agency_date) AS first_ever_agency_date,
        MAX(last_agency_date) AS last_agency_date
    FROM agency_history_ordered
    GROUP BY fbbid
),

-- State transitions with date windows and placement status
state_with_placement AS (
    SELECT 
        fbbid,
        transfer_date,
        next_transfer_date,
        recovery_suggested_state,
        recovery_suggested_substate,
        CASE 
            WHEN recovery_suggested_state IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR recovery_suggested_state IS NULL THEN 'Internal'
            WHEN recovery_suggested_state IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_status_at_time
    FROM state_transitions
),

-- Payments with state at time of payment and MOB_CO
payments_with_state AS (
    SELECT 
        p.fbbid,
        c.charge_off_date,
        YEAR(c.charge_off_date) AS co_year,
        YEAR(c.charge_off_date) || '-Q' || QUARTER(c.charge_off_date) AS co_quarter,
        LAST_DAY(c.charge_off_date) AS co_month,
        c.co_balance,
        date(p.payment_event_time) AS payment_date,
        LAST_DAY(date(p.payment_event_time)) AS payment_month,
        TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) AS payment_amount,
        s.placement_status_at_time,
        DATEDIFF('day', c.charge_off_date, date(p.payment_event_time)) AS days_since_co,
        FLOOR(DATEDIFF('day', c.charge_off_date, date(p.payment_event_time)) / 30) AS mob_co
    FROM bi.finance.payments_model p
    INNER JOIN charge_off_balances c ON p.fbbid = c.fbbid
    LEFT JOIN state_with_placement s 
        ON p.fbbid = s.fbbid 
        AND date(p.payment_event_time) > s.transfer_date 
        AND date(p.payment_event_time) <= s.next_transfer_date
    WHERE p.payment_status = 'FUND' 
      AND p.parent_payment_id IS NOT NULL
      AND date(p.payment_event_time) >= c.charge_off_date
),

-- Aggregate payments by internal vs external
post_co_payments AS (
    SELECT 
        fbbid,
        SUM(payment_amount) AS total_recovered_post_co,
        COUNT(*) AS payment_count_post_co,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date,
        -- Split by placement at time of payment
        SUM(CASE WHEN placement_status_at_time = 'Internal' OR placement_status_at_time IS NULL THEN payment_amount ELSE 0 END) AS internal_recovery_amount,
        SUM(CASE WHEN placement_status_at_time = 'External' THEN payment_amount ELSE 0 END) AS external_recovery_amount,
        COUNT(CASE WHEN placement_status_at_time = 'Internal' OR placement_status_at_time IS NULL THEN 1 END) AS internal_payment_count,
        COUNT(CASE WHEN placement_status_at_time = 'External' THEN 1 END) AS external_payment_count
    FROM payments_with_state
    GROUP BY fbbid
),

settlements AS (
    SELECT 
        fbbid,
        current_status AS settlement_status,
        settlement_created_date,
        settlement_end_time
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY event_time DESC) = 1
),

final_data AS (
    SELECT 
        co.fbbid,
        co.charge_off_date,
        LAST_DAY(co.charge_off_date) AS co_month,
        YEAR(co.charge_off_date) AS co_year,
        YEAR(co.charge_off_date) || '-Q' || QUARTER(co.charge_off_date) AS co_quarter,
        
        co.co_principal,
        co.co_fees,
        co.co_discount,
        co.co_balance,
        
        cs.recovery_suggested_state AS current_state,
        cs.recovery_suggested_substate AS current_substate,
        cs.outstanding_principal AS current_outstanding_principal,
        cs.fees_due AS current_fees,
        cs.discount_pending AS current_discount,
        cs.balance_at_snapshot AS current_balance,
        
        CASE 
            WHEN cs.recovery_suggested_substate IN ('3RD_P_SOLD') THEN 'SCJ'
            WHEN cs.recovery_suggested_substate IN ('ASPIRE_LAW') THEN 'ASPIRE_LAW'
            WHEN cs.recovery_suggested_substate IN ('BK_BL') THEN 'BK_BL'
            WHEN cs.recovery_suggested_substate IN ('EVANS_MUL') THEN 'EVANS_MUL' 
            WHEN cs.recovery_suggested_substate IN ('LP_HARVEST') THEN 'Harvest'
            WHEN cs.recovery_suggested_substate IN ('LP_WELTMAN') THEN 'Weltman'
            WHEN cs.recovery_suggested_substate IN ('MRS_PRIM', 'MRS_SEC') THEN 'MRS'
            WHEN cs.recovery_suggested_substate IN ('PB_CAP_PR', 'PB_CAPITAL') THEN 'PB_Capital'
            WHEN cs.recovery_suggested_substate IN ('SEQ_PRIM', 'SEQ_SEC') THEN 'SEQ'
            WHEN cs.recovery_suggested_state IN ('PROLIT', 'TR_LR') THEN 'External_non_agency'
            ELSE 'N/A'
        END AS current_vendor_name,
        
        CASE 
            WHEN cs.recovery_suggested_state IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR cs.recovery_suggested_state IS NULL THEN 'Internal'
            WHEN cs.recovery_suggested_state IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS current_placement_type,
        
        CASE WHEN cs.recovery_suggested_state = 'ELR' THEN 1 ELSE 0 END AS is_currently_with_agency,
        
        COALESCE(ah.num_agencies_placed, 0) AS num_agencies_ever_placed,
        CASE WHEN COALESCE(ah.num_agencies_placed, 0) > 0 THEN 1 ELSE 0 END AS ever_placed_with_agency_flag,
        ah.agencies_history,
        ah.first_ever_agency_date,
        ah.last_agency_date AS agency_exit_date,
        COALESCE(ah.total_days_with_agencies, 0) AS total_days_with_agencies,
        ROUND(COALESCE(ah.total_days_with_agencies, 0) / 30.0, 1) AS months_with_agencies,
        
        COALESCE(pmt.total_recovered_post_co, 0) AS total_recovered_post_co,
        COALESCE(pmt.payment_count_post_co, 0) AS payment_count_post_co,
        pmt.first_payment_date,
        pmt.last_payment_date,
        
        -- Recovery split by Internal vs External (based on state at time of payment)
        COALESCE(pmt.internal_recovery_amount, 0) AS internal_recovery_amount,
        COALESCE(pmt.external_recovery_amount, 0) AS external_recovery_amount,
        COALESCE(pmt.internal_payment_count, 0) AS internal_payment_count,
        COALESCE(pmt.external_payment_count, 0) AS external_payment_count,
        
        co.co_balance - COALESCE(pmt.total_recovered_post_co, 0) AS remaining_balance_to_recover,
        
        ROUND(COALESCE(pmt.total_recovered_post_co, 0) / NULLIF(co.co_balance, 0) * 100, 2) AS recovery_rate_pct,
        ROUND(COALESCE(pmt.internal_recovery_amount, 0) / NULLIF(co.co_balance, 0) * 100, 2) AS internal_recovery_rate_pct,
        ROUND(COALESCE(pmt.external_recovery_amount, 0) / NULLIF(co.co_balance, 0) * 100, 2) AS external_recovery_rate_pct,
        
        DATEDIFF('day', co.charge_off_date, CURRENT_DATE) AS days_since_charge_off,
        ROUND(DATEDIFF('day', co.charge_off_date, CURRENT_DATE) / 30.0, 1) AS months_since_charge_off,
        
        -- Settlement flags
        stl.settlement_status,
        stl.settlement_created_date,
        stl.settlement_end_time,
        CASE WHEN stl.settlement_status = 'FUNDED' THEN 1 ELSE 0 END AS is_settlement_funded_flag,
        CASE WHEN stl.settlement_status = 'ACTIVE' THEN 1 ELSE 0 END AS is_settlement_active_flag,
        CASE WHEN stl.settlement_status IN ('FUNDED', 'ACTIVE') THEN 1 ELSE 0 END AS is_settled_or_in_settlement_flag,
        
        -- Sold debt flag
        CASE WHEN cs.recovery_suggested_substate = '3RD_P_SOLD' THEN 1 ELSE 0 END AS is_sold_debt_flag,
        
        -- On books / Collectible flag (excludes sold and fully settled)
        CASE 
            WHEN cs.recovery_suggested_substate = '3RD_P_SOLD' THEN 0
            WHEN stl.settlement_status = 'FUNDED' THEN 0
            ELSE 1 
        END AS is_on_books_collectible_flag,
        
        -- Current Outstanding Principal from today's FMD snapshot (aligns with Collections_KM)
        COALESCE(cos.current_outstanding_principal_today, 0) AS current_outstanding_principal_today,
        
        -- Last 5 Year Charged Off Flag (aligns with Collections_KM is_within_last_5_yrs)
        CASE 
            WHEN co.charge_off_date BETWEEN DATEADD(YEAR, -5, CURRENT_DATE) AND CURRENT_DATE 
            THEN 1 ELSE 0 
        END AS is_last_5_year_co_flag
        
    FROM charge_off_balances co
    LEFT JOIN current_state cs ON co.fbbid = cs.fbbid
    LEFT JOIN total_agency_time ah ON co.fbbid = ah.fbbid
    LEFT JOIN post_co_payments pmt ON co.fbbid = pmt.fbbid
    LEFT JOIN settlements stl ON co.fbbid = stl.fbbid
    LEFT JOIN current_os_snapshot cos ON co.fbbid = cos.fbbid
)

SELECT * FROM final_data;


-- =====================================================
-- SUMMARY REPORT: Account Level Detail
-- =====================================================
SELECT 
    fbbid,
    charge_off_date,
    co_quarter,
    co_year,
    co_principal,
    co_balance AS charged_off_balance,
    current_outstanding_principal_today,
    current_placement_type,
    current_vendor_name,
    is_currently_with_agency,
    ever_placed_with_agency_flag,
    agencies_history,
    total_days_with_agencies,
    months_with_agencies,
    -- Recovery split by state at time of payment
    total_recovered_post_co,
    internal_recovery_amount,
    external_recovery_amount,
    remaining_balance_to_recover,
    recovery_rate_pct,
    internal_recovery_rate_pct,
    external_recovery_rate_pct,
    months_since_charge_off,
    -- Settlement & Sold Flags
    settlement_status,
    is_settlement_funded_flag,
    is_settlement_active_flag,
    is_sold_debt_flag,
    is_on_books_collectible_flag,
    -- Last 5 Year Flag (aligns with Collections_KM)
    is_last_5_year_co_flag
FROM analytics.credit.co_lookup_2021_onwards
ORDER BY charge_off_date, fbbid;


-- =====================================================
-- SUMMARY REPORT 1: By Charge-Off Cohort (Year/Quarter)
-- =====================================================
SELECT 
    co_year,
    co_quarter,
    
    COUNT(DISTINCT fbbid) AS total_accounts,
    SUM(co_balance) AS total_charged_off_balance,
    
    COUNT(DISTINCT CASE WHEN current_placement_type = 'Internal' THEN fbbid END) AS internal_recovery_accounts,
    SUM(CASE WHEN current_placement_type = 'Internal' THEN co_balance END) AS internal_recovery_balance,
    
    COUNT(DISTINCT CASE WHEN current_placement_type = 'External' THEN fbbid END) AS external_agency_accounts,
    SUM(CASE WHEN current_placement_type = 'External' THEN co_balance END) AS external_agency_balance,
    
    COUNT(DISTINCT CASE WHEN ever_placed_with_agency_flag = 1 THEN fbbid END) AS ever_placed_to_agency_accounts,
    
    -- Settlement & Sold Stats
    COUNT(DISTINCT CASE WHEN is_settlement_funded_flag = 1 THEN fbbid END) AS settled_funded_accounts,
    SUM(CASE WHEN is_settlement_funded_flag = 1 THEN co_balance END) AS settled_funded_balance,
    
    COUNT(DISTINCT CASE WHEN is_settlement_active_flag = 1 THEN fbbid END) AS settlement_active_accounts,
    SUM(CASE WHEN is_settlement_active_flag = 1 THEN co_balance END) AS settlement_active_balance,
    
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 1 THEN fbbid END) AS sold_debt_accounts,
    SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance END) AS sold_debt_balance,
    
    COUNT(DISTINCT CASE WHEN is_on_books_collectible_flag = 1 THEN fbbid END) AS on_books_collectible_accounts,
    SUM(CASE WHEN is_on_books_collectible_flag = 1 THEN co_balance END) AS on_books_collectible_balance,
    
    SUM(total_recovered_post_co) AS total_recovered,
    SUM(internal_recovery_amount) AS internal_recovered,
    SUM(external_recovery_amount) AS external_recovered,
    SUM(remaining_balance_to_recover) AS total_remaining_to_recover,
    
    ROUND(SUM(total_recovered_post_co) / NULLIF(SUM(co_balance), 0) * 100, 2) AS overall_recovery_rate_pct,
    ROUND(SUM(internal_recovery_amount) / NULLIF(SUM(co_balance), 0) * 100, 2) AS internal_recovery_rate_pct,
    ROUND(SUM(external_recovery_amount) / NULLIF(SUM(co_balance), 0) * 100, 2) AS external_recovery_rate_pct,
    
    ROUND(AVG(months_with_agencies), 1) AS avg_months_with_agencies
    
FROM analytics.credit.co_lookup_2021_onwards
GROUP BY co_year, co_quarter
ORDER BY co_year, co_quarter;


-- =====================================================
-- SUMMARY REPORT 2: By Current Vendor/Agency
-- =====================================================
SELECT 
    current_placement_type,
    current_vendor_name,
    
    COUNT(DISTINCT fbbid) AS account_count,
    SUM(co_balance) AS charged_off_balance,
    SUM(current_balance) AS current_outstanding,
    SUM(total_recovered_post_co) AS total_recovered,
    SUM(remaining_balance_to_recover) AS remaining_to_recover,
    
    ROUND(SUM(total_recovered_post_co) / NULLIF(SUM(co_balance), 0) * 100, 2) AS recovery_rate_pct,
    
    ROUND(AVG(months_with_agencies), 1) AS avg_months_with_agencies,
    ROUND(AVG(months_since_charge_off), 1) AS avg_months_since_co
    
FROM analytics.credit.co_lookup_2021_onwards
GROUP BY current_placement_type, current_vendor_name
ORDER BY current_placement_type, account_count DESC;


-- =====================================================
-- SUMMARY REPORT 3: Agency Placement Summary
-- =====================================================
SELECT 
    co_year,
    
    COUNT(DISTINCT fbbid) AS total_accounts,
    
    COUNT(DISTINCT CASE WHEN ever_placed_with_agency_flag = 0 THEN fbbid END) AS never_placed_to_agency,
    COUNT(DISTINCT CASE WHEN ever_placed_with_agency_flag = 1 THEN fbbid END) AS ever_placed_to_agency,
    
    COUNT(DISTINCT CASE WHEN is_currently_with_agency = 1 THEN fbbid END) AS currently_with_agency,
    
    SUM(CASE WHEN ever_placed_with_agency_flag = 0 THEN co_balance END) AS never_placed_balance,
    SUM(CASE WHEN ever_placed_with_agency_flag = 1 THEN co_balance END) AS ever_placed_balance,
    
    SUM(CASE WHEN ever_placed_with_agency_flag = 0 THEN total_recovered_post_co END) AS internal_only_recovered,
    SUM(CASE WHEN ever_placed_with_agency_flag = 1 THEN total_recovered_post_co END) AS agency_touched_recovered,
    
    SUM(CASE WHEN is_currently_with_agency = 1 THEN remaining_balance_to_recover END) AS currently_with_agency_remaining,
    
    ROUND(AVG(CASE WHEN ever_placed_with_agency_flag = 1 THEN total_days_with_agencies END), 0) AS avg_days_with_agencies

FROM analytics.credit.co_lookup_2021_onwards
GROUP BY co_year
ORDER BY co_year;


-- =====================================================
-- SUMMARY REPORT 4: Settlement & Sold Debt Summary
-- =====================================================
SELECT 
    co_year,
    
    COUNT(DISTINCT fbbid) AS total_accounts,
    SUM(co_balance) AS total_co_balance,
    
    -- Sold Debt (SCJ)
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 1 THEN fbbid END) AS sold_debt_accounts,
    SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance END) AS sold_debt_balance,
    
    -- Settlement FUNDED
    COUNT(DISTINCT CASE WHEN is_settlement_funded_flag = 1 THEN fbbid END) AS settled_funded_accounts,
    SUM(CASE WHEN is_settlement_funded_flag = 1 THEN co_balance END) AS settled_funded_balance,
    
    -- Settlement ACTIVE
    COUNT(DISTINCT CASE WHEN is_settlement_active_flag = 1 THEN fbbid END) AS settlement_active_accounts,
    SUM(CASE WHEN is_settlement_active_flag = 1 THEN co_balance END) AS settlement_active_balance,
    
    -- Combined: Funded OR Active
    COUNT(DISTINCT CASE WHEN is_settled_or_in_settlement_flag = 1 THEN fbbid END) AS funded_or_active_accounts,
    SUM(CASE WHEN is_settled_or_in_settlement_flag = 1 THEN co_balance END) AS funded_or_active_balance,
    
    -- On Books (Collectible - excludes sold & funded)
    COUNT(DISTINCT CASE WHEN is_on_books_collectible_flag = 1 THEN fbbid END) AS on_books_accounts,
    SUM(CASE WHEN is_on_books_collectible_flag = 1 THEN co_balance END) AS on_books_co_balance,
    SUM(CASE WHEN is_on_books_collectible_flag = 1 THEN remaining_balance_to_recover END) AS on_books_remaining_balance,
    
    -- Percentages
    ROUND(SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance END) / NULLIF(SUM(co_balance), 0) * 100, 2) AS pct_sold,
    ROUND(SUM(CASE WHEN is_settlement_funded_flag = 1 THEN co_balance END) / NULLIF(SUM(co_balance), 0) * 100, 2) AS pct_settled_funded,
    ROUND(SUM(CASE WHEN is_on_books_collectible_flag = 1 THEN remaining_balance_to_recover END) / NULLIF(SUM(co_balance), 0) * 100, 2) AS pct_still_collectible

FROM analytics.credit.co_lookup_2021_onwards
GROUP BY co_year
ORDER BY co_year;


-- =====================================================
-- SUMMARY REPORT 5: Last 5 Year Charged Off Summary
-- (Aligns with Collections_KM is_within_last_5_yrs logic)
-- =====================================================
SELECT 
    'Last 5 Year CO' AS summary,
    
    COUNT(DISTINCT fbbid) AS total_accounts,
    SUM(co_principal) AS total_co_principal,
    SUM(co_balance) AS total_co_balance,
    SUM(current_outstanding_principal_today) AS current_os_principal_today,
    
    -- By Placement Type
    COUNT(DISTINCT CASE WHEN current_placement_type = 'Internal' THEN fbbid END) AS internal_accounts,
    SUM(CASE WHEN current_placement_type = 'Internal' THEN current_outstanding_principal_today END) AS internal_current_os,
    
    COUNT(DISTINCT CASE WHEN current_placement_type = 'External' THEN fbbid END) AS external_accounts,
    SUM(CASE WHEN current_placement_type = 'External' THEN current_outstanding_principal_today END) AS external_current_os,
    
    -- Recovery
    SUM(total_recovered_post_co) AS total_recovered,
    SUM(internal_recovery_amount) AS internal_recovered,
    SUM(external_recovery_amount) AS external_recovered,
    
    -- Exclusions
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 1 THEN fbbid END) AS sold_accounts,
    SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance END) AS sold_balance,
    
    COUNT(DISTINCT CASE WHEN is_settlement_funded_flag = 1 THEN fbbid END) AS settled_accounts,
    SUM(CASE WHEN is_settlement_funded_flag = 1 THEN co_balance END) AS settled_balance,
    
    -- On Books (Collectible)
    COUNT(DISTINCT CASE WHEN is_on_books_collectible_flag = 1 THEN fbbid END) AS on_books_accounts,
    SUM(CASE WHEN is_on_books_collectible_flag = 1 THEN current_outstanding_principal_today END) AS on_books_current_os

FROM analytics.credit.co_lookup_2021_onwards
WHERE is_last_5_year_co_flag = 1;


-- =====================================================
-- SUMMARY REPORT 5b: Last 5 Year CO by Placement & Vendor
-- (For comparison with Collections_KM weekly_snapshot)
-- =====================================================
SELECT 
    current_placement_type,
    current_vendor_name,
    
    COUNT(DISTINCT fbbid) AS account_count,
    SUM(co_principal) AS co_principal,
    SUM(current_outstanding_principal_today) AS current_os_today,
    SUM(total_recovered_post_co) AS total_recovered,
    
    ROUND(SUM(total_recovered_post_co) / NULLIF(SUM(co_principal), 0) * 100, 2) AS recovery_rate_pct

FROM analytics.credit.co_lookup_2021_onwards
WHERE is_last_5_year_co_flag = 1
GROUP BY current_placement_type, current_vendor_name
ORDER BY current_placement_type, current_os_today DESC;


-- =====================================================
-- SUMMARY REPORT 6: Detailed Agency Time Analysis
-- =====================================================
SELECT 
    current_vendor_name,
    
    CASE 
        WHEN months_with_agencies = 0 THEN '0. Never with Agency'
        WHEN months_with_agencies <= 3 THEN '1. 0-3 months'
        WHEN months_with_agencies <= 6 THEN '2. 3-6 months'
        WHEN months_with_agencies <= 12 THEN '3. 6-12 months'
        WHEN months_with_agencies <= 24 THEN '4. 12-24 months'
        ELSE '5. 24+ months'
    END AS agency_tenure_bucket,
    
    COUNT(DISTINCT fbbid) AS account_count,
    SUM(co_balance) AS charged_off_balance,
    SUM(total_recovered_post_co) AS total_recovered,
    SUM(remaining_balance_to_recover) AS remaining_to_recover,
    ROUND(SUM(total_recovered_post_co) / NULLIF(SUM(co_balance), 0) * 100, 2) AS recovery_rate_pct

FROM analytics.credit.co_lookup_2021_onwards
WHERE current_placement_type = 'External'
GROUP BY current_vendor_name, agency_tenure_bucket
ORDER BY current_vendor_name, agency_tenure_bucket;


-- =====================================================
-- QUICK LOOKUP: Single Account Search (uncomment and modify fbbid)
-- =====================================================
-- SELECT * FROM analytics.credit.co_lookup_2021_onwards WHERE fbbid = <your_fbbid>;


-- =====================================================
-- COLLECTIBLE BALANCE ANALYSIS: 2022+ Charge-Offs Still on Books
-- Excludes: Sold Debt (SCJ) and Settled Debt (FUNDED)
-- Purpose: 5-year liquidation runway analysis
-- =====================================================

WITH first_table AS (
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

co_base_raw AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.charge_off_date,
        fmd.outstanding_principal_due AS co_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 1
      AND fmd.charge_off_date >= '2022-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fmd.loan_key ORDER BY fmd.edate ASC) = 1
),

co_base AS (
    SELECT 
        fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(co_principal) AS co_principal,
        SUM(co_principal) AS co_balance
    FROM co_base_raw
    GROUP BY fbbid
),

current_status AS (
    SELECT 
        fbbid,
        recovery_suggested_state,
        recovery_suggested_substate,
        outstanding_principal AS current_principal,
        (outstanding_principal + fees_due - discount_pending) AS current_balance
    FROM bi.public.daily_approved_customers_data
    WHERE edate = CURRENT_DATE - 1
      AND date(CHARGEOFF_TIME) >= '2022-01-01'
),

settlements AS (
    SELECT 
        fbbid,
        current_status AS settlement_status,
        settlement_created_date,
        settlement_end_time
    FROM analytics.credit.cjk_v_backy_settlements
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY event_time DESC) = 1
),

post_co_payments AS (
    SELECT 
        p.fbbid,
        SUM(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)) AS total_recovered_post_co
    FROM bi.finance.payments_model p
    INNER JOIN co_base c ON p.fbbid = c.fbbid
    WHERE p.payment_status = 'FUND' 
      AND p.parent_payment_id IS NOT NULL
      AND date(p.payment_event_time) >= c.charge_off_date
    GROUP BY p.fbbid
),

collectible_analysis AS (
    SELECT 
        co.fbbid,
        co.charge_off_date,
        YEAR(co.charge_off_date) AS co_year,
        YEAR(co.charge_off_date) || '-Q' || QUARTER(co.charge_off_date) AS co_quarter,
        
        co.co_balance,
        COALESCE(cs.current_balance, co.co_balance) AS current_balance,
        
        cs.recovery_suggested_state AS current_state,
        cs.recovery_suggested_substate AS current_substate,
        
        CASE 
            WHEN cs.recovery_suggested_substate = '3RD_P_SOLD' THEN 1 ELSE 0 
        END AS is_sold_debt_flag,
        
        CASE 
            WHEN s.settlement_status = 'FUNDED' THEN 1 ELSE 0 
        END AS is_settled_funded_flag,
        
        CASE 
            WHEN s.settlement_status = 'ACTIVE' THEN 1 ELSE 0 
        END AS is_settlement_active_flag,
        
        s.settlement_status,
        s.settlement_created_date,
        
        COALESCE(pmt.total_recovered_post_co, 0) AS total_recovered_post_co,
        
        CASE 
            WHEN cs.recovery_suggested_substate = '3RD_P_SOLD' THEN 'Sold (SCJ)'
            WHEN s.settlement_status = 'FUNDED' THEN 'Settled (FUNDED)'
            WHEN s.settlement_status = 'ACTIVE' THEN 'Settlement Active'
            ELSE 'On Books - Collectible'
        END AS account_status,
        
        CASE 
            WHEN cs.recovery_suggested_state IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR cs.recovery_suggested_state IS NULL THEN 'Internal'
            WHEN cs.recovery_suggested_state IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External/Agency'
            ELSE 'Unknown'
        END AS placement_type,
        
        CASE 
            WHEN cs.recovery_suggested_substate IN ('3RD_P_SOLD') THEN 'SCJ'
            WHEN cs.recovery_suggested_substate IN ('ASPIRE_LAW') THEN 'ASPIRE_LAW'
            WHEN cs.recovery_suggested_substate IN ('BK_BL') THEN 'BK_BL'
            WHEN cs.recovery_suggested_substate IN ('EVANS_MUL') THEN 'EVANS_MUL' 
            WHEN cs.recovery_suggested_substate IN ('LP_HARVEST') THEN 'Harvest'
            WHEN cs.recovery_suggested_substate IN ('LP_WELTMAN') THEN 'Weltman'
            WHEN cs.recovery_suggested_substate IN ('MRS_PRIM', 'MRS_SEC') THEN 'MRS'
            WHEN cs.recovery_suggested_substate IN ('PB_CAP_PR', 'PB_CAPITAL') THEN 'PB_Capital'
            WHEN cs.recovery_suggested_substate IN ('SEQ_PRIM', 'SEQ_SEC') THEN 'SEQ'
            ELSE 'N/A'
        END AS current_vendor,
        
        co.co_balance - COALESCE(pmt.total_recovered_post_co, 0) AS remaining_balance
        
    FROM co_base co
    LEFT JOIN current_status cs ON co.fbbid = cs.fbbid
    LEFT JOIN settlements s ON co.fbbid = s.fbbid
    LEFT JOIN post_co_payments pmt ON co.fbbid = pmt.fbbid
)

-- =====================================================
-- MAIN OUTPUT: Collectible Balance Summary by Year
-- =====================================================
SELECT 
    co_year,
    
    COUNT(DISTINCT fbbid) AS total_co_accounts,
    SUM(co_balance) AS total_co_balance,
    
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 1 THEN fbbid END) AS sold_accounts,
    SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance END) AS sold_balance,
    
    COUNT(DISTINCT CASE WHEN is_settled_funded_flag = 1 THEN fbbid END) AS settled_funded_accounts,
    SUM(CASE WHEN is_settled_funded_flag = 1 THEN co_balance END) AS settled_funded_balance,
    
    COUNT(DISTINCT CASE WHEN is_settlement_active_flag = 1 THEN fbbid END) AS settlement_active_accounts,
    SUM(CASE WHEN is_settlement_active_flag = 1 THEN co_balance END) AS settlement_active_balance,
    
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN fbbid END) AS on_books_accounts,
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN co_balance END) AS on_books_co_balance,
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN remaining_balance END) AS on_books_remaining_balance,
    
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN total_recovered_post_co END) AS on_books_recovered,
    
    ROUND(SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN remaining_balance END) / 
          NULLIF(SUM(co_balance), 0) * 100, 2) AS pct_remaining_collectible

FROM collectible_analysis
GROUP BY co_year
ORDER BY co_year;


-- =====================================================
-- DETAILED: Collectible Balance by Placement Type
-- (Only accounts still on books - excludes sold & settled)
-- =====================================================
SELECT 
    co_year,
    placement_type,
    current_vendor,
    account_status,
    
    COUNT(DISTINCT fbbid) AS account_count,
    SUM(co_balance) AS charged_off_balance,
    SUM(total_recovered_post_co) AS recovered_post_co,
    SUM(remaining_balance) AS remaining_to_collect,
    ROUND(SUM(total_recovered_post_co) / NULLIF(SUM(co_balance), 0) * 100, 2) AS recovery_rate_pct

FROM collectible_analysis
WHERE is_sold_debt_flag = 0 
  AND is_settled_funded_flag = 0
GROUP BY co_year, placement_type, current_vendor, account_status
ORDER BY co_year, placement_type, remaining_to_collect DESC;


-- =====================================================
-- GRAND TOTAL: 2022+ Collectible Balance Summary
-- =====================================================
SELECT 
    'TOTAL 2022+' AS summary,
    
    COUNT(DISTINCT fbbid) AS total_accounts,
    SUM(co_balance) AS total_charged_off,
    
    SUM(CASE WHEN is_sold_debt_flag = 1 THEN co_balance ELSE 0 END) AS sold_debt_excluded,
    SUM(CASE WHEN is_settled_funded_flag = 1 THEN co_balance ELSE 0 END) AS settled_debt_excluded,
    
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN co_balance ELSE 0 END) AS on_books_original_balance,
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN total_recovered_post_co ELSE 0 END) AS already_recovered,
    SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN remaining_balance ELSE 0 END) AS still_collectible_balance,
    
    COUNT(DISTINCT CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN fbbid END) AS collectible_accounts,
    
    ROUND(SUM(CASE WHEN is_sold_debt_flag = 0 AND is_settled_funded_flag = 0 THEN remaining_balance ELSE 0 END) / 
          NULLIF(SUM(co_balance), 0) * 100, 2) AS pct_still_collectible

FROM collectible_analysis;


-- =====================================================
-- ACCOUNT-LEVEL DETAIL: Collectible Accounts (On Books)
-- =====================================================
SELECT 
    fbbid,
    charge_off_date,
    co_year,
    co_quarter,
    co_balance AS original_co_balance,
    total_recovered_post_co,
    remaining_balance AS still_to_collect,
    placement_type,
    current_vendor,
    current_state,
    account_status,
    settlement_status,
    settlement_created_date
FROM collectible_analysis
WHERE is_sold_debt_flag = 0 
  AND is_settled_funded_flag = 0
ORDER BY co_year, remaining_balance DESC;


-- =====================================================
-- =====================================================
-- MOB-LEVEL RECOVERY CURVE ANALYSIS
-- =====================================================
-- =====================================================

-- Create MOB-level recovery table for curve analysis
CREATE OR REPLACE TABLE analytics.credit.co_mob_recovery_curve AS
WITH first_table AS (
    SELECT 
        loan_key,
        loan_operational_status
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE edate >= '2020-12-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_key ORDER BY edate DESC) = 1
),

charge_off_balances_raw AS (
    SELECT 
        fmd.fbbid,
        fmd.loan_key,
        fmd.charge_off_date,
        fmd.outstanding_principal_due AS co_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    LEFT JOIN first_table ft ON fmd.loan_key = ft.loan_key
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
      AND (ft.loan_operational_status <> 'CNCL' OR ft.loan_operational_status IS NULL)
      AND fmd.is_charged_off = 1
      AND fmd.charge_off_date >= '2021-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fmd.loan_key ORDER BY fmd.edate ASC) = 1
),

charge_off_balances AS (
    SELECT 
        fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(co_principal) AS co_balance
    FROM charge_off_balances_raw
    GROUP BY fbbid
),

status_history AS (
    SELECT 
        fbbid,
        edate,
        recovery_suggested_state,
        recovery_suggested_substate,
        CASE 
            WHEN recovery_suggested_state = LAG(recovery_suggested_state) OVER (PARTITION BY fbbid ORDER BY edate ASC)
                 AND (recovery_suggested_substate = LAG(recovery_suggested_substate) OVER (PARTITION BY fbbid ORDER BY edate ASC)
                      OR (recovery_suggested_substate IS NULL AND LAG(recovery_suggested_substate) OVER (PARTITION BY fbbid ORDER BY edate ASC) IS NULL))
            THEN 0 ELSE 1 
        END AS is_new_transition
    FROM bi.public.daily_approved_customers_data 
    WHERE date(CHARGEOFF_TIME) IS NOT NULL
      AND date(CHARGEOFF_TIME) >= '2021-01-01'
),

state_transitions AS (
    SELECT 
        fbbid,
        edate AS transfer_date,
        recovery_suggested_state,
        LEAD(edate, 1, CURRENT_DATE) OVER (PARTITION BY fbbid ORDER BY edate) AS next_transfer_date
    FROM status_history
    WHERE is_new_transition = 1 
      AND ((recovery_suggested_state = 'ELR' AND recovery_suggested_substate IS NOT NULL) 
           OR recovery_suggested_state <> 'ELR')
),

state_with_placement AS (
    SELECT 
        fbbid,
        transfer_date,
        next_transfer_date,
        CASE 
            WHEN recovery_suggested_state IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR recovery_suggested_state IS NULL THEN 'Internal'
            WHEN recovery_suggested_state IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_status_at_time
    FROM state_transitions
),

mob_payments AS (
    SELECT 
        p.fbbid,
        c.charge_off_date,
        YEAR(c.charge_off_date) AS co_year,
        YEAR(c.charge_off_date) || '-Q' || QUARTER(c.charge_off_date) AS co_quarter,
        LAST_DAY(c.charge_off_date) AS co_month,
        c.co_balance,
        date(p.payment_event_time) AS payment_date,
        LAST_DAY(date(p.payment_event_time)) AS payment_month,
        TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) AS payment_amount,
        s.placement_status_at_time,
        FLOOR(DATEDIFF('day', c.charge_off_date, date(p.payment_event_time)) / 30) AS mob_co
    FROM bi.finance.payments_model p
    INNER JOIN charge_off_balances c ON p.fbbid = c.fbbid
    LEFT JOIN state_with_placement s 
        ON p.fbbid = s.fbbid 
        AND date(p.payment_event_time) > s.transfer_date 
        AND date(p.payment_event_time) <= s.next_transfer_date
    WHERE p.payment_status = 'FUND' 
      AND p.parent_payment_id IS NOT NULL
      AND date(p.payment_event_time) >= c.charge_off_date
)

SELECT 
    fbbid,
    charge_off_date,
    co_year,
    co_quarter,
    co_month,
    co_balance,
    payment_date,
    payment_month,
    mob_co,
    placement_status_at_time,
    payment_amount,
    SUM(payment_amount) OVER (PARTITION BY fbbid ORDER BY payment_date ROWS UNBOUNDED PRECEDING) AS cumulative_recovery,
    ROUND(SUM(payment_amount) OVER (PARTITION BY fbbid ORDER BY payment_date ROWS UNBOUNDED PRECEDING) / NULLIF(co_balance, 0) * 100, 2) AS cumulative_recovery_rate_pct
FROM mob_payments;


-- =====================================================
-- MOB RECOVERY CURVE: By CO Year and MOB
-- =====================================================
SELECT 
    co_year,
    mob_co,
    
    COUNT(DISTINCT fbbid) AS accounts_with_payment,
    SUM(co_balance) AS cohort_co_balance,
    SUM(payment_amount) AS recovery_at_mob,
    
    SUM(CASE WHEN placement_status_at_time = 'Internal' OR placement_status_at_time IS NULL THEN payment_amount ELSE 0 END) AS internal_recovery_at_mob,
    SUM(CASE WHEN placement_status_at_time = 'External' THEN payment_amount ELSE 0 END) AS external_recovery_at_mob,
    
    ROUND(SUM(payment_amount) / NULLIF(SUM(co_balance), 0) * 100, 4) AS recovery_rate_at_mob_pct

FROM analytics.credit.co_mob_recovery_curve
WHERE co_year >= 2021
GROUP BY co_year, mob_co
ORDER BY co_year, mob_co;


-- =====================================================
-- MOB RECOVERY CURVE: By CO Quarter and MOB (More Granular)
-- =====================================================
SELECT 
    co_year,
    co_quarter,
    mob_co,
    
    COUNT(DISTINCT fbbid) AS accounts_with_payment,
    SUM(payment_amount) AS recovery_at_mob,
    
    SUM(CASE WHEN placement_status_at_time = 'Internal' OR placement_status_at_time IS NULL THEN payment_amount ELSE 0 END) AS internal_recovery_at_mob,
    SUM(CASE WHEN placement_status_at_time = 'External' THEN payment_amount ELSE 0 END) AS external_recovery_at_mob

FROM analytics.credit.co_mob_recovery_curve
WHERE co_year >= 2021
GROUP BY co_year, co_quarter, mob_co
ORDER BY co_year, co_quarter, mob_co;


-- =====================================================
-- CUMULATIVE RECOVERY CURVE: By CO Year and MOB
-- (Shows cumulative recovery rate at each MOB)
-- =====================================================
WITH cohort_balance AS (
    SELECT 
        co_year,
        SUM(DISTINCT co_balance) AS total_cohort_balance
    FROM (
        SELECT co_year, fbbid, co_balance
        FROM analytics.credit.co_mob_recovery_curve
        GROUP BY co_year, fbbid, co_balance
    )
    GROUP BY co_year
),

mob_recovery AS (
    SELECT 
        co_year,
        mob_co,
        SUM(payment_amount) AS recovery_at_mob,
        SUM(CASE WHEN placement_status_at_time = 'Internal' OR placement_status_at_time IS NULL THEN payment_amount ELSE 0 END) AS internal_recovery_at_mob,
        SUM(CASE WHEN placement_status_at_time = 'External' THEN payment_amount ELSE 0 END) AS external_recovery_at_mob
    FROM analytics.credit.co_mob_recovery_curve
    WHERE co_year >= 2021
    GROUP BY co_year, mob_co
)

SELECT 
    m.co_year,
    m.mob_co,
    c.total_cohort_balance,
    m.recovery_at_mob,
    m.internal_recovery_at_mob,
    m.external_recovery_at_mob,
    SUM(m.recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) AS cumulative_recovery,
    SUM(m.internal_recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) AS cumulative_internal_recovery,
    SUM(m.external_recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) AS cumulative_external_recovery,
    ROUND(SUM(m.recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) / NULLIF(c.total_cohort_balance, 0) * 100, 2) AS cumulative_recovery_rate_pct,
    ROUND(SUM(m.internal_recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) / NULLIF(c.total_cohort_balance, 0) * 100, 2) AS cumulative_internal_rate_pct,
    ROUND(SUM(m.external_recovery_at_mob) OVER (PARTITION BY m.co_year ORDER BY m.mob_co ROWS UNBOUNDED PRECEDING) / NULLIF(c.total_cohort_balance, 0) * 100, 2) AS cumulative_external_rate_pct
FROM mob_recovery m
JOIN cohort_balance c ON m.co_year = c.co_year
ORDER BY m.co_year, m.mob_co;


-- =====================================================
-- PAYMENT MONTH ANALYSIS: Recovery by Payment Month
-- =====================================================
SELECT 
    co_year,
    payment_month,
    mob_co,
    placement_status_at_time,
    
    COUNT(*) AS payment_count,
    COUNT(DISTINCT fbbid) AS unique_accounts,
    SUM(payment_amount) AS total_recovery

FROM analytics.credit.co_mob_recovery_curve
WHERE co_year >= 2021
GROUP BY co_year, payment_month, mob_co, placement_status_at_time
ORDER BY co_year, payment_month, mob_co;
