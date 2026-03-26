-- =====================================================
-- SETTLEMENT ANALYSIS FRAMEWORK
-- =====================================================
-- Comprehensive analysis of Fundbox Settlement Portfolio
-- Scope: Pre-CO (DPD 1-90) and Post-CO Settlements from 2022 onwards
-- 
-- Framework Components:
-- 1. Settlement Portfolio Base (with all dimensions)
-- 2. Settlement Conversion Funnel (Offer → Active → Funded/Failed)
-- 3. Settlement Velocity & Payment Behavior
-- 4. Recovery Rate & Discount (Haircut) Analysis
-- 5. Vintage Cohort Analysis (by CO quarter, MOB, DPD bucket)
-- 6. Agent Settlement Performance Attribution
-- 7. Portfolio Health & Liquidity Risk Metrics
-- =====================================================


-- =====================================================
-- VIEW 1: SETTLEMENT PORTFOLIO BASE
-- =====================================================
-- Master settlement table with all cohort dimensions
-- Source of truth for all downstream analysis

CREATE OR REPLACE VIEW analytics.credit.v_settlement_portfolio_base AS

WITH 
-- Settlement base data
settlements_raw AS (
    SELECT 
        fbbid,
        current_status AS settlement_status,
        settlement_created_date,
        settlement_end_time,
        event_time,
        COALESCE(CORRECTED_CREATED_TIME, settlement_created_date) AS corrected_created_date,
        ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY event_time DESC) AS rn
    FROM analytics.credit.cjk_v_backy_settlements
    WHERE settlement_created_date >= '2022-01-01'
),

settlements AS (
    SELECT * FROM settlements_raw WHERE rn = 1
),

-- Charge-off data for Post-CO context
charge_off_data AS (
    SELECT 
        fbbid,
        MIN(charge_off_date) AS charge_off_date,
        SUM(outstanding_principal_due) AS co_principal
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE is_charged_off = 1
      AND product_type <> 'Flexpay'
      AND ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
    GROUP BY fbbid
),

-- Customer state at settlement creation
customer_state_at_settlement AS (
    SELECT 
        s.fbbid,
        s.settlement_created_date,
        dacd.recovery_suggested_state,
        dacd.recovery_suggested_substate,
        dacd.outstanding_principal,
        dacd.fees_due,
        dacd.discount_pending,
        (dacd.outstanding_principal + dacd.fees_due - COALESCE(dacd.discount_pending, 0)) AS balance_at_settlement,
        CASE 
            WHEN dacd.recovery_suggested_state IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR dacd.recovery_suggested_state IS NULL THEN 'Internal'
            WHEN dacd.recovery_suggested_state IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_type_at_settlement
    FROM settlements s
    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON s.fbbid = dacd.fbbid
        AND dacd.edate = s.settlement_created_date
),

-- DPD at settlement creation (for Pre-CO)
dpd_at_settlement AS (
    SELECT 
        fmd.fbbid,
        fmd.edate,
        MAX(CASE WHEN fmd.dpd_days IS NULL THEN 0 ELSE fmd.dpd_days END) AS dpd_days,
        MAX(fmd.is_charged_off) AS is_charged_off,
        SUM(fmd.outstanding_principal_due) AS outstanding_principal_at_date
    FROM bi.finance.finance_metrics_daily fmd
    WHERE fmd.product_type <> 'Flexpay'
      AND fmd.ORIGINAL_PAYMENT_PLAN_DESCRIPTION NOT LIKE '%Term Loan%'
    GROUP BY fmd.fbbid, fmd.edate
),

-- Payments associated with settlements
settlement_payments AS (
    SELECT 
        p.fbbid,
        SUM(CASE WHEN p.payment_status = 'FUND' 
            AND date(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE)
            THEN TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) ELSE 0 END) AS settlement_payment_amount,
        COUNT(CASE WHEN p.payment_status = 'FUND' 
            AND date(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE)
            THEN 1 END) AS settlement_payment_count,
        MIN(CASE WHEN p.payment_status = 'FUND' 
            AND date(p.payment_event_time) >= s.settlement_created_date
            THEN date(p.payment_event_time) END) AS first_settlement_payment_date,
        MAX(CASE WHEN p.payment_status = 'FUND' 
            AND date(p.payment_event_time) BETWEEN s.settlement_created_date AND COALESCE(s.settlement_end_time, CURRENT_DATE)
            THEN date(p.payment_event_time) END) AS last_settlement_payment_date
    FROM bi.finance.payments_model p
    INNER JOIN settlements s ON p.fbbid = s.fbbid
    WHERE p.parent_payment_id IS NOT NULL
    GROUP BY p.fbbid
),

-- Customer industry/segment from approved customers
customer_attributes AS (
    SELECT DISTINCT
        fbbid,
        FIRST_VALUE(industry) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS industry,
        FIRST_VALUE(state) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS customer_state,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY fbbid ORDER BY edate DESC) AS credit_limit
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
)

SELECT 
    -- Settlement identifiers
    s.fbbid,
    s.settlement_status,
    s.settlement_created_date,
    s.settlement_end_time,
    s.corrected_created_date,
    
    -- Time dimensions
    DATE_TRUNC('month', s.settlement_created_date) AS settlement_month,
    DATE_TRUNC('quarter', s.settlement_created_date) AS settlement_quarter,
    YEAR(s.settlement_created_date) AS settlement_year,
    
    -- Settlement lifecycle
    CASE 
        WHEN s.settlement_status = 'FUNDED' THEN 'Completed'
        WHEN s.settlement_status = 'ACTIVE' THEN 'In Progress'
        WHEN s.settlement_status IN ('FAILED', 'CANCELLED') THEN 'Failed/Cancelled'
        ELSE 'Other'
    END AS settlement_lifecycle_stage,
    
    DATEDIFF('day', s.settlement_created_date, COALESCE(s.settlement_end_time, CURRENT_DATE)) AS days_in_settlement,
    
    -- Pre-CO vs Post-CO classification
    CASE 
        WHEN co.charge_off_date IS NULL THEN 'Pre-CO'
        WHEN s.settlement_created_date < co.charge_off_date THEN 'Pre-CO'
        ELSE 'Post-CO'
    END AS settlement_type,
    
    -- Charge-off context (for Post-CO)
    co.charge_off_date,
    LAST_DAY(co.charge_off_date) AS co_month,
    YEAR(co.charge_off_date) || '-Q' || QUARTER(co.charge_off_date) AS co_quarter,
    co.co_principal,
    DATEDIFF('day', co.charge_off_date, s.settlement_created_date) AS days_since_co_at_settlement,
    FLOOR(DATEDIFF('day', co.charge_off_date, s.settlement_created_date) / 30) AS mob_at_settlement,
    
    -- DPD context (for Pre-CO)
    dpd.dpd_days AS dpd_at_settlement,
    dpd.is_charged_off AS was_charged_off_at_settlement,
    CASE 
        WHEN dpd.dpd_days BETWEEN 1 AND 14 THEN '1-2 (DPD 1-14)'
        WHEN dpd.dpd_days BETWEEN 15 AND 56 THEN '3-8 (DPD 15-56)'
        WHEN dpd.dpd_days BETWEEN 57 AND 91 THEN '9-13 (DPD 57-91)'
        WHEN dpd.is_charged_off = 1 OR dpd.dpd_days > 91 THEN 'Charged Off'
        WHEN dpd.dpd_days = 0 OR dpd.dpd_days IS NULL THEN 'Current'
        ELSE 'Unknown'
    END AS dpd_bucket_at_settlement,
    
    -- Balance dimensions
    COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) AS balance_at_settlement,
    cs.outstanding_principal AS principal_at_settlement,
    cs.fees_due AS fees_at_settlement,
    cs.discount_pending AS discount_at_settlement,
    
    -- Balance tier
    CASE 
        WHEN COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) < 5000 THEN 'Small (<$5K)'
        WHEN COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) < 25000 THEN 'Medium ($5K-$25K)'
        WHEN COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) < 100000 THEN 'Large ($25K-$100K)'
        ELSE 'Enterprise ($100K+)'
    END AS balance_tier,
    
    -- Placement type
    COALESCE(cs.placement_type_at_settlement, 'Unknown') AS placement_type_at_settlement,
    cs.recovery_suggested_state AS recovery_state_at_settlement,
    cs.recovery_suggested_substate AS recovery_substate_at_settlement,
    
    -- Payment metrics
    COALESCE(sp.settlement_payment_amount, 0) AS settlement_payment_amount,
    COALESCE(sp.settlement_payment_count, 0) AS settlement_payment_count,
    sp.first_settlement_payment_date,
    sp.last_settlement_payment_date,
    DATEDIFF('day', s.settlement_created_date, sp.first_settlement_payment_date) AS days_to_first_payment,
    
    -- Recovery rate calculation
    ROUND(COALESCE(sp.settlement_payment_amount, 0) / NULLIF(COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal), 0) * 100, 2) AS recovery_rate_pct,
    
    -- Discount (haircut) analysis
    COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) - COALESCE(sp.settlement_payment_amount, 0) AS discount_amount,
    ROUND((COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal) - COALESCE(sp.settlement_payment_amount, 0)) / 
          NULLIF(COALESCE(cs.balance_at_settlement, dpd.outstanding_principal_at_date, co.co_principal), 0) * 100, 2) AS discount_pct,
    
    -- Customer attributes
    ca.industry,
    ca.customer_state AS geography,
    ca.credit_limit,
    
    -- Time on platform
    top.first_draw_date,
    DATEDIFF('month', top.first_draw_date, s.settlement_created_date) AS months_on_platform_at_settlement

FROM settlements s
LEFT JOIN charge_off_data co ON s.fbbid = co.fbbid
LEFT JOIN customer_state_at_settlement cs ON s.fbbid = cs.fbbid
LEFT JOIN dpd_at_settlement dpd ON s.fbbid = dpd.fbbid AND dpd.edate = s.settlement_created_date
LEFT JOIN settlement_payments sp ON s.fbbid = sp.fbbid
LEFT JOIN customer_attributes ca ON s.fbbid = ca.fbbid
LEFT JOIN time_on_platform top ON s.fbbid = top.fbbid;


-- =====================================================
-- VIEW 2: SETTLEMENT CONVERSION FUNNEL
-- =====================================================
-- Tracks settlement flow: Created → Active → Funded/Failed
-- Monthly cohort analysis of conversion rates

CREATE OR REPLACE VIEW analytics.credit.v_settlement_conversion_funnel AS

WITH monthly_settlements AS (
    SELECT 
        settlement_month,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        dpd_bucket_at_settlement,
        
        COUNT(DISTINCT fbbid) AS total_settlements_created,
        
        -- By status
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' THEN fbbid END) AS active_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') THEN fbbid END) AS failed_cancelled_settlements,
        
        -- Balance metrics
        SUM(balance_at_settlement) AS total_balance_at_settlement,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN balance_at_settlement END) AS funded_balance,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered_funded,
        
        -- Avg metrics
        AVG(days_in_settlement) AS avg_days_in_settlement,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate_funded,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END) AS avg_discount_pct_funded
        
    FROM analytics.credit.v_settlement_portfolio_base
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    settlement_month,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    dpd_bucket_at_settlement,
    
    total_settlements_created,
    active_settlements,
    funded_settlements,
    failed_cancelled_settlements,
    
    -- Conversion rates
    ROUND(funded_settlements * 100.0 / NULLIF(total_settlements_created, 0), 2) AS funding_rate_pct,
    ROUND(failed_cancelled_settlements * 100.0 / NULLIF(total_settlements_created, 0), 2) AS failure_rate_pct,
    ROUND(active_settlements * 100.0 / NULLIF(total_settlements_created, 0), 2) AS active_rate_pct,
    
    -- Balance metrics
    total_balance_at_settlement,
    funded_balance,
    total_recovered_funded,
    
    -- Recovery metrics
    ROUND(total_recovered_funded / NULLIF(funded_balance, 0) * 100, 2) AS cohort_recovery_rate_pct,
    ROUND(avg_days_in_settlement, 1) AS avg_days_in_settlement,
    ROUND(avg_days_to_fund, 1) AS avg_days_to_fund,
    ROUND(avg_recovery_rate_funded, 2) AS avg_recovery_rate_funded,
    ROUND(avg_discount_pct_funded, 2) AS avg_discount_pct_funded,
    
    -- Funnel efficiency ($ recovered per $ in pipeline)
    ROUND(total_recovered_funded / NULLIF(total_balance_at_settlement, 0) * 100, 2) AS funnel_efficiency_pct

FROM monthly_settlements
ORDER BY settlement_month DESC, settlement_type, placement_type_at_settlement;


-- =====================================================
-- VIEW 3: SETTLEMENT VELOCITY & PAYMENT BEHAVIOR
-- =====================================================
-- Analyzes time-to-fund and payment patterns

CREATE OR REPLACE VIEW analytics.credit.v_settlement_velocity AS

WITH velocity_metrics AS (
    SELECT 
        settlement_month,
        settlement_type,
        placement_type_at_settlement,
        balance_tier,
        mob_at_settlement,
        
        -- Time metrics (only for funded)
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded_count,
        
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund,
        MEDIAN(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS median_days_to_fund,
        MIN(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS min_days_to_fund,
        MAX(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS max_days_to_fund,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS p25_days_to_fund,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS p75_days_to_fund,
        
        -- Days to first payment
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_to_first_payment END) AS avg_days_to_first_payment,
        
        -- Payment count distribution
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_count END) AS avg_payment_count,
        
        -- Avg payment size
        AVG(CASE WHEN settlement_status = 'FUNDED' AND settlement_payment_count > 0 
            THEN settlement_payment_amount / settlement_payment_count END) AS avg_payment_size,
        
        -- Total amounts
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN balance_at_settlement END) AS total_funded_balance

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_status = 'FUNDED'
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    settlement_month,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    mob_at_settlement,
    
    funded_count,
    
    ROUND(avg_days_to_fund, 1) AS avg_days_to_fund,
    ROUND(median_days_to_fund, 1) AS median_days_to_fund,
    ROUND(p25_days_to_fund, 1) AS p25_days_to_fund,
    ROUND(p75_days_to_fund, 1) AS p75_days_to_fund,
    min_days_to_fund,
    max_days_to_fund,
    
    ROUND(avg_days_to_first_payment, 1) AS avg_days_to_first_payment,
    ROUND(avg_payment_count, 1) AS avg_payment_count,
    ROUND(avg_payment_size, 2) AS avg_payment_size,
    
    total_recovered,
    total_funded_balance,
    ROUND(total_recovered / NULLIF(total_funded_balance, 0) * 100, 2) AS recovery_rate_pct,
    
    -- Velocity score (inverse of days to fund, normalized)
    ROUND(30.0 / NULLIF(avg_days_to_fund, 0), 2) AS velocity_score

FROM velocity_metrics
WHERE funded_count > 0
ORDER BY settlement_month DESC;


-- =====================================================
-- VIEW 4: RECOVERY RATE & DISCOUNT ANALYSIS
-- =====================================================
-- Deep dive into settlement economics and haircut analysis

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
        
        -- Settlement counts
        COUNT(DISTINCT fbbid) AS total_settlements,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded_settlements,
        
        -- Balance at settlement
        SUM(balance_at_settlement) AS total_balance,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN balance_at_settlement END) AS funded_balance,
        
        -- Recovery amounts
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered,
        
        -- Discount (haircut) amounts
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN discount_amount END) AS total_discount,
        
        -- Distribution of recovery rates
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate,
        MEDIAN(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS median_recovery_rate,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS p25_recovery_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS p75_recovery_rate,
        
        -- Distribution of discount rates
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END) AS avg_discount_pct,
        MEDIAN(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END) AS median_discount_pct

    FROM analytics.credit.v_settlement_portfolio_base
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT 
    settlement_month,
    settlement_quarter,
    settlement_type,
    placement_type_at_settlement,
    balance_tier,
    dpd_bucket_at_settlement,
    mob_at_settlement,
    
    total_settlements,
    funded_settlements,
    ROUND(funded_settlements * 100.0 / NULLIF(total_settlements, 0), 2) AS funding_rate_pct,
    
    -- Balance metrics
    total_balance,
    funded_balance,
    total_recovered,
    total_discount,
    
    -- Aggregate recovery and discount rates
    ROUND(total_recovered / NULLIF(funded_balance, 0) * 100, 2) AS aggregate_recovery_rate_pct,
    ROUND(total_discount / NULLIF(funded_balance, 0) * 100, 2) AS aggregate_discount_pct,
    
    -- Distribution metrics
    ROUND(avg_recovery_rate, 2) AS avg_recovery_rate_pct,
    ROUND(median_recovery_rate, 2) AS median_recovery_rate_pct,
    ROUND(p25_recovery_rate, 2) AS p25_recovery_rate_pct,
    ROUND(p75_recovery_rate, 2) AS p75_recovery_rate_pct,
    
    ROUND(avg_discount_pct, 2) AS avg_discount_pct,
    ROUND(median_discount_pct, 2) AS median_discount_pct,
    
    -- Unit economics: Net recovery per settlement
    ROUND(total_recovered / NULLIF(funded_settlements, 0), 2) AS avg_recovery_per_settlement,
    ROUND(total_discount / NULLIF(funded_settlements, 0), 2) AS avg_discount_per_settlement

FROM discount_analysis
WHERE total_settlements > 0
ORDER BY settlement_month DESC;


-- =====================================================
-- VIEW 5: VINTAGE COHORT ANALYSIS
-- =====================================================
-- Settlement trends by charge-off vintage and MOB

CREATE OR REPLACE VIEW analytics.credit.v_settlement_vintage_analysis AS

WITH vintage_cohorts AS (
    SELECT 
        -- Charge-off vintage (for Post-CO)
        co_quarter,
        co_month,
        
        -- MOB at settlement
        mob_at_settlement,
        
        -- Settlement month
        settlement_month,
        
        -- Counts
        COUNT(DISTINCT fbbid) AS settlements_created,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS settlements_funded,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' THEN fbbid END) AS settlements_active,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') THEN fbbid END) AS settlements_failed,
        
        -- Balances
        SUM(balance_at_settlement) AS total_balance_at_settlement,
        SUM(co_principal) AS total_co_principal,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered,
        
        -- Recovery rates
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END) AS avg_discount_pct,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund

    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_type = 'Post-CO'
      AND co_quarter IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT 
    co_quarter,
    co_month,
    mob_at_settlement,
    settlement_month,
    
    settlements_created,
    settlements_funded,
    settlements_active,
    settlements_failed,
    
    -- Conversion rates
    ROUND(settlements_funded * 100.0 / NULLIF(settlements_created, 0), 2) AS funding_rate_pct,
    
    -- Balance metrics
    total_balance_at_settlement,
    total_co_principal,
    total_recovered,
    
    -- Recovery metrics
    ROUND(total_recovered / NULLIF(total_balance_at_settlement, 0) * 100, 2) AS recovery_rate_on_settlement_balance,
    ROUND(total_recovered / NULLIF(total_co_principal, 0) * 100, 2) AS recovery_rate_on_co_principal,
    
    ROUND(avg_recovery_rate, 2) AS avg_recovery_rate_pct,
    ROUND(avg_discount_pct, 2) AS avg_discount_pct,
    ROUND(avg_days_to_fund, 1) AS avg_days_to_fund,
    
    -- Cumulative metrics (for vintage curve)
    SUM(settlements_funded) OVER (PARTITION BY co_quarter ORDER BY mob_at_settlement) AS cumulative_funded_by_mob,
    SUM(total_recovered) OVER (PARTITION BY co_quarter ORDER BY mob_at_settlement) AS cumulative_recovered_by_mob

FROM vintage_cohorts
ORDER BY co_quarter DESC, mob_at_settlement;


-- =====================================================
-- VIEW 6: AGENT SETTLEMENT PERFORMANCE
-- =====================================================
-- Attribution of settlements to agents

CREATE OR REPLACE VIEW analytics.credit.v_settlement_agent_performance AS

WITH 
-- Get agent activity with settlement accepted disposition
agent_settlement_activity AS (
    SELECT
        CASE WHEN fundbox_id__c = 'Not Linked' THEN NULL 
             ELSE TRY_TO_NUMBER(fundbox_id__c) END AS fbbid,
        ASSIGNEE_NAME__C AS agent_name,
        ROLE_ID_NAME__C AS agent_role,
        date(createddate) AS activity_date,
        calldisposition AS disposition
    FROM external_data_sources.salesforce_nova.task
    WHERE LOWER(calldisposition) = 'settlement accepted'
      AND date(createddate) >= '2022-01-01'
      AND TRY_TO_NUMBER(fundbox_id__c) IS NOT NULL
),

-- Match settlements to agents
settlement_agent_attribution AS (
    SELECT 
        s.*,
        a.agent_name,
        a.agent_role,
        a.activity_date AS settlement_accepted_date,
        ROW_NUMBER() OVER (PARTITION BY s.fbbid ORDER BY a.activity_date DESC) AS rn
    FROM analytics.credit.v_settlement_portfolio_base s
    LEFT JOIN agent_settlement_activity a 
        ON s.fbbid = a.fbbid
        AND a.activity_date BETWEEN DATEADD('day', -14, s.settlement_created_date) AND s.settlement_created_date
),

-- Agent monthly metrics
agent_monthly AS (
    SELECT 
        agent_name,
        agent_role,
        DATE_TRUNC('month', settlement_created_date) AS performance_month,
        
        -- Settlement counts
        COUNT(DISTINCT fbbid) AS settlements_attributed,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS settlements_funded,
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') THEN fbbid END) AS settlements_failed,
        
        -- Balance metrics
        SUM(balance_at_settlement) AS total_balance_attributed,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered,
        
        -- Performance metrics
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END) AS avg_discount_pct,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund
        
    FROM settlement_agent_attribution
    WHERE rn = 1 AND agent_name IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT 
    agent_name,
    agent_role,
    performance_month,
    
    settlements_attributed,
    settlements_funded,
    settlements_failed,
    
    -- Conversion rates
    ROUND(settlements_funded * 100.0 / NULLIF(settlements_attributed, 0), 2) AS funding_rate_pct,
    
    -- Balance metrics
    total_balance_attributed,
    total_recovered,
    ROUND(total_recovered / NULLIF(total_balance_attributed, 0) * 100, 2) AS recovery_efficiency_pct,
    
    -- Performance metrics
    ROUND(avg_recovery_rate, 2) AS avg_recovery_rate_pct,
    ROUND(avg_discount_pct, 2) AS avg_discount_pct,
    ROUND(avg_days_to_fund, 1) AS avg_days_to_fund,
    
    -- Productivity
    ROUND(total_recovered / NULLIF(settlements_funded, 0), 2) AS avg_recovery_per_funded_settlement,
    
    -- Rolling averages
    AVG(settlements_funded) OVER (PARTITION BY agent_name ORDER BY performance_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m_avg_funded,
    AVG(total_recovered) OVER (PARTITION BY agent_name ORDER BY performance_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m_avg_recovered

FROM agent_monthly
ORDER BY performance_month DESC, total_recovered DESC;


-- =====================================================
-- VIEW 7: PORTFOLIO HEALTH & LIQUIDITY RISK
-- =====================================================
-- Assesses capital deployment and recovery velocity

CREATE OR REPLACE VIEW analytics.credit.v_settlement_portfolio_health AS

WITH 
-- Current state of settlement portfolio
current_portfolio AS (
    SELECT 
        -- Active settlements (capital deployed, not yet recovered)
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' THEN fbbid END) AS active_settlement_count,
        SUM(CASE WHEN settlement_status = 'ACTIVE' THEN balance_at_settlement END) AS active_settlement_balance,
        AVG(CASE WHEN settlement_status = 'ACTIVE' THEN days_in_settlement END) AS avg_days_active,
        
        -- Aging analysis for active
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement <= 30 THEN fbbid END) AS active_0_30_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement BETWEEN 31 AND 60 THEN fbbid END) AS active_31_60_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement BETWEEN 61 AND 90 THEN fbbid END) AS active_61_90_days,
        COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement > 90 THEN fbbid END) AS active_90_plus_days,
        
        SUM(CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement <= 30 THEN balance_at_settlement END) AS balance_active_0_30,
        SUM(CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement BETWEEN 31 AND 60 THEN balance_at_settlement END) AS balance_active_31_60,
        SUM(CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement BETWEEN 61 AND 90 THEN balance_at_settlement END) AS balance_active_61_90,
        SUM(CASE WHEN settlement_status = 'ACTIVE' AND days_in_settlement > 90 THEN balance_at_settlement END) AS balance_active_90_plus,
        
        -- Recently funded (last 30 days) - capital recovered
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE) THEN fbbid END) AS funded_last_30d_count,
        SUM(CASE WHEN settlement_status = 'FUNDED' AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE) THEN settlement_payment_amount END) AS recovered_last_30d,
        
        -- Failed in last 30 days
        COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE) THEN fbbid END) AS failed_last_30d_count,
        SUM(CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') AND settlement_end_time >= DATEADD('day', -30, CURRENT_DATE) THEN balance_at_settlement END) AS failed_balance_last_30d
        
    FROM analytics.credit.v_settlement_portfolio_base
),

-- Historical velocity (for trend analysis)
monthly_velocity AS (
    SELECT 
        settlement_month,
        COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded_count,
        SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS recovered_amount,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END) AS avg_days_to_fund,
        AVG(CASE WHEN settlement_status = 'FUNDED' THEN recovery_rate_pct END) AS avg_recovery_rate
    FROM analytics.credit.v_settlement_portfolio_base
    WHERE settlement_month >= DATEADD('month', -12, CURRENT_DATE)
    GROUP BY 1
),

-- Calculate trends
velocity_trends AS (
    SELECT 
        AVG(funded_count) AS avg_monthly_funded,
        AVG(recovered_amount) AS avg_monthly_recovery,
        AVG(avg_days_to_fund) AS overall_avg_days_to_fund,
        AVG(avg_recovery_rate) AS overall_avg_recovery_rate,
        STDDEV(recovered_amount) AS stddev_monthly_recovery
    FROM monthly_velocity
)

SELECT 
    CURRENT_DATE AS report_date,
    
    -- Active Portfolio (Capital at Risk)
    cp.active_settlement_count,
    cp.active_settlement_balance AS capital_deployed_in_settlements,
    ROUND(cp.avg_days_active, 1) AS avg_days_in_active_settlement,
    
    -- Aging Distribution (counts)
    cp.active_0_30_days,
    cp.active_31_60_days,
    cp.active_61_90_days,
    cp.active_90_plus_days,
    
    -- Aging Distribution (balances)
    cp.balance_active_0_30,
    cp.balance_active_31_60,
    cp.balance_active_61_90,
    cp.balance_active_90_plus,
    
    -- Aging Risk Score (higher = more capital in older settlements)
    ROUND((COALESCE(cp.balance_active_31_60, 0) * 1 + 
           COALESCE(cp.balance_active_61_90, 0) * 2 + 
           COALESCE(cp.balance_active_90_plus, 0) * 3) / 
          NULLIF(cp.active_settlement_balance, 0) * 100, 2) AS aging_risk_score,
    
    -- Recent Performance (Last 30 Days)
    cp.funded_last_30d_count,
    cp.recovered_last_30d AS capital_recovered_last_30d,
    cp.failed_last_30d_count,
    cp.failed_balance_last_30d,
    
    -- Velocity Metrics
    ROUND(vt.avg_monthly_funded, 0) AS avg_monthly_funded_settlements,
    ROUND(vt.avg_monthly_recovery, 2) AS avg_monthly_recovery_amount,
    ROUND(vt.overall_avg_days_to_fund, 1) AS overall_avg_days_to_fund,
    ROUND(vt.overall_avg_recovery_rate, 2) AS overall_avg_recovery_rate_pct,
    
    -- Liquidity Risk Indicators
    ROUND(cp.active_settlement_balance / NULLIF(vt.avg_monthly_recovery, 0), 1) AS months_to_clear_active_portfolio,
    ROUND(COALESCE(cp.balance_active_90_plus, 0) / NULLIF(cp.active_settlement_balance, 0) * 100, 2) AS pct_balance_over_90_days,
    
    -- Volatility indicator
    ROUND(vt.stddev_monthly_recovery / NULLIF(vt.avg_monthly_recovery, 0) * 100, 2) AS recovery_volatility_pct

FROM current_portfolio cp
CROSS JOIN velocity_trends vt;


-- =====================================================
-- VIEW 8: INDUSTRY & GEOGRAPHY SEGMENTATION
-- =====================================================
-- Settlement performance by merchant segment

CREATE OR REPLACE VIEW analytics.credit.v_settlement_segmentation AS

SELECT 
    -- Industry
    COALESCE(industry, 'Unknown') AS industry,
    COALESCE(geography, 'Unknown') AS geography,
    settlement_type,
    balance_tier,
    
    -- Settlement counts
    COUNT(DISTINCT fbbid) AS total_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS funded_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status = 'ACTIVE' THEN fbbid END) AS active_settlements,
    COUNT(DISTINCT CASE WHEN settlement_status IN ('FAILED', 'CANCELLED') THEN fbbid END) AS failed_settlements,
    
    -- Conversion rate
    ROUND(COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) * 100.0 / 
          NULLIF(COUNT(DISTINCT fbbid), 0), 2) AS funding_rate_pct,
    
    -- Balance metrics
    SUM(balance_at_settlement) AS total_balance,
    SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS total_recovered,
    
    -- Recovery rate
    ROUND(SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) / 
          NULLIF(SUM(CASE WHEN settlement_status = 'FUNDED' THEN balance_at_settlement END), 0) * 100, 2) AS recovery_rate_pct,
    
    -- Discount rate
    ROUND(AVG(CASE WHEN settlement_status = 'FUNDED' THEN discount_pct END), 2) AS avg_discount_pct,
    
    -- Velocity
    ROUND(AVG(CASE WHEN settlement_status = 'FUNDED' THEN days_in_settlement END), 1) AS avg_days_to_fund,
    
    -- Avg balance
    ROUND(AVG(balance_at_settlement), 2) AS avg_balance_at_settlement

FROM analytics.credit.v_settlement_portfolio_base
GROUP BY 1, 2, 3, 4
HAVING COUNT(DISTINCT fbbid) >= 5
ORDER BY total_recovered DESC;


-- =====================================================
-- EXECUTIVE SUMMARY QUERIES
-- =====================================================

-- Query 1: Overall Settlement Portfolio Summary (Current Snapshot)
/*
SELECT 
    settlement_type,
    settlement_lifecycle_stage,
    COUNT(DISTINCT fbbid) AS settlement_count,
    SUM(balance_at_settlement) AS total_balance,
    SUM(settlement_payment_amount) AS total_recovered,
    ROUND(SUM(settlement_payment_amount) / NULLIF(SUM(balance_at_settlement), 0) * 100, 2) AS recovery_rate_pct,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    ROUND(AVG(days_in_settlement), 1) AS avg_days_in_settlement
FROM analytics.credit.v_settlement_portfolio_base
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- Query 2: Monthly Trend Summary
/*
SELECT 
    settlement_month,
    settlement_type,
    COUNT(DISTINCT fbbid) AS settlements_created,
    COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) AS settlements_funded,
    SUM(CASE WHEN settlement_status = 'FUNDED' THEN settlement_payment_amount END) AS recovered,
    ROUND(COUNT(DISTINCT CASE WHEN settlement_status = 'FUNDED' THEN fbbid END) * 100.0 / 
          NULLIF(COUNT(DISTINCT fbbid), 0), 2) AS funding_rate_pct
FROM analytics.credit.v_settlement_portfolio_base
WHERE settlement_month >= DATEADD('month', -12, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
*/

-- Query 3: Portfolio Health Dashboard
/*
SELECT * FROM analytics.credit.v_settlement_portfolio_health;
*/

-- Query 4: Top Performing Agents (Last 3 Months)
/*
SELECT 
    agent_name,
    agent_role,
    SUM(settlements_funded) AS total_settlements_funded,
    SUM(total_recovered) AS total_recovered,
    ROUND(AVG(funding_rate_pct), 2) AS avg_funding_rate,
    ROUND(AVG(avg_recovery_rate_pct), 2) AS avg_recovery_rate
FROM analytics.credit.v_settlement_agent_performance
WHERE performance_month >= DATEADD('month', -3, CURRENT_DATE)
GROUP BY 1, 2
HAVING SUM(settlements_funded) >= 5
ORDER BY total_recovered DESC
LIMIT 20;
*/

-- Query 5: Vintage Performance Curve (Post-CO)
/*
SELECT 
    co_quarter,
    mob_at_settlement,
    SUM(settlements_funded) AS funded,
    SUM(total_recovered) AS recovered,
    SUM(cumulative_recovered_by_mob) AS cumulative_recovered
FROM analytics.credit.v_settlement_vintage_analysis
WHERE co_quarter >= '2022-Q1'
GROUP BY 1, 2
ORDER BY 1, 2;
*/
