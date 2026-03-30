-- Payment Past Due Query


CREATE OR REPLACE TABLE analytics.credit.agent_collection_mk_v3_test AS
WITH data AS (
    SELECT
        a.*,
        -- Derive effective call_result mirroring the final_agent logic in test2:
        --   If DP2 is a validated settlement → use DP2's call_result
        --   Otherwise → COALESCE(DP1, DP2) call_result
        CASE
            WHEN call_result_dp2 = 'settlement accepted' AND settlement_flag = 1
            THEN call_result_dp2
            ELSE COALESCE(call_result_dp1, call_result_dp2)
        END AS call_result
    FROM analytics.credit.agent_collection_mk_v2_1303 a
    WHERE last_day(a.transaction_transmission_time) >= '2020-01-31'
),

data2 AS (
    SELECT
        fbbid,
        disposition_time_1,
        call_result_dp1,
        disposition_time_2,
        call_result_dp2,
        settlement_flag,
        call_result,
        final_agent,
        final_agent_disposition_time,
        look_back,
        transaction_transmission_time,
        SUM(payment_total_amount) AS payment_total_amount
    FROM data
    GROUP BY ALL
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Repayments pipeline (NEW - using FACT_DPD / DIM_DPD)
-- ─────────────────────────────────────────────────────────────────────────────
dpd_events AS (
    SELECT 
        DIM.FBBID, 
        DIM.LOAN_KEY, 
        DIM.PAST_DUE_DATE, 
        FACT.BUCKET_CHANGE_REASON,
        FACT.APD_LOAN_TOTAL_AMOUNT, 
        FACT.APD_BUCKET_TOTAL_AMOUNT,
        FACT.EVENT_TIME,
        FACT.FROM_TIME,
        FACT.TO_TIME,
        FACT.DPD_BUCKET,
        FACT.TRIGGERED_BY_PAYMENT_ID
    FROM BI.FINANCE.FACT_DPD FACT
    INNER JOIN BI.FINANCE.DIM_DPD DIM
        USING (HASH_TABLE_KEY)
    INNER JOIN BI.FINANCE.DIM_LOAN DL 
        ON DIM.LOAN_KEY = DL.LOAN_KEY
    WHERE FACT.FROM_TIME >= '2023-01-01'
),

funded_payments AS (
    SELECT DISTINCT DE.TRIGGERED_BY_PAYMENT_ID
    FROM BI.FINANCE.PAYMENTS_MODEL PM
    INNER JOIN dpd_events DE
        ON PM.RELATED_SERVICE_ID = DE.TRIGGERED_BY_PAYMENT_ID
        AND PM.PAYMENT_STATUS = 'FUND'
),

dpd_with_funded_check AS (
    SELECT 
        CASE WHEN FP.TRIGGERED_BY_PAYMENT_ID IS NULL THEN 'not funded' ELSE 'funded' END AS funded_check,
        DE.*
    FROM dpd_events DE
    LEFT JOIN funded_payments FP
        ON DE.TRIGGERED_BY_PAYMENT_ID = FP.TRIGGERED_BY_PAYMENT_ID
),

repayments_with_amount AS (
    SELECT 
        A.FBBID,
        A.LOAN_KEY,
        A.TRIGGERED_BY_PAYMENT_ID,
        B.RELATED_SERVICE_ID,
        B.PAYMENT_EVENT_TIME,
        TO_DOUBLE(B.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) AS repayment_amount
    FROM dpd_with_funded_check A
    LEFT JOIN BI.FINANCE.PAYMENTS_MODEL B
        ON A.LOAN_KEY = B.LOAN_KEY 
        AND A.TRIGGERED_BY_PAYMENT_ID = B.RELATED_SERVICE_ID 
        AND B.PAYMENT_STATUS = 'FUND' 
        AND B.DIRECTION = 'D' 
        AND B.PARENT_PAYMENT_ID IS NOT NULL
    WHERE A.funded_check = 'funded'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY A.FBBID, A.LOAN_KEY, B.RELATED_SERVICE_ID, B.PAYMENT_EVENT_TIME 
        ORDER BY B.PAYMENT_EVENT_TIME
    ) = 1
),

repayments_agg AS (
    SELECT 
        FBBID,
        DATE(PAYMENT_EVENT_TIME) AS repayment_date,
        SUM(repayment_amount) AS repayments
    FROM repayments_with_amount
    GROUP BY 1, 2
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Past-due payment pipeline (same as v3)
-- ─────────────────────────────────────────────────────────────────────────────
fmd_agg AS (
    SELECT
        fmd.loan_key,
        fmd.edate,
        MAX(fmd.dpd_days)       AS dpd_days,
        MAX(fmd.dpd_bucket)     AS dpd_bucket,
        MAX(fmd.is_charged_off) AS is_charged_off_any
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    WHERE edate >= '2023-01-01'
    GROUP BY 1, 2
),

fmd_with_lag AS (
    SELECT *,
        ZEROIFNULL(LAG(dpd_bucket) OVER (PARTITION BY loan_key ORDER BY edate)) AS prev_dpd_bucket
    FROM fmd_agg
),

fmd_final AS (
    SELECT *,
        CASE WHEN dpd_bucket < prev_dpd_bucket THEN 'DROP IN DPD' END AS checking
    FROM fmd_with_lag
),

t2 AS (
    SELECT * FROM fmd_final
    WHERE checking = 'DROP IN DPD'
       OR prev_dpd_bucket > 0
),

T3 AS (
    SELECT DISTINCT
        T2.*,
        PM_1.payment_planned_transmission_date,
        PM_1.payment_event_time,
        PM_1.PAYMENT_ID,
        PM_1.RELATED_SERVICE_ID,
        PM_1.PAYMENT_TYPE,
        PM_1.FBBID,
        PM_1.PAYMENT_DESCRIPTION,
        PM_1.PAYMENT_METHOD_TYPE,
        PM_1.PARENT_PAYMENT_ID,
        TO_DOUBLE(PM_1.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) AS payment_amount
    FROM BI.FINANCE.PAYMENTS_MODEL PM_1
    INNER JOIN T2
        ON  PM_1.LOAN_KEY = T2.LOAN_KEY
        AND CASE
                WHEN PM_1.PAYMENT_METHOD_TYPE = 'CC' THEN PM_1.PAYMENT_STATUS  = 'FUND'
                ELSE                                      PM_1.PAYMENT_STATUS IN ('AUTH', 'TRNS')
            END
        AND PM_1.payment_event_time::DATE = T2.EDATE
        AND PM_1.DIRECTION = 'D'
    WHERE EXISTS (
        SELECT 1 FROM BI.FINANCE.PAYMENTS_MODEL PM_2
        WHERE PM_1.PAYMENT_ID = PM_2.PAYMENT_ID
          AND PM_2.PAYMENT_STATUS = 'FUND'
    )
),

T4 AS (
    SELECT DISTINCT
        PM.PAYMENT_ID,
        PM.RELATED_SERVICE_ID,
        PM.LOAN_KEY,
        PM.PAYMENT_TYPE,
        PM.PAYMENT_CONTEXT,
        PM.PAYMENT_DESCRIPTION,
        PM.PAYMENT_METHOD_TYPE,
        TO_DOUBLE(PM.PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT) AS parent_payment_amount
    FROM BI.FINANCE.PAYMENTS_MODEL PM
    INNER JOIN T3 ON PM.PAYMENT_ID = T3.PARENT_PAYMENT_ID
    QUALIFY PM.PAYMENT_EVENT_TIME = MAX(PM.PAYMENT_EVENT_TIME) OVER (PARTITION BY PM.PAYMENT_ID)
),

FINAL_T AS (
    SELECT
        T3.*,
        T4.PAYMENT_TYPE        AS parent_pmt_payment_type,
        T4.PAYMENT_CONTEXT     AS parent_pmt_payment_context,
        T4.PAYMENT_DESCRIPTION AS parent_pmt_payment_description,
        T4.PAYMENT_METHOD_TYPE AS parent_pmt_method_type,
        T4.parent_payment_amount
    FROM T3
    LEFT JOIN T4 ON T3.PARENT_PAYMENT_ID = T4.PAYMENT_ID
),

final_table AS (
    SELECT
        T2.*,
        FINAL_T.* EXCLUDE (EDATE, LOAN_KEY, dpd_days, dpd_bucket, is_charged_off_any, prev_dpd_bucket, checking)
    FROM T2
    LEFT JOIN FINAL_T
        ON T2.EDATE = FINAL_T.EDATE
       AND T2.LOAN_KEY = FINAL_T.LOAN_KEY
),

final_table2 AS (
    SELECT * FROM final_table
    WHERE payment_amount IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbbid, loan_key, edate, dpd_bucket,
                     payment_planned_transmission_date, payment_id,
                     related_service_id, payment_type, payment_description, payment_amount
        ORDER BY fbbid
    ) = 1
),

past_due_payments AS (
    SELECT
        fbbid,
        edate,
        SUM(payment_amount) AS past_due_payment
    FROM final_table2
    GROUP BY 1, 2
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Join past-due payments to base data
-- Note: settlement validation is already done via settlement_flag in test2
--       — no with_settlements CTE needed
-- ─────────────────────────────────────────────────────────────────────────────
with_past_due_payments AS (
    SELECT
        a.*,
        b.past_due_payment,
        c.repayments
    FROM data2 a
    LEFT JOIN past_due_payments b
        ON  a.fbbid = b.fbbid
        AND a.transaction_transmission_time = b.edate
    LEFT JOIN repayments_agg c
        ON  a.fbbid = c.fbbid
        AND a.transaction_transmission_time = c.repayment_date
),

-- ─────────────────────────────────────────────────────────────────────────────
-- DPD bucket at each payment
-- ─────────────────────────────────────────────────────────────────────────────
fmd_agg1 AS (
    SELECT
        fmd.fbbid,
        fmd.edate,
        MAX(fmd.dpd_days)                AS dpd_days,
        MAX(fmd.dpd_bucket)              AS dpd_bucket,
        SUM(fmd.outstanding_balance_due) AS outstanding_balance_due_all_loans,
        MAX(fmd.is_charged_off)          AS is_charged_off_any
    FROM BI.FINANCE.FINANCE_METRICS_DAILY fmd
    WHERE edate >= '2023-01-01'
    GROUP BY 1, 2
),

with_dpd_bucket AS (
    SELECT
        l.*,
        LAG(dpd_bucket, 1, NULL) OVER (
            PARTITION BY fbbid, final_agent, final_agent_disposition_time
            ORDER BY transaction_transmission_time
        ) AS prev_dpd_bucket
    FROM (
        SELECT
            a.*,
            b.edate      AS dpd_date,
            b.dpd_bucket
        FROM with_past_due_payments a
        LEFT JOIN fmd_agg1 b
            ON  a.fbbid = b.fbbid
            AND date(a.transaction_transmission_time) = DATEADD(day, 1, b.edate)
    ) l
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Custom plan detection — simplified to 1/0 flag
-- ─────────────────────────────────────────────────────────────────────────────
active_custom_plans AS (
    WITH plan_ranges AS (
        SELECT
            fbbid,
            loan_key,
            payment_plan_start_date,
            CASE
                WHEN time_units = 'MONTH' THEN duration * 30
                WHEN time_units = 'WEEK'  THEN duration * 7
                WHEN time_units = 'DAY'   THEN duration
                ELSE 0
            END AS duration_days
        FROM bi.finance.DIM_PAYMENT_PLAN
        WHERE duration IS NOT NULL
          AND is_custom_plan = 1
    ),
    expanded AS (
        SELECT
            fbbid,
            loan_key,
            payment_plan_start_date,
            DATEADD(day, duration_days, payment_plan_start_date) AS end_date
        FROM plan_ranges
    )
    SELECT * FROM expanded
),

with_custom_flag AS (
    SELECT
        a.*,
        b.payment_plan_start_date,
        b.end_date,
        -- Simplified: 1 if a custom plan exists for this payment, 0 otherwise
        CASE WHEN b.payment_plan_start_date IS NOT NULL THEN 1 ELSE 0 END AS custom_plan,
        ROW_NUMBER() OVER (
            PARTITION BY a.fbbid, a.final_agent, a.look_back,
                         a.transaction_transmission_time, a.call_result
            ORDER BY b.payment_plan_start_date
        ) AS rn
    FROM with_dpd_bucket a
    LEFT JOIN active_custom_plans b
        ON  a.fbbid = b.fbbid
        AND a.call_result = 'payment'
        AND a.final_agent_disposition_time::date <= b.payment_plan_start_date
        AND DATEDIFF(day, a.final_agent_disposition_time, b.payment_plan_start_date) <= 30
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Dispo inclusion filter (replaces with_disposition2 from v3)
-- ─────────────────────────────────────────────────────────────────────────────
with_dispo_inclusion AS (
    SELECT *,
        CASE
            WHEN call_result IN ('promise to pay', 'payment') AND look_back <= 30 THEN 1
            WHEN call_result = 'settlement accepted'          AND look_back <= 365 THEN 1
            ELSE 0
        END AS dispo_inclusion_flag
    FROM with_custom_flag
    WHERE rn = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Final attribution flag
-- ─────────────────────────────────────────────────────────────────────────────
final_table_agent_attribution AS (
    SELECT *,
        CASE
            WHEN call_result = 'promise to pay' AND look_back <= 15              THEN 1
            WHEN call_result = 'payment' AND custom_plan = 0 AND look_back <= 15 THEN 1
            WHEN call_result = 'payment' AND custom_plan = 1 AND look_back <= 30 THEN 1
            WHEN call_result = 'settlement accepted'                             THEN 1
            ELSE 0
        END AS flag
    FROM with_dispo_inclusion
    WHERE dispo_inclusion_flag = 1
)

,
final_data_4 as
(SELECT
    *,
    CASE
        WHEN flag = 1 AND custom_plan = 1    THEN payment_total_amount
        WHEN flag = 1 AND settlement_flag = 1 THEN payment_total_amount
        WHEN flag = 1                        THEN past_due_payment
        ELSE 0
    END AS past_due_updated_logic,
    case when look_back <=30 then payment_total_amount else 0 end as Payment_amount_old_logic

FROM final_table_agent_attribution
),
OCP_Payments 
as
(
select *,
TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)   AS payment_total_amount,
from bi.finance.payments_model
where payment_context = 'OCP' and payment_status = 'FUND'
and direction ='D'
)

,With_ocp_Payments as
(
Select l.* from 
(
    Select a.*,
    b.payment_context,
    b.payment_total_amount as ocp_payment,
    row_number() over(partition by a.fbbid,final_agent,final_agent_disposition_time,transaction_transmission_time order by ocp_payment) as rn_check
    from final_data_4 a
    left join ocp_payments b
    on a.fbbid = b.fbbid 
    and a.transaction_transmission_time = date(b.payment_event_time) and (past_due_updated_logic is null or past_due_updated_logic =0)
)l where rn_check = 1
)
--Select * from with_OCP_Payments where rn_check=1;
Select 
* ,
past_due_updated_logic as past_due_payment_updated_logic_2changes,
case 
    when (past_due_updated_logic is null or past_due_updated_logic = 0) 
    and call_result = 'payment' and look_back <=15 and custom_plan = 0 then OCP_payment
    when (past_due_updated_logic is null or past_due_updated_logic = 0) 
    and call_result = 'promise to pay' and look_back <=15 then OCP_payment
else past_due_updated_logic
end as past_due_payment_updated_logic
from With_ocp_Payments;







