CREATE OR REPLACE TABLE analytics.credit.agent_collection_mk_v1_1303
(
    PAYMENT_ID_1,
    PAYMENT_TOTAL_AMOUNT_1,
    FBBID_1,
    AGENT_NAME_DP1,
    TRANSACTION_TRANSMISSION_TIME_1,
    DP1,
    CALL_RESULT_DP1,
    PAYMENT_ID_2,
    PAYMENT_TOTAL_AMOUNT_2,
    FBBID_2,
    AGENT_NAME_DP2,
    TRANSACTION_TRANSMISSION_TIME_2,
    DP2,
    CALL_RESULT_DP2,
    -- Settlement disposition (0-365 days)
    PAYMENT_ID_SETTLEMENT,
    FBBID_SETTLEMENT,
    AGENT_NAME_SETTLEMENT,
    TRANSACTION_TRANSMISSION_TIME_SETTLEMENT,
    DP_SETTLEMENT,
    SETTLEMENT_FLAG
)
AS (

WITH eligible_payments AS (
    SELECT pd.*
    FROM bi.finance.payments_model pd

    LEFT JOIN (
        SELECT FBBID, EDATE,
               MAX(CASE WHEN dpd_days IS NULL THEN 0 ELSE dpd_days END) AS dpd_days,
               MAX(is_charged_off) AS is_charged_off
        FROM bi.finance.finance_metrics_daily
        GROUP BY 1, 2
    ) fmd
        ON  pd.fbbid = fmd.fbbid
        AND pd.payment_created_time::date = DATEADD(day, 1, fmd.edate)

    LEFT JOIN bi.public.daily_approved_customers_data dacd
        ON  pd.fbbid = dacd.fbbid
        AND pd.payment_created_time::date = DATEADD(day, 1, dacd.edate)

    WHERE pd.direction      = 'D'
      AND pd.payment_status = 'FUND'
      AND pd.loan_key       IS NOT NULL
      AND (dacd.recovery_suggested_state <> 'ELR' OR dacd.recovery_suggested_state IS NULL)

    ORDER BY pd.payment_id DESC
)

, agent_activity AS (
    SELECT
        id,
        lower(call_result)                                   AS call_result,
        task_date,
        createddate                                          AS task_datetime_old,
        createdbyid,
        fbbid,
        DESCRIPTION,
        user_name__C                                         AS agent_name,
        dateadd(second, -1 * call_duration, createddate)    AS task_datetime
    FROM (
        SELECT
            a.id,
            to_date(a.CREATEDDATE)                           AS task_date,
            a.CALLDISPOSITION                                AS call_result,
            COALESCE(a.CALLDURATIONINSECONDS, 0)             AS call_duration,
            a.calltype,
            a.createddate,
            a.createdbyid,
            a.subject,
            a.calldurationinseconds,
            a.DESCRIPTION,
            a.ACCOUNTID,
            a.role_id_name__C,
            b.NAME                                           AS user_name__C,
            CASE
                WHEN lower(a.fundbox_id__C) = 'not linked' OR a.fundbox_id__C IS NULL
                THEN c.fundbox_id__C
                ELSE a.fundbox_id__C
            END                                              AS FbbID
        FROM external_data_sources.salesforce_nova.task a
        LEFT JOIN EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.USER b
            ON a.CREATEDBYID = b.id
        LEFT JOIN (
            SELECT DISTINCT Fundbox_ID__C, ID
            FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.ACCOUNT
            WHERE fundbox_id__C IS NOT NULL
        ) c ON a.accountid = c.id
        WHERE (
                lower(a.ROLE_ID_NAME__C) LIKE '%collections%'
             OR lower(a.role_id_name__C) LIKE '%recovery%'
             OR a.role_id_name__C IN ('ER agent', 'LR Agent', 'Loan Ops Agent')
             )
          AND a.role_id_name__C <> 'Late Recovery - TH'
          AND a.CREATEDDATE    >= to_date('2020-10-19')
          AND b.NAME           <> 'Fundbox Resolutions'
          AND lower(a.calldisposition) IN ('promise to pay', 'payment', 'settlement accepted')
    )
    WHERE agent_name <> 'Alisha McGee'
      AND agent_name <> 'Candace Smith'
)

, disp1 AS (
    -- Earliest disposition within the last 14 days before payment date
    SELECT
        p.payment_id,
        p.fbbid,
        a.agent_name,
        a.task_datetime,
        a.call_result,
        date(p.payment_planned_transmission_date)           AS payment_transmission_date,
        TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)   AS payment_total_amount,
        ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY a.task_datetime ASC) AS rn_dp1
    FROM eligible_payments p
    LEFT JOIN agent_activity a ON p.fbbid = a.fbbid
    WHERE a.task_datetime::date BETWEEN DATEADD(DAY, -14, date(p.payment_planned_transmission_date))
                                    AND date(p.payment_planned_transmission_date)
)

, disp2 AS (
    -- Latest disposition older than 15 days before payment date
    SELECT
        p.payment_id,
        p.fbbid,
        a.agent_name,
        a.task_datetime,
        a.call_result,
        date(p.payment_planned_transmission_date)           AS payment_transmission_date,
        TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)   AS payment_total_amount,
        ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY a.task_datetime DESC) AS rn_dp2
    FROM eligible_payments p
    LEFT JOIN agent_activity a ON p.fbbid = a.fbbid
    WHERE a.task_datetime::date <= DATEADD(DAY, -15, date(p.payment_planned_transmission_date))
)

, disp_settlement AS (
    -- Settlement accepted dispositions within 0-365 days before payment date
    -- Latest settlement call wins
    SELECT
        p.payment_id,
        p.fbbid,
        a.agent_name,
        a.task_datetime,
        a.call_result,
        date(p.payment_planned_transmission_date)           AS payment_transmission_date,
        TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)   AS payment_total_amount,
        ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY a.task_datetime DESC) AS rn_settlement
    FROM eligible_payments p
    LEFT JOIN agent_activity a ON p.fbbid = a.fbbid
    WHERE a.call_result = 'settlement accepted'
      AND a.task_datetime::date BETWEEN DATEADD(DAY, -365, date(p.payment_planned_transmission_date))
                                    AND date(p.payment_planned_transmission_date)
)

SELECT
      d1.payment_id                                                         AS payment_id_1
    , d1.payment_total_amount                                               AS payment_total_amount_1
    , d1.fbbid                                                              AS fbbid_1
    , d1.agent_name_dp1
    , d1.payment_transmission_date                                          AS transaction_transmission_time_1
    , d1.dp1
    , d1.call_result_dp1
    , d2.payment_id                                                         AS payment_id_2
    , d2.payment_total_amount                                               AS payment_total_amount_2
    , d2.fbbid                                                              AS fbbid_2
    , d2.agent_name_dp2
    , d2.payment_transmission_date                                          AS transaction_transmission_time_2
    , d2.dp2
    , d2.call_result_dp2
    -- Settlement disposition columns (0-365 days)
    , ds.payment_id                                                         AS payment_id_settlement
    , ds.fbbid                                                              AS fbbid_settlement
    , ds.agent_name_settlement
    , ds.payment_transmission_date                                          AS transaction_transmission_time_settlement
    , ds.dp_settlement

    -- ── Settlement flag ──────────────────────────────────────────────────────
    -- Evaluated for disp_settlement (0-365 days window)
    --   FUNDED  : settlement_end_date must be on or after the payment date
    --   ACTIVE  : payment date falls within [settlement_created_date, settlement_end_date]
    -- All other cases → 0
    , CASE
        WHEN ds.agent_name_settlement IS NOT NULL
            AND (
                    (   bs.current_status <>'ACTIVE'
                    AND bs.settlement_end_time >= COALESCE(d1.payment_transmission_date, d2.payment_transmission_date, ds.payment_transmission_date)
                    )
                 OR
                    (   bs.current_status = 'ACTIVE'
                    AND bs.settlement_created_date <= COALESCE(d1.payment_transmission_date, d2.payment_transmission_date, ds.payment_transmission_date)
                    AND (   bs.settlement_end_time >= COALESCE(d1.payment_transmission_date, d2.payment_transmission_date, ds.payment_transmission_date)
                         OR bs.settlement_end_time IS NULL)
                    )
                )
        THEN 1
        ELSE 0
      END                                                                   AS settlement_flag
    -- ─────────────────────────────────────────────────────────────────────────

FROM (
    SELECT payment_id, payment_transmission_date, payment_total_amount, fbbid,
           agent_name  AS agent_name_dp1,
           task_datetime AS dp1,
           call_result AS call_result_dp1
    FROM disp1
    WHERE rn_dp1 = 1
) d1

FULL OUTER JOIN (
    SELECT payment_id, payment_transmission_date, payment_total_amount, fbbid,
           agent_name  AS agent_name_dp2,
           task_datetime AS dp2,
           call_result AS call_result_dp2
    FROM disp2
    WHERE rn_dp2 = 1
) d2 ON d1.payment_id = d2.payment_id

-- Join settlement disposition (0-365 days)
FULL OUTER JOIN (
    SELECT payment_id, payment_transmission_date, payment_total_amount, fbbid,
           agent_name  AS agent_name_settlement,
           task_datetime AS dp_settlement
    FROM disp_settlement
    WHERE rn_settlement = 1
) ds ON COALESCE(d1.payment_id, d2.payment_id) = ds.payment_id

-- Join backy_settlements for settlement validation
-- Pick the settlement that was ACTIVE at the time of PAYMENT (not disposition)
LEFT JOIN analytics.credit.cjk_v_backy_settlements bs
    ON  ds.fbbid = bs.fbbid
    AND ds.agent_name_settlement IS NOT NULL
    -- Settlement must have been created on or before the PAYMENT date
    AND bs.settlement_created_date <= COALESCE(d1.payment_transmission_date, d2.payment_transmission_date, ds.payment_transmission_date)

-- Deduplicate: Keep the most recent settlement that existed at payment time
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(d1.payment_id, d2.payment_id, ds.payment_id)
    ORDER BY bs.settlement_created_date DESC NULLS LAST
) = 1

);


CREATE OR REPLACE TABLE analytics.credit.agent_collection_mk_v2_1303
(
    FBBID,
    PAYMENT_ID,
    PAYMENT_TOTAL_AMOUNT,
    TRANSACTION_TRANSMISSION_TIME,
    AGENT_NAME_DP1,
    DISPOSITION_TIME_1,
    CALL_RESULT_DP1,
    AGENT_NAME_DP2,
    DISPOSITION_TIME_2,
    CALL_RESULT_DP2,
    AGENT_NAME_SETTLEMENT,
    DISPOSITION_TIME_SETTLEMENT,
    SETTLEMENT_FLAG,
    FINAL_AGENT,
    FINAL_AGENT_DISPOSITION_TIME,
    FINAL_CALL_RESULT,
    LOOK_BACK
)
AS (

SELECT
      COALESCE(fbbid_1, fbbid_2, fbbid_settlement)                             AS fbbid
    , COALESCE(payment_id_1, payment_id_2, payment_id_settlement)              AS payment_id
    , COALESCE(payment_total_amount_1, payment_total_amount_2)                 AS payment_total_amount
    , COALESCE(transaction_transmission_time_1,
               transaction_transmission_time_2,
               transaction_transmission_time_settlement)                       AS transaction_transmission_time
    , agent_name_dp1
    , dp1                                                                      AS disposition_time_1
    , call_result_dp1
    , agent_name_dp2
    , dp2                                                                      AS disposition_time_2
    , call_result_dp2
    , agent_name_settlement
    , dp_settlement                                                            AS disposition_time_settlement
    , settlement_flag

    -- ── Updated FINAL_AGENT ───────────────────────────────────────────────────
    -- Priority order:
    --   1. Settlement (0-365 days) with valid settlement_flag = 1
    --   2. DP1 (0-14 days) - PTP/Payment
    --   3. DP2 (15+ days) - PTP/Payment
    , CASE
        WHEN agent_name_settlement IS NOT NULL AND settlement_flag = 1
        THEN agent_name_settlement
        ELSE COALESCE(agent_name_dp1, agent_name_dp2)
      END                                                                      AS final_agent

    , CASE
        WHEN agent_name_settlement IS NOT NULL AND settlement_flag = 1
        THEN dp_settlement
        ELSE COALESCE(dp1, dp2)
      END                                                                      AS final_agent_disposition_time

    , CASE
        WHEN agent_name_settlement IS NOT NULL AND settlement_flag = 1
        THEN 'settlement accepted'
        ELSE COALESCE(call_result_dp1, call_result_dp2)
      END                                                                      AS final_call_result
    -- ─────────────────────────────────────────────────────────────────────────

    , datediff(day, 
               CASE WHEN agent_name_settlement IS NOT NULL AND settlement_flag = 1
                    THEN dp_settlement ELSE COALESCE(dp1, dp2) END,
               COALESCE(transaction_transmission_time_1, 
                        transaction_transmission_time_2,
                        transaction_transmission_time_settlement))             AS look_back

FROM analytics.credit.agent_collection_mk_test1_v1
WHERE datediff(day, 
               CASE WHEN agent_name_settlement IS NOT NULL AND settlement_flag = 1
                    THEN dp_settlement ELSE COALESCE(dp1, dp2) END,
               COALESCE(transaction_transmission_time_1, 
                        transaction_transmission_time_2,
                        transaction_transmission_time_settlement)
              ) <= 365

qualify row_number() over (partition by 
    COALESCE(fbbid_1, fbbid_2, fbbid_settlement),
    COALESCE(payment_id_1, payment_id_2, payment_id_settlement),
    COALESCE(transaction_transmission_time_1, transaction_transmission_time_2, transaction_transmission_time_settlement)
order by payment_id_1) = 1
order by 1
);


CREATE OR REPLACE TABLE analytics.credit.agent_collection_mk_v3_1303 AS
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
    FROM analytics.credit.agent_collection_mk_test2_v2 a
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
    WHERE edate >= '2024-01-01'
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
        b.past_due_payment
    FROM data2 a
    LEFT JOIN past_due_payments b
        ON  a.fbbid = b.fbbid
        AND a.transaction_transmission_time = b.edate
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
    WHERE edate >= '2024-01-01'
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

FROM final_table_agent_attribution;
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
    Select a.*,
    b.payment_context,
    b.payment_total_amount as ocp_payment,
    row_number() over(partition by a.fbbid,final_agent,final_agent_disposition_time,transaction_transmission_time order by ocp_payment) as rn_check
    from final_data_4 a
    left join ocp_payments b
    on a.fbbid = b.fbbid 
    and a.transaction_transmission_time = date(b.payment_event_time) and (past_due_updated_logic is null or past_due_updated_logic =0)
)
Select * from with_OCP_Payments where rn_check=1;




Select 
last_day(transaction_transmission_time) as payment_month,
final_agent,
call_result,
custom_plan,
look_back,
sum(Payment_amount_old_logic) as Payment_amount_old_logic,
sum(past_due_updated_logic) as past_due_updated_logic
from analytics.credit.agent_collection_mk_test3_v3
where transaction_transmission_time >='2025-01-01'
group by all
order by all;


-- with with_settlements as
-- (
-- Select l.* from 
-- (    select a.*,
--     b.settlement_created_date,
--     b.settlement_end_time,
--     b.current_status
--     from analytics.credit.agent_collection_mk_test3_v3 a
--     left join analytics.credit.cjk_v_backy_settlements b
--     on a.fbbid = b.fbbid 
--     qualify row_number() over(partition by a.fbbid
--     order by b.settlement_created_date desc) = 1
-- )l
-- )
-- Select * from with_settlements where settlement_created_date is not null;

-- Select * from analytics.credit.agent_collection_mk_test2_v2
-- where fbbid = 311790 
-- order by transaction_transmission_time;

-- Select * from analytics.credit.cjk_v_backy_settlements
-- where fbbid = 311790
