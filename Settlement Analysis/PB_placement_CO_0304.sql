/*================================================================================
  Third party vendor placement — PB Capital charge-off file

  PG_SIGNEE_FIRST_NAME / PG_SIGNEE_LAST_NAME: COALESCE across all sources present
  (audit PG, CUSTOMERS_RT_DATA, settlement Person_name, profile person_name).
  One row per fbbid — no extra rows; joins are LEFT only.
================================================================================*/

CREATE OR REPLACE TABLE analytics.credit.PB_placement_CO_0304 AS
WITH Base_table AS (
    SELECT *
    FROM analytics.credit.Charge_off_P_B_accounts
    WHERE
        CO_year >= 2021
        AND PG_Flag = 1
        AND placement_status = 'Internal'
        AND WEDNESDAY_STATE <> 'EOL'
),
data2 AS (
    SELECT DISTINCT
        wp.fbbid,
        CASE
            WHEN wp.wednesday_state = 'ILR' OR sms.INDEX_15_TRANSFER = 1 THEN 'Primary'
            WHEN wp.wednesday_state = 'TR_ILR' THEN 'Secondary'
            ELSE NULL
        END AS placement_type,
        sms.Business_name AS BusinessLegalName,
        sms.Person_name AS PersonName,
        a.full_street_address AS BusinessAddress,
        a.city AS BusinessCity,
        a.state AS BusinessState,
        a.zip_code AS BusinessZip,
        a.email AS Email,
        a.mobile_number AS OriginalPhoneNumber,
        sms.sms_number_from_settlement AS SmsNumber,
        sms.opt_sms_1_0 AS sms_opt_in,
        0 AS Email_unsubscribed,
        a.first_approved_time AS First_txn_time,
        i.streak_start_date AS Current_delq_start_date,
        h.last_payment_date AS LastPaymentDate,
        h.last_payment_date AS last_txn_time,

        CASE
            WHEN a.first_draw_time >= '2019-09-15' AND sms.FIRST_LIEN_SIGNING_TIME IS NOT NULL THEN 1
            ELSE 0
        END AS Lien_filed,

        1 AS "Placement #",
        CURRENT_DATE() AS Date_placed,
        b.co_date AS CHARGE_OFF_DATE,

        CASE
            WHEN f.PG_Signee_First_Name IS NOT NULL
                 AND f.PG_Signee_Last_Name IS NOT NULL
                 AND a.state NOT IN ('MS', 'NC', 'SC', 'SD', 'ME', 'NH', 'WV')
                 AND (
                        (a.state <> 'CA' AND (c.cur_principal + c.cur_fees) >= 10000)
                        OR
                        (a.state = 'CA' AND (c.cur_principal + c.cur_fees) >= 20000)
                    )
            THEN 'Yes' ELSE 'No'
        END AS legal_eligible,

        COALESCE(
            f.PG_Signee_First_Name,
            pg_rt.PG_FIRST_NAME,
            NULLIF(TRIM(SPLIT_PART(TRIM(sms.Person_name), ' ', 1)), ''),
            NULLIF(TRIM(SPLIT_PART(TRIM(f.ProfilePersonName), ' ', 1)), '')
        ) AS PG_SIGNEE_FIRST_NAME,

        COALESCE(
            f.PG_Signee_Last_Name,
            pg_rt.PG_LAST_NAME,
            NULLIF(TRIM(REGEXP_REPLACE(TRIM(sms.Person_name), '^[^ ]+ ', '')), ''),
            NULLIF(TRIM(REGEXP_REPLACE(TRIM(f.ProfilePersonName), '^[^ ]+ ', '')), '')
        ) AS PG_SIGNEE_LAST_NAME,

        a.state AS CONSUMER_BUREAU_REPORT_STATE,

        CASE
            WHEN a.sub_product = 'Line Of Credit' THEN 'LOC'
            ELSE 'TL'
        END AS Product_type,

        c.cur_balance AS Total_balance,
        c.cur_principal AS OS_principal,
        c.cur_fees AS Non_principal_balance
    FROM bi.public.daily_approved_customers_data a
    INNER JOIN Base_table wp ON a.fbbid = wp.fbbid
    LEFT JOIN (
        SELECT fbbid, from_date co_date
        FROM bi.finance.customer_finance_statuses_scd_v
        WHERE status_name = 'IS_CHARGEOFF' AND status_value = 1
    ) b ON a.fbbid = b.fbbid
    LEFT JOIN (
        SELECT fbbid, SUM(principal) cur_principal, SUM(fees - discount_pending) cur_fees, SUM(balance_calc) cur_balance
        FROM bi.finance.fact_balance
        WHERE balance_type = 'BALANCE DUE FUNDED' AND DATE(to_ble_time_calc) >= '2030-01-01'
        GROUP BY 1
    ) c ON a.fbbid = c.fbbid
    LEFT JOIN (
        SELECT fbbid, PG_FIRST_NAME, PG_LAST_NAME
        FROM bi.public.CUSTOMERS_RT_DATA
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY fbbid
            ORDER BY PG_FIRST_NAME NULLS LAST, PG_LAST_NAME NULLS LAST
        ) = 1
    ) pg_rt ON a.fbbid = pg_rt.fbbid
    LEFT JOIN (
        SELECT
            fbbid,
            initcap(auditlog_FN) AS PG_Signee_First_Name,
            initcap(auditlog_LN) AS PG_Signee_Last_Name,
            ProfilePersonName
        FROM (
            SELECT
                al.fbbid,
                lower(ale.first_name) AS Auditlog_FN,
                lower(ale.last_name) AS Auditlog_LN,
                fbp.person_name AS ProfilePersonName,
                ROW_NUMBER() OVER (PARTITION BY al.fbbid ORDER BY al.created_time DESC) AS rn
            FROM cdc.audit_log.audit_logs al
            JOIN bi.PUBLIC.APPROVED_CUSTOMERS_DATA fbp ON fbp.fbbid = al.fbbid
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
    ) f ON a.fbbid = f.fbbid
    LEFT JOIN (
        SELECT fbbid, MAX(payment_planned_transmission_date) last_payment_date
        FROM bi.finance.payments_model
        WHERE payment_status = 'FUND' AND direction = 'D'
        GROUP BY 1
    ) h ON a.fbbid = h.fbbid
    LEFT JOIN (
        SELECT fbbid, streak_start_date
        FROM cdc.recovery.recovery_business
    ) i ON a.fbbid = i.fbbid
    LEFT JOIN (
        SELECT DISTINCT
            FBBID,
            INDEX_15_TRANSFER,
            OPT_SMS_1_0,
            SMS_Number AS sms_number_from_settlement,
            FIRST_LIEN_SIGNING_TIME,
            Business_name,
            Person_name
        FROM TABLEAU.CREDIT.SETTLEMENT_MASTER_TABLE1_NEW
        QUALIFY ROW_NUMBER() OVER (PARTITION BY FBBID ORDER BY OPT_SMS_1_0 DESC) = 1
    ) sms ON a.fbbid = sms.fbbid
    WHERE a.edate = CURRENT_DATE()
)

SELECT
    fbbid AS "Fbbid",
    placement_type AS "placement type",
    BusinessLegalName AS "Business Name",
    PersonName AS "Person Name",
    BusinessAddress AS "Business Addr1 Street",
    BusinessCity AS "Business Addr1 City",
    BusinessState AS "Business Addr1 State",
    BusinessZip AS "Business Addr1 Zip5",
    Email AS "Email Address",
    OriginalPhoneNumber AS "Original Phone number",
    SmsNumber AS "Sms Number",
    sms_opt_in AS "Sms Opt in",
    Email_unsubscribed AS "Email unsubscribed",
    First_txn_time AS "First_txn_time",
    Current_delq_start_date AS "Current_delq_start_date",
    LastPaymentDate AS "Last Payment Date",
    last_txn_time AS "last_txn_time",
    Lien_filed AS "Lien filed",
    "Placement #" AS "Placement #",
    Date_placed AS "Date placed",
    CHARGE_OFF_DATE AS "CHARGE_OFF_DATE",
    legal_eligible AS "legal_eligible",
    PG_SIGNEE_FIRST_NAME AS "PG_SIGNEE_FIRST_NAME",
    PG_SIGNEE_LAST_NAME AS "PG_SIGNEE_LAST_NAME",
    CONSUMER_BUREAU_REPORT_STATE AS "CONSUMER_BUREAU_REPORT_STATE",
    Product_type AS "Product type",
    ROW_NUMBER() OVER (ORDER BY fbbid) AS "Row number",
    'PB_Capital' AS "Third Party",
    Total_balance AS "Total balance",
    OS_principal AS "OS principal",
    Non_principal_balance AS "Non-principal balance"
FROM data2;
