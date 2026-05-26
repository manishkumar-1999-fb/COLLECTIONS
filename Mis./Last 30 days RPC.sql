WITH base AS (
    SELECT *
    FROM ANALYTICS.CREDIT.SETTLEMENT_TEST_MASTER
    WHERE snapshot_date = '2026-05-11'
      AND dpd_bucket IN (3, 4, 5, 6, 7, 8)
),
params AS (
    SELECT MAX(edate) AS this_week_snapshot
    FROM BI.FINANCE.FINANCE_METRICS_DAILY
    WHERE DAYOFWEEK(edate) BETWEEN 1 AND 5
),
source_five9_view AS (
    SELECT
        cl.fbbid::VARCHAR AS fbbid,
        COALESCE(cl.date_call::DATE, DATE(cl.date_time_call)) AS call_date,
        cl.disposition::VARCHAR AS disposition,
        cl.contacted AS contacted
    FROM ANALYTICS.CREDIT.V_FIVE9_CALL_LOG cl
    WHERE cl.fbbid IS NOT NULL
      AND COALESCE(cl.date_call::DATE, DATE(cl.date_time_call)) IS NOT NULL
      AND (
          TRIM(LOWER(COALESCE(cl.call_type, cl.CALL_TYPE)::VARCHAR)) IN ('inbound', 'outbound', 'manual')
          OR COALESCE(cl.call_type, cl.CALL_TYPE) IS NULL
      )
),
source_five9_report AS (
    SELECT
        cd.fbbid::VARCHAR AS fbbid,
        TO_DATE(fr._data:"DATE"::VARCHAR, 'YYYY/MM/DD') AS call_date,
        fr._data:DISPOSITION::VARCHAR AS disposition,
        fr._DATA:CONTACTED::BOOLEAN AS contacted
    FROM BI.PUBLIC.CUSTOMERS_DATA cd
    INNER JOIN ANALYTICS.CREDIT.FIVE9_CALL_LOG_REPORT fr
        ON REGEXP_REPLACE(cd.phone, '\\D', '') = fr._DATA:DNIS::STRING
    WHERE cd.fbbid IS NOT NULL
      AND TRIM(LOWER(fr._DATA:CAMPAIGN::STRING)) LIKE '%collections%'
      AND (
          TRIM(LOWER(fr._DATA:"CALL TYPE"::STRING)) IN ('inbound', 'outbound', 'manual')
          OR fr._DATA:"CALL TYPE" IS NULL
      )
      AND TO_DATE(fr._data:"DATE"::VARCHAR, 'YYYY/MM/DD') IS NOT NULL
),
source_salesforce_task AS (
    SELECT
        TRY_TO_NUMBER(sf.fundbox_id__c)::VARCHAR AS fbbid,
        sf.lastmodifieddate::DATE AS call_date,
        sf.calldisposition::VARCHAR AS disposition,
        NULL::BOOLEAN AS contacted
    FROM EXTERNAL_DATA_SOURCES.SALESFORCE_NOVA.TASK sf
    WHERE TRY_TO_NUMBER(sf.fundbox_id__c) IS NOT NULL
      AND sf.lastmodifieddate IS NOT NULL
      AND (
          TRIM(LOWER(sf.calltype::VARCHAR)) IN ('inbound', 'outbound', 'manual')
          OR sf.calltype IS NULL
      )
),
collections_call_events AS (
    SELECT fbbid, call_date, disposition, contacted FROM source_five9_view
    UNION ALL
    SELECT fbbid, call_date, disposition, contacted FROM source_five9_report
    UNION ALL
    SELECT fbbid, call_date, disposition, contacted FROM source_salesforce_task
),
contact_flags AS (
    SELECT
        e.fbbid,
        e.call_date,
        e.disposition,
        e.contacted,
        CASE
            WHEN TRIM(LOWER(COALESCE(e.disposition, ''))) IN (
                'f-rpc', 'rpc', 'd-promise to pay', 'promise to pay',
                'b-payment', 'payment', 'third party', 'g-third party'
            )
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%rpc%'
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%promise%pay%'
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%ptp%'
             OR (
                 TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%payment%'
                 AND TRIM(LOWER(COALESCE(e.disposition, ''))) NOT LIKE '%no payment%'
                 AND TRIM(LOWER(COALESCE(e.disposition, ''))) NOT LIKE '%non-payment%'
             )
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%third party%'
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%settlement accepted%'
             OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%settlement payment%'
             OR COALESCE(e.contacted::BOOLEAN, FALSE) = TRUE
            THEN 1
            ELSE 0
        END AS meaningful_contact_ind,
        CASE
            WHEN TRIM(LOWER(COALESCE(e.disposition, ''))) IN ('f-rpc', 'rpc')
              OR TRIM(LOWER(COALESCE(e.disposition, ''))) LIKE '%rpc%'
            THEN 1
            ELSE 0
        END AS strict_rpc_ind
    FROM collections_call_events e
),
rpc_or_contact_last_30d AS (
    SELECT
        c.fbbid,
        MAX(c.strict_rpc_ind) AS flag_strict_rpc_30d,
        MAX(c.meaningful_contact_ind) AS flag_meaningful_contact_30d,
        MAX(c.call_date) AS last_call_date_in_window,
        MAX(CASE WHEN c.meaningful_contact_ind = 1 THEN c.call_date END) AS last_meaningful_contact_date,
        MAX(CASE WHEN c.strict_rpc_ind = 1 THEN c.call_date END) AS last_strict_rpc_date
    FROM contact_flags c
    CROSS JOIN params p
    WHERE c.call_date BETWEEN DATEADD('day', -30, p.this_week_snapshot) AND p.this_week_snapshot
    GROUP BY c.fbbid
)
SELECT
    p.this_week_snapshot,
    b.fbbid,
    COALESCE(r.flag_strict_rpc_30d, 0) AS flag_strict_rpc_30d,
    COALESCE(r.flag_meaningful_contact_30d, 0) AS flag_meaningful_contact_30d,
    r.last_strict_rpc_date,
    r.last_meaningful_contact_date,
    r.last_call_date_in_window
FROM base b
CROSS JOIN params p
LEFT JOIN rpc_or_contact_last_30d r ON r.fbbid = b.fbbid::VARCHAR
ORDER BY b.fbbid;
