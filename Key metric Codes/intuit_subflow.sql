CREATE OR REPLACE VIEW INDUS.PUBLIC.INTUIT_SUBFLOW AS (

with
        qbf as (select fbbid from cdc_v2.preapproval.preapproval_requests group by 1),
        page as (
            select
                coalesce(user_id_raw, user_id_retro) user_id,
                SPLIT_PART(SPLIT_PART(first_query_string, 'campaign_name=', 2), '&', 1) as campaign
            from cdc_v2.visitors.visits
            where SPLIT_PART(SPLIT_PART(first_query_string, 'campaign_name=', 2), '&', 1) in ('intuit-inelig','intuit-dec','intuit-overview','Intuit%20Direct%20Mail')
            qualify row_number() over (partition by user_id order by CREATED_TIME desc) = 1
            ),
        drv as (select fbbid from bi.reports.qbf_loan_report group by 1),
        sso as (
            select coalesce(user_id_raw, user_id_retro) user_id, REGEXP_SUBSTR(first_query_string, 'cid=(.*?)&rrid', 1, 1, 'e', 1) as campaign
            from cdc_v2.visitors.visits where first_query_string ilike 'utm_source=intuit&utm_medium=sso%'
            qualify row_number() over (partition by user_id order by CREATED_TIME desc) = 1
            )
    select
        b.fbbid,
        case
            when d.campaign = 'intuit-inelig' then 'Ineligible'
            when b.registration_campaign_name = 'intuit-inelig' then 'Ineligible'
            when d.campaign = 'intuit-dec' then 'Decline'
            when b.registration_campaign_name = 'intuit-dec' then 'Decline'
            when d.campaign = 'intuit-overview' then 'Overview'
            when b.registration_campaign_name = 'intuit-overview' then 'Overview'
            when b.registration_campaign_name ='Intuit Direct Mail' then 'Direct Mail'
            when d.campaign = 'Intuit%20Direct%20Mail' then 'Direct Mail'
            when registration_campaign_source = 'quickbooks-financing' then 'Marketplace'
            when c.fbbid is not null then 'Marketplace'
            when registration_campaign_name ilike 'DR_E%' then 'Email'
            when e.campaign ilike 'dr_e%' then 'Email'
            when registration_campaign_name ='intuit_appcenter' then 'App Center'
            when e.user_id is not null then 'App Center'
            when registration_campaign_name ilike 'qbd-em%' then 'Email'
            when registration_campaign_name ilike 'qbd-dm%' then 'Direct Mail'
            when registration_campaign_name ilike 'dm_%' then 'Direct Mail'
            when registration_campaign_name ilike '%_ipd_%' then 'In-Product Display'
            when registration_campaign_name ilike 'em_%' then 'Email'
            when registration_campaign_name ilike '%email%' then 'Email'
            when registration_campaign_name ilike 'qbd-2018%' then 'Direct Mail'
            when registration_campaign_name ilike 'qbd-2019%' then 'Direct Mail'
            when registration_campaign_name in ('prm-quick2000','prm-try2000') then 'Direct Mail'
            when registration_campaign_name = 'ipp_fundbox_dr_QBFdm93' then 'Direct Mail'
            else null
            end as sub_channel
    from drv a
    join bi.public.customers_data b
        on a.fbbid = b.fbbid
    left join qbf c
        on a.fbbid = c.fbbid
    left join page d
        on b.user_id = d.user_id
    left join sso e
        on b.user_id = e.user_id
    where true
        --and b.registration_time::date between '2024-01-01' and '2025-12-31'
    group by 1,2
)    ;
--1432125
--1471441

SELECT COUNT(*) FROM INDUS.PUBLIC.INTUIT_SUBFLOW;
SELECT COUNT(DISTINCT FBBID) FROM INDUS.PUBLIC.INTUIT_SUBFLOW;
SELECT * FROM INDUS.PUBLIC.INTUIT_SUBFLOW where sub_channel = 'Direct Mail';

select * from bi.public.customers_data where fbbid in (
329280,
397506,
106016,
402541,
331269);

select * from bi.public.customers_data where fbbid in (1432125,1471441) limit 10;
