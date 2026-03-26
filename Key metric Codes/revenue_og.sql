CREATE or replace view indus.public.revenue_new_ar as (

with FEATURE_STORE_POP AS (
    SELECT *
    FROM data_science.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_POPULATION
    WHERE TIME_MARK = 'Ongoing_ONGOING'
    AND PRODUCT_TYPE IN ('DirectDraw', 'InvoiceClear')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY FBBID, TIME_MARK, CUTOFF_TIME, PRODUCT_TYPE ORDER BY FBBID DESC) = 1 -- Remove Exact Duplicates in population table
),

feature_store_revenue as (
    SELECT 
        fbbid, 
        cutoff_time,
        feature_value
    FROM data_science.RISK_RUN_ALL_FEATURES_SIMULATION.FEATURE_STORE_ALL_FEATURES
    where (fbbid, cutoff_time) in (select fbbid, cutoff_time from feature_store_pop)
    -- AND LAST_UPDATED_JOB_ID <> 0
    AND FEATURE_NAME IN ('FI_V12_MULTIPLE_ACCOUNT_M3_AVG_MONTHLY_INCOME')
),

dacd_plus_fs as (
    select 
           dacd.fbbid,
           dacd.edate,
           dacd.product,
           dacd.sub_product,
           dacd.outstanding_principal as os,
           dacd.dpd_days,
           dacd.credit_limit as cl,
           dacd.last_draw_time,
           dacd.account_status,
           dacd.credit_status,
           dacd.underwriting_flow as uw_flow,
           dacd.first_approved_time::date as fad,
           dacd.fi_data_update_to_time::date as fi_date,
           dacd.PLATFORM_DATA_UPDATE_TO_TIME::date as as_date,
           datediff('day', dacd.fi_data_update_to_time::date, dacd.edate) as days_since_fi, 
           datediff('day', dacd.PLATFORM_DATA_UPDATE_TO_TIME::date, dacd.edate) as days_since_as, 
           datediff('day', x.CUTOFF_TIME::date, dacd.edate) as days_since_ob, 
           x.size::int as ob_rev,
           /*cd.FIRST_ACCOUNT_SIZE_FI::int as rrfi1, --fi rev
           cd.FIRST_ACCOUNT_SIZE_ACCOUNTING_SOFTWARE::int as rras1,
           dacd.risk_review_fi_account_size::int as rrfi2, --fi rev
           dacd.RISK_REVIEW_ACCOUNTING_SOFTWARE_ACCOUNT_SIZE::int as rras2,*/
           --coalesce(rras1,rras2) rras,
           --coalesce(rrfi1,rrfi2) rrfi,
           case when dateadd('day', 90, dacd.fi_data_update_to_time::date)       <= dacd.edate then null else dacd.risk_review_fi_account_size::int end as adj_rrfa,
           case when dateadd('day', 90, dacd.PLATFORM_DATA_UPDATE_TO_TIME::date) <= dacd.edate then null else dacd.RISK_REVIEW_ACCOUNTING_SOFTWARE_ACCOUNT_SIZE::int end as adj_rras, --not older than 90 days data
                
           fsr.cutoff_time as fiv12_cutoff_time,
           datediff('day', fsr.cutoff_time, dacd.edate) as days_since_fiv12,
           
           fsr.feature_value::int as fiv12_mult_monthly_rev,

    from bi.public.daily_approved_customers_data as dacd
    --
    left join (select *,rank()over(partition by fbbid order by CREATED_TIME desc) as run from data_science.risk_first_decision_simulation.chain_results) x		
        on dacd.fbbid = x.fbbid		
        and x.run = 1

    left join feature_store_revenue as fsr
    on fsr.fbbid = dacd.fbbid
    and fsr.cutoff_time::date between dateadd('day', -30, dacd.edate) and dacd.edate

    where true
    and dacd.edate >= '2021-01-01'
    qualify row_number() over (partition by dacd.edate, dacd.fbbid order by fsr.cutoff_time desc, fsr.feature_value desc) = 1
                                
),

w_datas as (
    select a.*, b.as_revenue::int as as_revenue, b.as_revenue_created_time, datediff('day', as_revenue_created_time, a.edate) as days_since_as_revenue,

    from dacd_plus_fs as a
    
    left join ANALYTICS.CREDIT.DAILY_APPROVED_CUSTOMERS_AS_REVENUE as b
    on true
    and a.fbbid = b.fbbid
    and b.edate = a.edate
),

w_filtering as (
    select *
    from w_datas
    where true
)


select fbbid, edate,
        fad, datediff('day',fad,edate) as days_since_approval,
        sub_product,
        account_status,
        os,
        dpd_days,


       days_since_fi,
       days_since_as,
       days_since_fiv12,
       days_since_as_revenue,
       as_date,
        fi_date,
       
       --rrfi,
       --rras,
       ob_rev,
       fiv12_mult_monthly_rev,
       as_revenue,
       
       -- prefer fiv12 if available, otherwise choose the field from DACD
       -- maybe we should add a check for if the fiv12 or revenue_as is out of date?
       
       case when coalesce(days_since_fiv12,days_since_fi) <= 365 and days_since_approval >= 60 then coalesce(fiv12_mult_monthly_rev, ob_rev)
            when coalesce(days_since_fiv12,days_since_fi) <= 365 and days_since_approval < 60 then coalesce(ob_rev, fiv12_mult_monthly_rev)
       else null end as fi_rev,
       case when days_since_as <= 365 and days_since_approval >= 60 then coalesce(as_revenue, ob_rev) 
            when days_since_as <= 365 and days_since_approval < 60 then coalesce(ob_rev,as_revenue) 
       else null end as as_rev,
       
       -- prefer FI over AS
       CASE WHEN coalesce(fi_rev, as_rev) > 20000000 THEN 20000000 ELSE coalesce(fi_rev, as_rev) END as biz_rev,
       
from w_filtering
where sub_product <> 'Credit Builder'
)
;

--select * from indus.public.revenue_new_ar where biz_rev is null and year(fad) > 2023 and edate = '2025-05-14' and account_status ='active' and os > 0 and dpd_days < 98 order by fad;