-----Checking 52 week customers-------------------------------------
create or replace view indus.public.customers_52week as
(
with flags_raw as (
    select created_time as ct, last_modified_time as lmt,  fbbid, feature_flag_id as ffi, value, 
    
    from cdc_v2.feature_flags_hist.BUSINESS_FEATURE_FLAG_PREFERENCES bffp 
    
    where true
    and feature_flag_id in (336, 212)
    qualify row_number() over(partition by fbbid order by last_modified_time desc) = 1
    --and rn = 1
),

flags_edate_fbbid_lvl as (
    select a.fbbid, 
           a.edate,
           A.CREDIT_LIMIT,
           a.fee_percentage_12,
           a.fee_percentage_24,
           a.fee_percentage_52,
           a.fee_rate_12,
           a.fee_rate_24,
           a.fee_rate_52,
           a.dpd_days,
           a.is_chargeoff,
           a.first_approved_time::date as fad,
           a.sub_product,
           a. account_status,
           case when b.value then 1 else 0 end as has_fbp_52w,
           case when c.value then 1 else 0 end as has_reg_52w,
           max(has_fbp_52w) over(partition by a.fbbid, a.edate) as ever_had_fbp,
           max(has_reg_52w) over(partition by a.fbbid, a.edate) as ever_had_reg,
           
    from bi.public.daily_approved_customers_data as a
    
    left join flags_raw as b
    on true
    and a.fbbid = b.fbbid
    and b.lmt <= a.edate
    and b.ffi = 212

    left join flags_raw as c
    on true
    and a.fbbid = c.fbbid
    and c.lmt <= a.edate
    and c.ffi = 336
    
    where true
    --and a.edate >= '2024-01-01'
    and a.sub_product <> 'Credit Builder'
    and a.sub_product <> 'mca'
    and ifnull(a.account_status, 'pass') <> 'suspended'
    and (CASE 
    WHEN is_chargeoff = 0 AND dpd_days IS NULL THEN 0
    ELSE dpd_days 
    END) < 98

    and is_chargeoff = 0
    
    qualify row_number() over(partition by a.fbbid, a.edate order by b.lmt desc, c.lmt desc) = 1
)
--select * from flags_edate_fbbid_lvl where fbbid in (311172, 301228) order by fbbid, edate
select edate, fad, fbbid,sub_product, account_status, dpd_days,is_chargeoff,CREDIT_LIMIT,
case when (ever_had_fbp = 1 or ever_had_reg = 1) then 52
     when (fee_percentage_24 is null or fee_percentage_24 = 0) then 12
     else 24
     end as max_term,
case when (ever_had_fbp = 1 or ever_had_reg = 1) then fee_percentage_52
     when (fee_percentage_24 is null or fee_percentage_24 = 0) then fee_percentage_12
     else fee_percentage_24
     end as MAX_PERCENTAGE,
fee_percentage_12, fee_percentage_24, fee_percentage_52, ever_had_fbp, ever_had_reg from flags_edate_fbbid_lvl );

----------------------------Debt Load------------------------------------------

create or replace view indus.public.alternative_lenders_vol_debt as
(

with cte as(

select yt.fbbid
,date_trunc('month', yt.tdate::date) as trans_month
,sum(case when transaction_type = 'debit' then yt.amount else 0 end) as mon_debits
,sum(case when transaction_type = 'credit' then yt.amount else 0 end) as mon_credits
,max(rev_og.biz_rev) as biz_revenue
,MAX(TP.MAX_TERM) AS MAX_TERM
,MAX(TP.MAX_PERCENTAGE) AS MAX_PERCENTAGE
,MAX(TP.CREDIT_LIMIT) AS CREDIT_LIMIT
,(MAX(CREDIT_LIMIT) * (1 + MAX(MAX_PERCENTAGE/100))/MAX(MAX_TERM)) * 4.33 AS MinMax_Debt_Payments

from 
(   
select *
,transaction_date::date as tdate
from cdc_v2.fi_connect.yodlee_transactions 
)
yt
left join data_science.yodlee_transactions_features.features f
on yt.id = f.transaction_primary_id

LEFT JOIN 
(SELECT * from indus.public.revenue_new_ar) rev_og
on yt.fbbid = rev_og.fbbid
and yt.tdate = rev_og.edate

LEFT JOIN indus.public.customers_52week TP
ON YT.FBBID = TP.FBBID
AND YT.TDATE = TP.EDATE

where is_business_alternative_loan_v1 = 1
and transaction_date::date >= '2021-01-01'

--and transaction_type in ('debit')
group by 1,2
)

, cte_month AS (

SELECT a.*,
B.mon_credits AS mon_credits_1,
LAG(A.MON_DEBITS,1) OVER (PARTITION BY A.FBBID ORDER BY A.TRANS_MONTH) AS MON_DEBIT_1, 
LAG(A.MON_DEBITS,2) OVER (PARTITION BY A.FBBID ORDER BY A.TRANS_MONTH) AS MON_DEBIT_2, 
LAG(A.MON_DEBITS,3) OVER (PARTITION BY A.FBBID ORDER BY A.TRANS_MONTH) AS MON_DEBIT_3

FROM CTE A
LEFT JOIN CTE B
ON A.FBBID = B.FBBID
AND DATEDIFF('MONTH',B.TRANS_MONTH,A.TRANS_MONTH) = 1
)

SELECT
    fbbid,
    trans_month,
    --sum(mon_debits)  as orig_debits,
    --sum(mon_debit_1) as orig_debits_1,
    --sum(mon_debit_2) as orig_debits_2,
    SUM(mon_credits_1) AS ALT_MONTH_ORIG,
    max(biz_revenue) as biz_revenue,
    (SUM(mon_debit_3) + SUM(mon_debit_1) + sum(mon_debit_2))/3 AS AVG_ALT_ORIG ,
    SUM(MinMax_Debt_Payments) AS MinMax_Debt_Payments,
    CASE 
    WHEN MAX(biz_revenue) IS NULL or MAX(biz_revenue) = 0 THEN NULL
    ELSE ((SUM(mon_debits) + SUM(mon_debit_1) + SUM(mon_debit_2))/3 + SUM(MinMax_Debt_Payments))/ MAX(biz_revenue)
END AS DEBT_LOAD
    
    
FROM 
    cte_MONTH
GROUP BY 
    1,2
);

create or replace view indus.public.alternative_lenders_WAL_SHARE as
(
select yt.fbbid
--,yt.transaction_type
--,yt.tdate as edate
,CASE 
		WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', yt.tdate::date+4)::date+2
		WHEN datediff('day', yt.tdate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', yt.tdate, current_date()) <= 0 THEN NULL 
		WHEN datediff('day', yt.tdate, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
		ELSE DATE_TRUNC('WEEK', yt.tdate::date+4)::date+2
	END week_END_DATE


,SUM(yt.amount) AS NFBX_CREDITS

from 
(   
select *
,transaction_date::date as tdate
from cdc_v2.fi_connect.yodlee_transactions 
)
yt
left join data_science.yodlee_transactions_features.features f
on yt.id = f.transaction_primary_id
where is_business_alternative_loan_v1 = 1
and transaction_date::date >= '2021-01-01'

and transaction_type in ('credit')
group by 1,2);

