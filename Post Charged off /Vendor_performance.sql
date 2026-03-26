--CREATE OR REPLACE TABLE ANALYTICS.CREDIT.Vendor_performance AS(
WITH ranked_transfers AS (
Select l.*,
    lead(transfer_date,1,'2090-01-01') over(partition by fbbid order by transfer_date) as next_transfer_date,
    lead(recovery_suggested_substate,1,null) over(partition by fbbid order by transfer_date) as next_recovery_agency
from 
 (   SELECT 
            fbbid,
            edate AS transfer_date,
            date(CHARGEOFF_TIME) as charge_off_date,
            recovery_suggested_substate AS recovery_suggested_substate,
            outstanding_principal,
            fees_due,
            discount_pending,
            (outstanding_principal + fees_due - discount_pending) AS transfer_balance,
            -- Rank 1 = First Vendor, Rank 2 = Second Vendor
        ROW_NUMBER() OVER(PARTITION BY fbbid,recovery_suggested_substate ORDER BY edate ASC) as assignment_rank
        FROM bi.public.daily_approved_customers_data
        WHERE recovery_suggested_state = 'ELR' 
        and recovery_suggested_substate not in ('3RD_P_HOLD')
        -- Ensuring we only grab the very first snapshot for each vendor assignment
        QUALIFY ROW_NUMBER() OVER(PARTITION BY fbbid, recovery_suggested_substate ORDER BY edate ASC) = 1
    )l 
    --where assignment_rank = 1
),
Base_data as
(
select *,
    case 
    when recovery_suggested_substate in ('3RD_P_SOLD') then 'SCJ'
    when recovery_suggested_substate in ('ASPIRE_LAW') then 'ASPIRE_LAW'
    when recovery_suggested_substate in ('BK_BL')      then 'BK_BL'
    when recovery_suggested_substate in ('EVANS_MUL') then 'EVANS_MUL' 
    when recovery_suggested_substate in ('LP_HARVEST') then 'Harvest'
    when recovery_suggested_substate in ('LP_WELTMAN') then 'Weltman'
    when recovery_suggested_substate in ('MRS_PRIM','MRS_SEC') then 'MRS'
    when recovery_suggested_substate in ('PB_CAP_PR','PB_CAPITAL') then 'PB_Capital'
    when recovery_suggested_substate in ('SEQ_PRIM','SEQ_SEC') then 'SEQ'
    else 'other'
    end as Vendor_name
from ranked_transfers
)
-- Select * from base_data 
-- where vendor_name = 'PB_Capital';

--Select top 1000* from base_data;
,
Payments as
(
Select 
FBBID,
originator,
date(payment_event_time) as payment_event_time,
sum(TO_DOUBLE(PAYMENT_COMPONENTS_JSON:PAYMENT_AMOUNT)) AS payment_amount,
from bi.finance.payments_model
where payment_status = 'FUND' 
-- and (originator ilike '%BL%' or originator ilike '%Bankruptcy%' or originator ilike '%B&L%' or originator ilike '%P&B%' or originator ilike '%SEQ%'
-- or originator ilike '%WWR%' or originator ilike '%WELTMAN%' or originator ilike '%Harvest' or originator ilike '%HSB%') 
and payment_event_time >='2024-01-01' and parent_payment_id is not null
group by all
)
-- Select * from payments 
-- where originator ilike 'SEQ' and last_day(payment_event_time) = '2025-07-31';

,
with_payments as
(
    Select a.*,
    b.originator,
    b.payment_event_time,
    b.payment_amount
    from base_data a
    left join payments b
    on a.fbbid = b.fbbid 
    and (b.payment_event_time > a.transfer_date and b.payment_event_time<= a.next_transfer_date)
)
,
final_data as
(
    select 
    fbbid,
    transfer_date,
    last_day(transfer_date) as transfer_month,
    charge_off_date,
    Outstanding_principal,fees_due,discount_pending,transfer_balance,
    vendor_name,
    originator,
    case 
    when originator in ('B&L','BL') then 'BL'
    when originator ilike '%HARVEST%' then 'Harvest'
    when originator ilike '%MRS%' then 'MRS'
    when Originator ilike '%P&B%' then 'PB_Capital'
    when originator ilike '%SEQ%' then 'SEQ'
    when originator in ('WWR','Weltman') then 'Weltman'
    else 'other'
    end as payment_vendor,
    payment_event_time,
    last_day(payment_event_time) as payment_month,
    datediff('day',transfer_date,payment_event_time)as days_between_transfer_and_payment,
    floor(days_between_transfer_and_payment/30) mob,
    row_number() OVER (PARTITION BY fbbid,vendor_name ORDER BY payment_event_time) as row_num,
    Case 
    when vendor_name in ('BK_BL') and payment_vendor = 'BL' then payment_amount
    when vendor_name in ('Harvest') and payment_vendor in ('Harvest') then payment_amount
    when vendor_name in ('MRS') and payment_vendor in ('MRS') then payment_amount
    when vendor_name in ('PB_Capital') and payment_vendor in ('PB_Capital') then payment_amount
    when vendor_name in ('SEQ') and payment_vendor in ('SEQ') then payment_amount
    when vendor_name in ('Weltman') and payment_vendor in ('Weltman') then payment_amount
    when vendor_name in ('ASPIRE_LAW') then payment_amount
    when vendor_name in ('EVANS_MUL') then payment_amount
    when vendor_name in ('SCJ') and transfer_date<='2025-12-31' then round((transfer_balance * 0.06), 2) 
    when vendor_name in ('SCJ') and transfer_date>='2026-01-01' then round((transfer_balance * 0.07), 2) 
    else 0 end as payment_amount
    from with_payments
)
select * from final_data 
where vendor_name = 'ASPIRE_LAW' and payment_amount is not null order by transfer_month;
--where last_day(payment_event_time) = '2025-06-30' and payment_vendor = 'other';

select 
last_day(transfer_date) as transfer_month,
year(transfer_date) || '-Q' || quarter(transfer_date) as transfer_quarter,
vendor_name,
last_day(payment_event_time) as payment_month,
year(payment_event_time) || '-Q' || quarter(payment_event_time) as payment_quarter,
payment_vendor,
mob,
ceil(mob / 3) as qob,
row_num,
count(*)as ct,
sum(outstanding_principal) as osp,
sum(transfer_balance) as transfer_balance,
sum(payment_amount) as paid
from final_data
group by all
);



Select 
*
from ANALYTICS.CREDIT.Vendor_performance
where vendor_name = 'PB_Capital'