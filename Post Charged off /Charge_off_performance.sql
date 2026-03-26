
create or replace table analytics.credit.charged_off_liquidation_250226 as
with status_history AS (
    SELECT 
        fbbid,
        edate AS transfer_date,
        recovery_suggested_state,
        recovery_suggested_substate,
        outstanding_principal,
        fees_due,
        discount_pending,
        date(CHARGEOFF_TIME) as charge_off_date,
        (outstanding_principal + fees_due - discount_pending) AS transfer_balance,
        -- Trigger a 1 if the state OR the substate changes from the previous record
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
)
--Select * from status_history where fbbid = 400743 and is_new_transition = 1;
,
state_transitions AS (
    -- Filter to only the rows where a change happened, then rank them descending
    Select l.*,
    lead(transfer_date,1,'2090-01-01') over(partition by fbbid order by transfer_date) as next_transfer_date
    from 
    (SELECT 
        fbbid,
        transfer_date,
        recovery_suggested_state,
        recovery_suggested_substate,
        outstanding_principal,
        fees_due,
        discount_pending,
        transfer_balance,
        charge_off_date,
        ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY transfer_date DESC) as recency_rank
    FROM status_history
    WHERE is_new_transition = 1 and ((recovery_suggested_state = 'ELR' and recovery_suggested_substate is not null) or recovery_suggested_state <>'ELR')
    )l
 )   
-- Select * from state_transitions where fbbid = 400743 order by transfer_date;
,Base_data as
(
Select *,
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
    when recovery_suggested_state in ('PROLIT','TR_LR') then 'External_non_agency'
    else 'other'
    end as Vendor_name,
            CASE 
            WHEN RECOVERY_SUGGESTED_STATE IN ('ILR', 'LR', 'ER', 'FB_TL', 'CB_DLQ', 'HEAL', 'TR_ILR', 'EOL', 'PRELIT', 'LPD', 'MCA_HE') 
                 OR RECOVERY_SUGGESTED_STATE IS NULL THEN 'Internal'
            WHEN RECOVERY_SUGGESTED_STATE IN ('ELR', 'PROLIT', 'TR_LR') THEN 'External'
            ELSE 'Unknown'
        END AS placement_status
        from state_transitions
)
,Payments as
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
and payment_event_time >='2020-01-01' and parent_payment_id is not null
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
,final_data as
(
    select 
    fbbid,
    transfer_date,
    next_transfer_date,
    last_day(transfer_date) as transfer_month,
    charge_off_date,
    last_day(charge_off_date) as co_month,
    Outstanding_principal,fees_due,discount_pending,transfer_balance,
    vendor_name,
    originator,
    placement_status,
    row_number() over(partition by fbbid order by transfer_date asc) as rn,
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
    datediff('day',charge_off_date,payment_event_time)as days_between_CO_and_payment,    
    floor(days_between_transfer_and_payment/30) mob,
    floor(days_between_CO_and_payment/30) mob_CO,    
    Case 
    when placement_status = 'Internal' then payment_amount
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
    when vendor_name ='External_non_agency' then payment_amount 
    else 0 end as payment_amount
    from with_payments
)
Select * from final_data ;
-- where fbbid = 1342838 order by transfer_date ;
-- where placement_status = 'Internal' and payment_amount is not null and last_day(payment_event_time) = '2026-02-28';


select 
transfer_month,
year(transfer_date) || '-Q' || quarter(transfer_date) as transfer_quarter,
co_month,
year(Charge_off_date) || '-Q' || quarter(Charge_off_date) as transfer_quarter,
vendor_name,
last_day(payment_event_time) as payment_month,
year(payment_event_time) || '-Q' || quarter(payment_event_time) as payment_quarter,
placement_status,
payment_vendor,
mob,
mob_CO,
sum(case when rn =1 then outstanding_principal end) as co_principal,
sum(case when rn = 1 then 1 else 0 end) as unqiue_count,
count(*)as ct,
sum(outstanding_principal) as osp,
sum(transfer_balance) as transfer_balance,
sum(payment_amount) as paid
from final_data
group by all;


Select *,
year(Charge_off_date) || '-Q' || quarter(Charge_off_date) as CO_quarter,
from analytics.credit.charged_off_liquidation_250226
where co_month in ('2023-01-31','2023-02-28','2023-03-31') 
and placement_status = 'Internal'
--and payment_amount is not null 
order by fbbid,mob_co;



Select * from analytics.credit.cjk_v_backy_settlements
where fbbid in (
'8831',
'105063',
'119092',
'150171',
'158449',
'298053',
'327565',
'375787',
'396256',
'397067',
'401422',
'406698',
'406904',
'411699',
'711424',
'746398',
'785210',
'852644',
'871117',
'985413',
'1075079',
'1084944',
'1153914',
'1160035',
'1171146',
'1185694',
'1214444',
'1256550',
'1372415',
'1386554',
'1388177',
'1396217'
)