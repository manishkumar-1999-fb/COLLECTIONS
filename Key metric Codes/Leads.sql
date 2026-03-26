-----------------------------------------------------------------------------LEADS FUNNEL----------------------------------------------------------------------

    CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1_new_td AS

        WITH RECURSIVE a AS (
            SELECT DATEFROMPARTS(2020, 12, 30) AS week_end_date
            UNION ALL
            SELECT DATEADD(DAY, 7, week_end_date)
            FROM a
            WHERE week_end_date <= current_date()-7
        )

        , b AS (
        SELECT CASE WHEN dayofweek(current_date()) = 3 THEN current_date() ELSE current_date()-1 END week_end_date
        )

        , channel_type_new_table AS (
        SELECT 'Unknown' channel_type_new UNION SELECT 'Lendio-Direct Sales' channel_type_new UNION SELECT 'Lendio-Embedded' channel_type_new UNION SELECT 'InvoiceSimple' channel_type_new UNION SELECT 'Joist' channel_type_new
        )

        -- , channel_type_evc_table AS (
        -- SELECT 'Unknown' channel_type_evc UNION SELECT 'InvoiceSimple' channel_type_new UNION SELECT 'Joist' channel_type_new
        -- )

        , risk_bucket_table AS (
        SELECT 'No Bucket' risk_bucket UNION SELECT 'OB: 1-4' risk_bucket UNION SELECT 'OB: 5-7' risk_bucket UNION SELECT 'OB: 8-10' risk_bucket UNION SELECT 'OB: 11-12' risk_bucket UNION SELECT 'OB: 13+' risk_bucket 
        )

        , partner_name_table AS (
        SELECT 'Lendio' partner_name 
        UNION SELECT 'smallbusinessloans' partner_name 
        UNION SELECT 'BusinessLoans' partner_name 
        UNION SELECT 'sofi' partner_name 
        UNION SELECT 'Cardiff' partner_name 
        UNION SELECT  'Nav' partner_name 
        UNION SELECT 'AtoB' partner_name 
        UNION SELECT 'Bluevine' partner_name 
        UNION SELECT 'ZenBusiness' partner_name 
        UNION SELECT 'EverCommerce' partner_name
        -- UNION SELECT 'Joist' partner_name 
        -- UNION SELECT 'InvoiceSimple' partner_name
        UNION SELECT 'Anansii' partner_name 
        UNION SELECT '1West' partner_name 
        )

        , preapproval_filter_table AS (
        SELECT 'Preapproval' preapproval_filter UNION SELECT 'Non-Preapproval' preapproval_filter
        )

        , industry_type_table AS (
        SELECT 'ASWR' industry_type UNION SELECT 'Others' industry_type UNION SELECT 'Real Estate, Rental & Hospitality' industry_type UNION SELECT 'Professional Services' industry_type UNION SELECT 'Retail & Wholesale Trade' industry_type UNION SELECT 'Transportation & Warehousing' industry_type UNION SELECT 'Construction' industry_type UNION SELECT 'No Data' industry_type
        )

        , customer_revenue_table AS (
        SELECT '$0 - $500K' customer_revenue UNION 
    select '$500K - $1.5M' customer_revenue UNION 
    select '> $1.5M' customer_revenue UNION 
--    select '> $1M' customer_revenue_group UNION 
    select 'Other/No Data' customer_revenue
        )
                

        SELECT a.week_end_date
        , ct.CHANNEL_TYPE_NEW
        , rbt.risk_bucket bucket_group
        , pt.PARTNER_NAME
        , pft.preapproval_filter
        , it.industry_type
        , crt.customer_revenue

        FROM a

        CROSS JOIN CHANNEL_TYPE_NEW_TABLE ct
        CROSS JOIN risk_bucket_table rbt
        CROSS JOIN PARTNER_NAME_TABLE pt
        CROSS JOIN PREAPPROVAL_FILTER_TABLE pft
        CROSS JOIN INDUSTRY_TYPE_TABLE it
        CROSS JOIN CUSTOMER_REVENUE_TABLE crt

        UNION

        SELECT b.week_end_date
        , ct.CHANNEL_TYPE_NEW
        , rbt.risk_bucket bucket_group
        , pt.PARTNER_NAME
        , pft.preapproval_filter
        , it.industry_type
        , crt.customer_revenue

        FROM b

        CROSS JOIN CHANNEL_TYPE_NEW_TABLE  ct
        CROSS JOIN risk_bucket_table rbt
        CROSS JOIN PARTNER_NAME_TABLE pt
        CROSS JOIN PREAPPROVAL_FILTER_TABLE pft
        CROSS JOIN INDUSTRY_TYPE_TABLE it
        CROSS JOIN CUSTOMER_REVENUE_TABLE crt

        ORDER BY 1 DESC,2
        ;


    CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_leads_td AS (
        SELECT *, 1 AS ONE
        FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1_new_td);


-----------------------------------------------------------------------------BASE TABLE----------------------------------------------------------------------

    CREATE OR REPLACE TABLE INDUS.PUBLIC.leads_base_table_td AS 
    (
        SELECT PA.fbbid
            , channel
            , partner
            , intuit_flow
            , nav_flow
            , lendio_flow
            , national_funding_flow
            , evercommerce_flow
            , tier
            , sub_product
            , industry_type
            , PA.ob_risk_bucket_egl
            --, ob_risk_bucket_group
            , PA.ob_bucket_group
            , customer_annual_revenue_group as customer_annual_revenue_group
            , week_end
            , registration_time
            , reg_start_week_end_date
            , cip_connected_week_end_date
            , first_connected_week_end_date
            , reg_complete_week_end_date
            , risk_review_week_end_date
            , first_dec_week_end_date
            , app_week_end_date
            , ftd_week_end_date
            , is_underwriting_new
            , is_risk_review
            , pq_months_in_business
            , has_pq_months_in_business
            , calc_revenue
            , has_calc_revenue
            , is_approved
            , is_fraud
            , rejection_reason
            , rejection_reason_2
            , is_rejected
            , first_rejected_reason
            , is_ftu
            , is_ftd_0_7
            , is_ftd_8_28
            , is_ftd_29_60
            , is_ftd_61_
            , is_ftd7
            , is_ftd28
            , CD.first_draw_amount
            , first_approved_credit_limit
            , underwritten_time 
            , current_credit_status
            , current_credit_status_reason
            , fico_onboarding

            -- , CASE WHEN PA.partner_name = 'Lendio' AND PA.channel_type = 'Unknown' THEN 'Lendio-Embedded' 
            -- WHEN PA.partner_name = IN ('InvoiceSimple', 'Joist') AND PA.channel_type = 'Unknown' THEN 'EverCommerce' 
            -- ELSE PA.channel_type END AS channel_type_new 

            , CASE WHEN PA.partner_name = 'Lendio' AND PA.channel_type = 'Unknown' THEN 'Lendio-Embedded' 
            WHEN (PA.partner_name = 'InvoiceSimple' AND PA.channel_type = 'Unknown') THEN 'InvoiceSimple'
            WHEN (PA.partner_name = 'Joist') THEN 'Joist' ELSE PA.channel_type END AS channel_type_new

            , CASE WHEN PA.partner_name IN ('InvoiceSimple' , 'Joist') THEN 'EverCommerce' ELSE PA.partner_name END AS partner_name_upd

            , PA.initial_lead_submission_timestamp::date AS lead_date
            
            , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', lead_date+4)::date+2
                    WHEN datediff('day', lead_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', lead_date, current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', lead_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', lead_date+4)::date+2
                    END lead_week_end_date
                    
            , PA.prequal_timestamp::date AS prequal_date
            
            , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', prequal_date+4)::date+2
                    WHEN datediff('day', prequal_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', prequal_date, current_date()) <= 0 THEN NULL 
                    WHEN datediff('day', prequal_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                    ELSE DATE_TRUNC('WEEK', prequal_date+4)::date+2
                    END prequal_week_end_date
                    
            , CASE WHEN PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.prequal_decision = 'rejected' THEN 0
                    ELSE 0
                    END is_pre_approval
                    
            , CASE WHEN PA.partner_name = 'Lendio' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'Nav' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'Bluevine' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = '1West' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'sofi' AND PA.prequal_decision = 'approved' THEN 1
                    --WHEN PA.partner_name = 'Cardiff' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'ZenBusiness' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'Joist' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'InvoiceSimple' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'Anansii' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'AtoB' AND PA.prequal_decision = 'approved' THEN 1
                    WHEN PA.partner_name = 'BusinessLoans' THEN 1
                    WHEN PA.partner_name = 'Cardiff' THEN 1
                    WHEN pa.partner_name = 'smallbusinessloans' THEN 1
                    ELSE 0
                    END pre_approval_flag
                    
            , PA.preapproval_filter
            , PA.pre_approval_amount AS pre_approved_credit_limit
            , PA.prequal_decision AS pre_approved_decision

            , CASE WHEN PA.prequal_decision = 'approved' AND is_approved = 1 THEN 1 ELSE 0 END is_match_approval
            , CASE WHEN PA.prequal_decision = 'approved' AND is_approved = 0 THEN 1 ELSE 0 END is_match_declined
            , CASE WHEN PA.prequal_decision = 'approved' AND is_approved = 1 and PA.pre_approval_amount <= first_approved_credit_limit THEN 1 ELSE 0 END is_match_credit_limit
        
            , CASE WHEN is_fraud = 1 then 1
                    WHEN current_credit_status_reason in ('Rejected for fraud','Reject but got a chance to manual review by fraud','Fraud consistency score card') then 1
                    WHEN current_credit_status_reason in ('Potential prohibited business name','Compliance: Nature of business') then 1
                    ELSE 0
                    END fraud_compliance_excl
            
            , PA.email1_sending_time::date email_1_date
            , PA.email2_sending_time::date email_2_date
            , PA.email3_sending_time::date email_3_date
            , PA.email4_sending_time::date email_4_date
            , PA.email5_sending_time::date email_5_date
            , PA.email6_sending_time::date email_6_date

            , CASE WHEN email1_was_opened IS NOT NULL THEN 1 ELSE 0 END email_1_opened
            , CASE WHEN email2_was_opened IS NOT NULL THEN 1 ELSE 0 END email_2_opened
            , CASE WHEN email3_was_opened IS NOT NULL THEN 1 ELSE 0 END email_3_opened
            , CASE WHEN email4_was_opened IS NOT NULL THEN 1 ELSE 0 END email_4_opened
            , CASE WHEN email5_was_opened IS NOT NULL THEN 1 ELSE 0 END email_5_opened
            , CASE WHEN email6_was_opened IS NOT NULL THEN 1 ELSE 0 END email_6_opened

            , CASE WHEN email1_was_opened IS NOT NULL 
                OR email2_was_opened IS NOT NULL
                OR email3_was_opened IS NOT NULL
                OR email4_was_opened IS NOT NULL 
                OR email5_was_opened IS NOT NULL
                OR email6_was_opened IS NOT NULL THEN 1
                ELSE 0
                END any_email_opened

            , PA.calculated_annual_revenue leads_annual_revenue
            , CASE WHEN leads_annual_revenue >= 0 AND leads_annual_revenue < 500000 THEN  '$0 - $500K'
                WHEN leads_annual_revenue >= 500000 AND leads_annual_revenue < 1500000 THEN  '$500K - $1.5M'
                --WHEN leads_annual_revenue >= 500000 AND leads_annual_revenue < 1000000 THEN  '$500K - $1M'
                WHEN leads_annual_revenue >= 1500000 THEN '> $1.5M'
                ELSE 'Other/No Data' 
                END leads_annual_revenue_bucket

            , CASE WHEN PA.fico_score <= 850 THEN PA.fico_score	ELSE NULL END AS leads_fico

            , CASE WHEN datediff('day',PA.prequal_timestamp,CD.registration_time)<8 THEN 1 ELSE 0 END AS is_reg7_pa
            , CASE WHEN datediff('day',PA.prequal_timestamp,CD.registration_time)<15 THEN 1 ELSE 0 END AS is_reg14_pa
            , CASE WHEN datediff('day',PA.prequal_timestamp,CD.registration_time)<29 THEN 1 ELSE 0 END AS is_reg28_pa

            , CASE WHEN datediff('day',PA.initial_lead_submission_timestamp,CD.registration_time)<8 THEN 1 ELSE 0 END AS is_reg7
            , CASE WHEN datediff('day',PA.initial_lead_submission_timestamp,CD.registration_time)<15 THEN 1 ELSE 0 END AS is_reg14
            , CASE WHEN datediff('day',PA.initial_lead_submission_timestamp,CD.registration_time)<29 THEN 1 ELSE 0 END AS is_reg28

        FROM 
        ((SELECT t1.*
            , t2.EMAIL6_SENDING_TIME
            , t2.EMAIL6_WAS_OPENED
            , t2.EMAIL6_CLICK_THROUGH_RATE
            , t2.EMAIL5_SENDING_TIME
            , t2.EMAIL5_WAS_OPENED
            , t2.EMAIL5_CLICK_THROUGH_RATE
            , t2.EMAIL4_SENDING_TIME
            , t2.EMAIL4_WAS_OPENED
            , t2.EMAIL4_CLICK_THROUGH_RATE
            , t2.EMAIL3_SENDING_TIME
            , t2.EMAIL3_WAS_OPENED
            , t2.EMAIL3_CLICK_THROUGH_RATE
            , t2.EMAIL2_SENDING_TIME
            , t2.EMAIL2_WAS_OPENED
            , T2.EMAIL2_CLICK_THROUGH_RATE, t2.EMAIL1_SENDING_TIME, T2.EMAIL1_WAS_OPENED, T2.EMAIL1_CLICK_THROUGH_RATE

            FROM 
                (SELECT a.fbbid
                , b.EAGLET_BUCKET as ob_risk_bucket_egl
                , CASE
                    WHEN b.EAGLET_BUCKET BETWEEN 1 AND 4 THEN 'OB: 1-4' -- Use b.EAGLET_BUCKET here
                    WHEN b.EAGLET_BUCKET BETWEEN 5 AND 7 THEN 'OB: 5-7' -- And here
                    WHEN b.EAGLET_BUCKET BETWEEN 8 AND 10 THEN 'OB: 8-10' -- And here
                    WHEN b.EAGLET_BUCKET BETWEEN 11 AND 12 THEN 'OB: 11-12' -- And here
                    WHEN b.EAGLET_BUCKET >= 13 THEN 'OB: 13+' -- And here
                    ELSE 'No Bucket'
                END ob_bucket_group
		        --END ob_bucket_group
                    , partner_name 
                    , annual_revenue
                    , calculated_annual_revenue
                    , initial_lead_submission_timestamp
                    , prequal_timestamp
                    , lead_sunset_timestamp
                    , in_prequal_api
                    , speed_to_lead
                    , prequal_decision
                    , pre_approval_amount
                    , fico_score
                    , channel_type
                    , final_decision
                    , first_draw_amount
                    , CASE WHEN partner_name IN ('Lendio','Nav','AtoB','Bluevine','1West','Anansii','sofi','ZenBusiness','InvoiceSimple','Joist') THEN 'Preapproval'
                            WHEN partner_name IN ('BusinessLoans','Cardiff','smallbusinessloans') THEN 'Non-Preapproval'
                            END AS preapproval_filter
                FROM bi.CUSTOMERS.LEADS_DATA a left JOIN ANALYTICS.CREDIT.EAGLET_KEY_METRICS_SCORES b on a.fbbid = b.fbbid
                WHERE partner_name in ('Lendio', 'BusinessLoans', 'Nav', 'AtoB','smallbusinessloans','Bluevine','1West','Anansii','sofi','ZenBusiness','InvoiceSimple', 'Joist')) t1

        LEFT JOIN 
                (
                SELECT fbbid
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_6' THEN sending_time ELSE NULL END) AS email6_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_6' THEN was_opened ELSE NULL END) AS email6_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_6' THEN CLICK_THROUGH_RATE ELSE NULL END) AS email6_click_through_rate
                    
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_5' THEN sending_time ELSE NULL END) AS email5_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_5' THEN was_opened ELSE NULL END) AS email5_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_5' THEN CLICK_THROUGH_RATE ELSE NULL END ) AS email5_click_through_rate
                
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_4' THEN sending_time ELSE NULL END) AS email4_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_4' THEN was_opened ELSE NULL END) AS email4_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_4' THEN CLICK_THROUGH_RATE ELSE NULL END) AS email4_click_through_rate
                    
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_3' THEN sending_time ELSE NULL END) AS email3_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_3' THEN was_opened ELSE NULL END)  AS email3_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_3' THEN CLICK_THROUGH_RATE ELSE NULL END) AS email3_click_through_rate
                
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_2' THEN sending_time ELSE NULL END) AS email2_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_2' THEN was_opened ELSE NULL END) AS email2_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_2' THEN CLICK_THROUGH_RATE ELSE NULL END) AS email2_click_through_rate
                    
                     , MIN(CASE WHEN TYPE = 'marketplace_lead_email_1' THEN sending_time ELSE NULL END) AS email1_sending_time
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_1' THEN was_opened ELSE NULL END) AS email1_was_opened
                     , MAX(CASE WHEN TYPE = 'marketplace_lead_email_1' THEN CLICK_THROUGH_RATE ELSE NULL END) AS email1_click_through_rate
            
                FROM
                    (SELECT FBBID, TYPE, SENDING_TIME, WAS_OPENED, CLICK_THROUGH_RATE FROM CDC_V2.COMMUNICATION.MAIL_EVENTS)
                        WHERE TYPE IN ('marketplace_lead_email_6',
                                'marketplace_lead_email_5',
                                'marketplace_lead_email_4',
                                'marketplace_lead_email_3',
                                'marketplace_lead_email_2',
                                'marketplace_lead_email_1')
                            AND SENDING_TIME IS NOT NULL 
                            AND FBBID IS NOT NULL 
                        GROUP BY 1) t2
                        ON t1.fbbid = t2.FBBID)
                        ) PA

        LEFT JOIN 
            (
            SELECT fbbid
                , channel
                , partner
                , intuit_flow
                , nav_flow
                , lendio_flow
                , national_funding_flow
                , evercommerce_flow
                , tier
                , sub_product
                , industry_type
                
                --, ob_risk_bucket_group
                --, CASE WHEN OB_BUCKET_GROUP IS NULL THEN 'No Bucket' ELSE OB_BUCKET_GROUP END AS OB_BUCKET_GROUP

                , fico_onboarding
                , CASE WHEN fico_onboarding < 600 THEN '<600'
                        WHEN fico_onboarding >= 600 AND fico_onboarding < 650 THEN '600-649'
                        WHEN fico_onboarding >= 650 AND fico_onboarding < 700 THEN '650-699'
                        WHEN fico_onboarding >= 700 AND fico_onboarding < 750 THEN '700-749'
                        WHEN fico_onboarding >= 750 THEN '750+'
                        ELSE 'NULL'
                        END fico_bucket

                , customer_annual_revenue_group

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                        WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0
                            AND datediff('day', registration_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                        END week_end

                , registration_time
                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                        WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0
                            AND datediff('day', registration_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', registration_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', registration_time::date+4)::date+2
                        END reg_start_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
                        WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', cip_connected_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', cip_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', cip_connected_time::date+4)::date+2
                        END cip_connected_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_connected_time::date+4)::date+2
                        WHEN datediff('day', first_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', first_connected_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_connected_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_connected_time::date+4)::date+2
                        END first_connected_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                        WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', registration_flow_completed_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', registration_flow_completed_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', registration_flow_completed_time::date+4)::date+2
                        END reg_complete_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
                        WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', first_risk_review_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_risk_review_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_risk_review_time::date+4)::date+2
                        END risk_review_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_decision_time::date+4)::date+2
                        WHEN datediff('day', first_decision_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', first_decision_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_decision_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_decision_time::date+4)::date+2
                        END first_dec_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
                        WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', first_approved_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_approved_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_approved_time::date+4)::date+2
                        END app_week_end_date

                , CASE WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                        WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 
                            AND datediff('day', first_draw_time::date, current_date()) <= 0 THEN NULL 
                        WHEN datediff('day', first_draw_time::date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
                        ELSE DATE_TRUNC('WEEK', first_draw_time::date+4)::date+2
                        END ftd_week_end_date
            
                , CASE WHEN is_underwriting_old = 1 THEN 1 
                       WHEN current_credit_status_reason in ('Onboarding dynamic decision reject') then 1
                       ELSE 0
                       END AS is_underwriting_new -- Uses current credit status reason instead of first_rejected_reason field (As in the customer level pull)
                
                , CASE WHEN first_risk_review_time IS NULL THEN 0 ELSE 1 END AS is_risk_review
                
                , MONTH(registration_time::date) - in_business_since_month + (YEAR(registration_time::date) - in_business_since_year)*12 pq_months_in_business
                , CASE WHEN IN_BUSINESS_SINCE_MONTH IS NOT NULL THEN 1 ELSE 0 END has_pq_months_in_business
                
                , calc_revenue
                , has_calc_revenue
                , is_approved
                , is_fraud
                
                , rejection_reason
                , CASE WHEN rejection_reason IS NOT NULL THEN rejection_reason
                        WHEN rejection_reason IS NULL and current_credit_status ='rejected' THEN 'Other Rejections'
                        WHEN rejection_reason IS NULL and current_credit_status !='rejected' THEN 'Not Rejected'
                        ELSE 'Check cases'
                        END AS rejection_reason_2
                , CASE WHEN rejection_reason_2 = 'Not Rejected' THEN 0	ELSE 1 END AS is_rejected
                , first_rejected_reason
                
                , is_ftu
                , is_ftd_0_7
                , is_ftd_8_28
                , is_ftd_29_60
                , is_ftd_61_
                , is_ftd7
                , is_ftd28
                , first_draw_amount
                , CASE WHEN datediff(day, first_approved_time::date, first_draw_time::date)<=60 THEN '<=60 Day Tenure'
                        WHEN datediff(day, first_approved_time::date, first_draw_time::date)>60 THEN '>60 Day Tenure'
                        END AS tenure
                
                , first_approved_credit_limit
                , NVL(first_approved_time, first_rejected_time) underwritten_time 
                , current_credit_status
                , current_credit_status_reason
            
                FROM 
                (SELECT *, ROW_NUMBER() OVER (PARTITION BY FBBID ORDER BY WEEK_END_DATE) AS rnk from analytics.credit.customer_level_data_td)
                WHERE rnk=1
                ) CD

            ON PA.fbbid=CD.fbbid
            )
            ;

-----------------------------------------------------------------------------FINAL AGG TABLE----------------------------------------------------------------------
    CREATE OR REPLACE TABLE indus.public.leads_agg_td AS
    (WITH leads AS 
        (SELECT *
            , CASE WHEN partner_name_upd = 'Lendio' THEN is_pre_approval
                    WHEN partner_name_upd = 'Nav' THEN is_pre_approval
                    WHEN partner_name_upd = 'AtoB' THEN is_pre_approval
                    WHEN partner_name_upd = 'Bluevine' THEN is_pre_approval
                    WHEN partner_name_upd = '1West' THEN is_pre_approval
                    WHEN partner_name_upd = 'sofi' THEN is_pre_approval
                    --WHEN partner_name = 'Cardiff' THEN is_pre_approval
                    WHEN partner_name_upd = 'ZenBusiness' THEN is_pre_approval
                    WHEN partner_name_upd = 'EverCommerce' THEN is_pre_approval
                    WHEN partner_name_upd = 'Anansii' THEN is_pre_approval
                    ELSE 0 END preapproval
            , CASE WHEN partner_name_upd in ('Lendio','Nav','AtoB','Bluevine','1West','sofi','Cardiff','ZenBusiness','Anansii','EverCommerce') 
                    THEN partner_name_upd ELSE NULL END preapproval_partner_flag
            , CASE WHEN customer_annual_revenue_group IS NULL then 'Other/No Data' ELSE customer_annual_revenue_group END customer_annual_revenue_group
            , CASE WHEN industry_type IS NULL then 'No Data' ELSE industry_type END industry_type_filled
            , CASE WHEN ob_bucket_group IS NULL then 'No Bucket' ELSE ob_bucket_group END ob_bucket_group_filled

        FROM INDUS.PUBLIC.LEADS_BASE_TABLE_TD)

    SELECT A.week_end_date
        , A.partner_name
        , A.channel_type_new
        , A.customer_revenue
        , A.preapproval_filter
        , A.bucket_group
        , A.industry_type
        ------- Horizontal Metrics
        , HOR.leads
        , HOR.leads_pa
        , HOR.pre_approvals
        , HOR.registrations
        , HOR.registrations_pa
        , HOR.cip_connected
        , HOR.cip_connected_pa
        , HOR.underwritten
        , HOR.underwritten_pa
        , HOR.underwritten_excl_fraud
        , HOR.underwritten_excl_fraud_pa
        , HOR.und_fraud
        , HOR.und_fraud_pa
        , HOR.approvals
        , HOR.approvals_pa
        , HOR.approval_match
        , HOR.approval_decline
        , HOR.credit_match
        , HOR.ftd_ever
        , HOR.ftd_ever_pa
        , HOR.ftd_7
        , HOR.ftd_7_pa
        , HOR.ftd_28
        , HOR.ftd_28_pa
        , HOR.reg_7
        , HOR.reg_7_pa
        , HOR.reg_14
        , HOR.reg_14_pa
        , HOR.reg_28
        , HOR.reg_28_pa
        , HOR.email_1_received
        , HOR.email_2_received
        , HOR.email_3_received
        , HOR.email_4_received
        , HOR.email_5_received
        , HOR.email_6_received
        , HOR.email_received
        , HOR.email_1_opened
        , HOR.email_2_opened
        , HOR.email_3_opened
        , HOR.email_4_opened
        , HOR.email_5_opened
        , HOR.email_6_opened
        , HOR.email_opened
        , HOR.pre_approved_credit_limit
        , HOR.approved_credit_limit
        , HOR.delta_credit_limit
        , HOR.delta_missed_cl_offers
        , HOR.not_overall_match
        , HOR.lead_fico
        , HOR.lead_fico_denom
        , HOR.pre_approved_fico
        , HOR.pre_approved_fico_denom
        , HOR.not_pre_approved_fico
        , HOR.not_pre_approved_fico_denom
        , HOR.underwritten_fico
        , HOR.underwritten_fico_denom
        -- , HOR.pre_approved_approved_fico
        -- , HOR.pre_approved_approved_fico_denom
        , HOR.leads_fico_below_600
        , HOR.leads_fico_600_649
        , HOR.leads_fico_650_699
        , HOR.leads_fico_700_749
        , HOR.leads_fico_above_750
        , HOR.pre_approved_fico_below_600
        , HOR.pre_approved_fico_600_649
        , HOR.pre_approved_fico_650_699
        , HOR.pre_approved_fico_700_749
        , HOR.pre_approved_fico_above_750
        , HOR.not_pre_approved_fico_below_600
        , HOR.not_pre_approved_fico_600_649
        , HOR.not_pre_approved_fico_650_699
        , HOR.not_pre_approved_fico_700_749
        , HOR.not_pre_approved_fico_above_750
        , HOR.underwritten_fico_below_600
        , HOR.underwritten_fico_600_649
        , HOR.underwritten_fico_650_699
        , HOR.underwritten_fico_700_749
        , HOR.underwritten_fico_above_750
        , HOR.approved_fico_below_600
        , HOR.approved_fico_600_649
        , HOR.approved_fico_650_699
        , HOR.approved_fico_700_749
        , HOR.approved_fico_above_750
        -- , HOR.PA_DECLINED_FICO_BELOW_600
        -- , HOR.PA_DECLINED_FICO_600_649
        -- , HOR.PA_DECLINED_FICO_650_699
        -- , HOR.PA_DECLINED_FICO_700_749
        -- , HOR.PA_DECLINED_FICO_ABOVE_750
        , HOR.leads_rev_below_30k
        , HOR.leads_rev_30k_100k
        , HOR.leads_rev_100k_250k
        , HOR.leads_rev_above_250k
        , HOR.pre_approved_rev_below_30k
        , HOR.pre_approved_rev_30k_100k
        , HOR.pre_approved_rev_100k_250k
        , HOR.pre_approved_rev_above_250k
        , HOR.not_pre_approved_rev_below_30k
        , HOR.not_pre_approved_rev_30k_100k
        , HOR.not_pre_approved_rev_100k_250k
        , HOR.not_pre_approved_rev_above_250k
        , HOR.underwritten_rev_below_30k
        , HOR.underwritten_rev_30k_100k
        , HOR.underwritten_rev_100k_250k
        , HOR.underwritten_rev_above_250k
        , HOR.approved_rev_below_30k
        , HOR.approved_rev_30k_100k
        , HOR.approved_rev_100k_250k
        , HOR.approved_rev_above_250k
        , HOR.first_approved_credit_limit
        , HOR.first_draw_amount
        , HOR.pre_approved_declined_fico_denom
        , HOR.lead_revenue
        , HOR.lead_revenue_denom
        , HOR.pre_approved_revenue
        , HOR.pre_approved_revenue_denom
        , HOR.not_pre_approved_revenue
        , HOR.not_pre_approved_revenue_denom
        , HOR.underwritten_revenue
        , HOR.underwritten_revenue_denom
        , HOR.pre_approved_approved_revenue
        , HOR.pre_approved_approved_revenue_denom
        , HOR.approved_fico
        , HOR.approved_fico_denom
        , HOR.approved_revenue
        , HOR.approved_revenue_denom
        , HOR.total_first_approved_credit_limit
        ------- Vertical Metrics
        -- Leads
        , L.leads_vert
        , L.leads_vert_pa
        , L.leads_fico_vert
        , L.leads_fico_denom_vert
        , L.leads_revenue_vert
        , L.leads_revenue_denom_vert
        , L.leads_fico_below_600_vert
        , L.leads_fico_600_649_vert
        , L.leads_fico_650_699_vert
        , L.leads_fico_700_749_vert
        , L.leads_fico_above_750_vert
        , L.leads_rev_below_30k_vert
        , L.leads_rev_30k_100k_vert
        , L.leads_rev_100k_250k_vert
        , L.leads_rev_above_250k_vert
        -- Prequal
        , PQ.pre_approvals_vert
        , PQ.PRE_APPROVED_CREDIT_LIMIT_VERT
        , PQ.not_pre_approved_fico_vert
        , PQ.not_pre_approved_fico_denom_vert
        , PQ.not_pre_approved_revenue_vert
        , PQ.not_pre_approved_revenue_denom_vert
        , PQ.not_pre_approved_fico_below_600_vert
        , PQ.not_pre_approved_fico_600_649_vert
        , PQ.not_pre_approved_fico_650_699_vert
        , PQ.not_pre_approved_fico_700_749_vert
        , PQ.not_pre_approved_fico_above_750_vert
        , PQ.pre_approved_fico_vert
        , PQ.pre_approved_fico_denom_vert
        , PQ.pre_approved_revenue_vert
        , PQ.pre_approved_revenue_denom_vert
        , PQ.pre_approved_fico_below_600_vert
        , PQ.pre_approved_fico_600_649_vert
        , PQ.pre_approved_fico_650_699_vert
        , PQ.pre_approved_fico_700_749_vert
        , PQ.pre_approved_fico_above_750_vert
        , PQ.pre_approved_rev_below_30k_VERT
        , PQ.pre_approved_rev_30k_100k_VERT
        , PQ.pre_approved_rev_100k_250k_VERT
        , PQ.pre_approved_rev_above_250k_VERT
        , PQ.not_pre_approved_rev_below_30k_VERT
        , PQ.not_pre_approved_rev_30k_100k_VERT
        , PQ.not_pre_approved_rev_100k_250k_VERT
        , PQ.not_pre_approved_rev_above_250k_VERT
        --- Registrations
        , R.registrations_vert
        , R.registrations_vert_pa
        , R.reg_7_vert
        , R.reg_7_pa_vert
        , R.reg_14_vert
        , R.reg_14_pa_vert
        , R.reg_28_vert
        , R.reg_28_pa_vert
        --- CIP
        , CIP.cip_connected_vert
        , CIP.cip_connected_vert_pa
        --- Underwritten
        , UW.underwritten_vert
        , UW.underwritten_vert_excl_fraud
        , UW.und_fraud_vert
        , UW.underwritten_vert_pa
        , UW.underwritten_vert_excl_fraud_pa
        , UW.und_fraud_vert_pa
        , UW.underwritten_fico_vert
        , UW.underwritten_revenue_vert
        , UW.underwritten_revenue_denom_vert
        , UW.underwritten_fico_denom_vert
        , UW.underwritten_fico_below_600_VERT
        , UW.underwritten_fico_600_649_VERT
        , UW.underwritten_fico_650_699_VERT
        , UW.underwritten_fico_700_749_VERT
        , UW.underwritten_fico_above_750_VERT
        -- , UW.underwritten_BL_fico_below_600_VERT
        -- , UW.underwritten_BL_fico_600_649_VERT
        -- , UW.underwritten_BL_fico_650_699_VERT
        -- , UW.underwritten_BL_fico_700_749_VERT
        -- , UW.underwritten_BL_fico_above_750_VERT
        -- , UW.underwritten_SBL_fico_below_600_VERT
        -- , UW.underwritten_SBL_fico_600_649_VERT
        -- , UW.underwritten_SBL_fico_650_699_VERT
        -- , UW.underwritten_SBL_fico_700_749_VERT
        -- , UW.underwritten_SBL_fico_above_750_VERT
        , UW.underwritten_rev_below_30k_VERT
        , UW.underwritten_rev_30k_100k_VERT
        , UW.underwritten_rev_100k_250k_VERT
        , UW.underwritten_rev_above_250k_VERT
        -- , UW.underwritten_BL_rev_below_30k_VERT
        -- , UW.underwritten_BL_rev_30k_100K_VERT
        -- , UW.underwritten_BL_rev_100k_250k_VERT
        -- , UW.underwritten_BL_rev_above_250k_VERT
        -- , UW.underwritten_SBL_rev_below_30k_VERT
        -- , UW.underwritten_SBL_rev_30k_100K_VERT
        -- , UW.underwritten_SBL_rev_100k_250k_VERT
        -- , UW.underwritten_SBL_rev_above_250k_VERT
        --- Approvals
        , APP.approvals_vert
        , APP.approvals_vert_pa
        , APP.credit_match_vert
        , APP.approved_credit_limit_vert
        , APP.delta_credit_limit_vert
        , APP.delta_missed_cl_offers_vert
        , APP.not_overall_match_vert
        -- , APP.pa_approved_fico_below_600_vert
        -- , APP.pa_approved_fico_600_649_vert
        -- , APP.pa_approved_fico_650_699_vert
        -- , APP.pa_approved_fico_700_749_vert
        -- , APP.pa_approved_fico_above_750_vert
        -- , APP.pa_approved_rev_below_30k_VERT
        -- , APP.pa_approved_rev_30k_100k_VERT
        -- , APP.pa_approved_rev_100k_250k_VERT
        -- , APP.pa_approved_rev_above_250k_VERT
        , APP.approved_fico_vert
        , APP.approved_fico_denom_vert
        , APP.approved_revenue_vert
        , APP.approved_revenue_denom_vert
        , APP.approved_fico_below_600_VERT
        , APP.approved_fico_600_649_VERT
        , APP.approved_fico_650_699_VERT
        , APP.approved_fico_700_749_VERT
        , APP.approved_fico_above_750_VERT
        -- , APP.approved_BL_fico_below_600_VERT
        -- , APP.approved_BL_fico_600_649_VERT
        -- , APP.approved_BL_fico_650_699_VERT
        -- , APP.approved_BL_fico_700_749_VERT
        -- , APP.approved_BL_fico_above_750_VERT
        -- , APP.approved_SBL_fico_below_600_VERT
        -- , APP.approved_SBL_fico_600_649_VERT
        -- , APP.approved_SBL_fico_650_699_VERT
        -- , APP.approved_SBL_fico_700_749_VERT
        -- , APP.approved_SBL_fico_above_750_VERT
        , APP.approved_rev_below_30k_VERT
        , APP.approved_rev_30k_100k_VERT
        , APP.approved_rev_100k_250k_VERT
        , APP.approved_rev_above_250k_VERT
        -- , APP.approved_BL_rev_below_30k_VERT
        -- , APP.approved_BL_rev_30k_100k_VERT
        -- , APP.approved_BL_rev_100k_250k_VERT
        -- , APP.approved_BL_rev_above_250k_VERT
        -- , APP.approved_SBL_rev_below_30k_VERT
        -- , APP.approved_SBL_rev_30k_100k_VERT
        -- , APP.approved_SBL_rev_100k_250k_VERT
        -- , APP.approved_SBL_rev_above_250k_VERT
        , APP.total_first_approved_credit_limit_vert
        --- First Time Draws
        , FTD.ftd_ever_vert
        , FTD.ftd_ever_vert_pa
        , FTD.ftd_7_vert
        , FTD.ftd_7_vert_pa
        , FTD.ftd_28_vert
        , FTD.ftd_28_vert_pa
        , FTD.first_draw_amount_vert

    FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_leads_td A

    ---------------------------------------------------------- HORIZONTAL METRICS ----------------------------------------------------------
    LEFT JOIN 
        (SELECT lead_week_end_date AS week_end_date
            , partner_name_upd
            , leads_annual_revenue_bucket
            , channel_type_new
            , ob_bucket_group_filled
            , preapproval_filter
            , industry_type_filled
            
            , COUNT(distinct fbbid) leads
            , COUNT(CASE WHEN preapproval_partner_flag is not null then fbbid
                        ELSE NULL end) AS leads_pa
                        
            , SUM(preapproval) AS pre_approvals
            
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN 1 ELSE 0 end) AS registrations
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN 1 ELSE 0 end) AS registrations_pa
        
            
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL AND cip_connected_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS cip_connected
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                        AND cip_connected_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS cip_connected_pa
            
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_underwriting_new ELSE 0 end) AS underwritten
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_underwriting_new 
                        ELSE 0 end) AS underwritten_pa
        
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL and fraud_compliance_excl <> 1 THEN is_underwriting_new 
                        ELSE 0 end) AS underwritten_excl_fraud
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag=1 AND registration_time IS NOT NULL AND fraud_compliance_excl <> 1 
                        THEN is_underwriting_new ELSE 0 END) as underwritten_excl_fraud_pa
        
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN fraud_compliance_excl ELSE 0 end) AS und_fraud
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                        THEN fraud_compliance_excl ELSE 0 end) AS und_fraud_pa
            
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL AND app_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS approvals
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                        AND app_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS approvals_pa
            
            , SUM(is_match_approval) approval_match
            , SUM(is_match_declined) approval_decline
            , SUM(is_match_credit_limit) credit_match
            
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftu ELSE 0 END) ftd_ever
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftu ELSE 0 END) ftd_ever_pa
        
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd7 ELSE 0 END) ftd_7
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd7 ELSE 0 END) ftd_7_pa
            , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd28 ELSE 0 END) ftd_28
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd28 ELSE 0 END) ftd_28_pa
        
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg7_pa ELSE 0 END) reg_7_pa
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg14_pa ELSE 0 END) reg_14_pa
            , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg28_pa ELSE 0 END) reg_28_pa
            , SUM(CASE WHEN pre_approval_flag=1 AND registration_time IS NOT NULL THEN is_reg7 ELSE 0 END) reg_7
            , SUM(CASE WHEN pre_approval_flag=1 AND registration_time IS NOT NULL THEN is_reg14 ELSE 0 END) reg_14
            , SUM(CASE WHEN pre_approval_flag=1 AND registration_time IS NOT NULL THEN is_reg28 ELSE 0 END) reg_28
        
            , SUM(CASE WHEN email_1_date IS NOT NULL THEN 1 ELSE 0 end) email_1_received 
            , SUM(CASE WHEN email_2_date IS NOT NULL THEN 1 ELSE 0 end) email_2_received 
            , SUM(CASE WHEN email_3_date IS NOT NULL THEN 1  ELSE 0 end) email_3_received 
            , SUM(CASE WHEN email_4_date IS NOT NULL THEN 1  ELSE 0 end) email_4_received 
            , SUM(CASE WHEN email_5_date IS NOT NULL THEN 1  ELSE 0 end) email_5_received 
            , SUM(CASE WHEN email_6_date IS NOT NULL THEN 1  ELSE 0 end) email_6_received 
            , SUM(CASE WHEN email_1_date IS NOT NULL 
                        OR email_2_date IS NOT NULL 
                        OR email_3_date IS NOT NULL 
                        OR email_4_date IS NOT NULL 
                        OR email_5_date IS NOT NULL 
                        OR email_6_date IS NOT NULL THEN 1  
                        ELSE 0 END) email_received
        
            , SUM(CASE WHEN email_1_opened IS NOT NULL THEN email_1_opened
                        WHEN email_2_opened IS NOT NULL THEN email_2_opened
                        WHEN email_3_opened IS NOT NULL THEN email_3_opened
                        WHEN email_4_opened IS NOT NULL THEN email_4_opened
                        WHEN email_5_opened IS NOT NULL THEN email_5_opened 
                        WHEN email_6_opened IS NOT NULL THEN email_6_opened
                        ELSE 0 END) AS email_opened
            , SUM(email_1_opened) email_1_opened
            , SUM(email_2_opened) email_2_opened
            , SUM(email_3_opened) email_3_opened
            , SUM(email_4_opened) email_4_opened
            , SUM(email_5_opened) email_5_opened
            , SUM(email_6_opened) email_6_opened
        
            , SUM(CASE WHEN is_pre_approval = 1 THEN pre_approved_credit_limit ELSE 0 end) pre_approved_credit_limit
            , SUM(CASE WHEN is_match_approval = 1 THEN first_approved_credit_limit ELSE 0 end) approved_credit_limit
            , SUM(CASE WHEN is_match_approval = 1 THEN first_approved_credit_limit - pre_approved_credit_limit ELSE 0 end) delta_credit_limit
            , SUM(CASE WHEN is_match_approval = 1 AND is_match_credit_limit = 0 
                    THEN first_approved_credit_limit - pre_approved_credit_limit ELSE 0 end) delta_missed_cl_offers
            , SUM(CASE WHEN is_match_approval = 1 AND is_match_credit_limit = 0 THEN 1 ELSE 0 end) not_overall_match
            
            , SUM (leads_fico) lead_fico --All leads FICO
            , SUM(CASE WHEN leads_fico IS NOT NULL THEN 1 ELSE 0 end) lead_fico_denom --All leads FICO denominator
            , SUM(CASE WHEN is_pre_approval = 1 THEN leads_fico ELSE 0 end) pre_approved_fico --Pre-approved FICO
            , SUM(CASE WHEN leads_fico IS NOT NULL THEN is_pre_approval ELSE 0 end) pre_approved_fico_denom --Pre-approved FICO denominator
            , SUM(CASE WHEN is_pre_approval = 0 THEN leads_fico ELSE 0 end) not_pre_approved_fico --Not pre-approved FICO
            , SUM(CASE WHEN leads_fico IS NOT NULL AND is_pre_approval = 0 THEN 1 ELSE 0 end) not_pre_approved_fico_denom --Not pre-approved FICO denominator
            , SUM(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 THEN fico_onboarding ELSE 0 end) underwritten_fico --UW FICO
            , SUM(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 AND fico_onboarding IS NOT NULL THEN 1 ELSE 0 END) underwritten_fico_denom --UW FICO denominator
            , SUM(CASE WHEN is_pre_approval = 1 AND leads_fico IS NOT NULL THEN is_approved ELSE 0 end) pre_approved_approved_fico_denom  --Approved FICO denominator
            , SUM(CASE WHEN is_pre_approval = 1 AND is_approved=1 THEN leads_fico ELSE 0 end) pre_approved_approved_fico --Approved FICO 
        
            -- Leads FICO distribution
            , COUNT(CASE WHEN leads_fico IS NOT NULL AND leads_fico < 600 THEN fbbid ELSE NULL END) leads_fico_below_600
            , count(CASE WHEN leads_fico IS NOT NULL AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) leads_fico_600_649
            , count(CASE WHEN leads_fico IS NOT NULL  AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) leads_fico_650_699
            , count(CASE WHEN leads_fico IS NOT NULL  AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) leads_fico_700_749
            , count(CASE WHEN leads_fico IS NOT NULL  AND leads_fico >= 750 THEN fbbid ELSE NULL END) leads_fico_above_750
            -- Pre-approved FICO distribution
            , COUNT(CASE WHEN is_pre_approval = 1 AND leads_fico < 600 THEN fbbid ELSE NULL END) pre_approved_fico_below_600
            , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) pre_approved_fico_600_649
            , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) pre_approved_fico_650_699
            , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) pre_approved_fico_700_749
            , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 750 THEN fbbid ELSE NULL END) pre_approved_fico_above_750
            -- Not pre-approved FICO distribution
            , COUNT(CASE WHEN is_pre_approval = 0 AND leads_fico < 600 THEN fbbid ELSE NULL END) not_pre_approved_fico_below_600
            , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) not_pre_approved_fico_600_649
            , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) not_pre_approved_fico_650_699
            , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) not_pre_approved_fico_700_749
            , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 750 THEN fbbid ELSE NULL END) not_pre_approved_fico_above_750
            -- UW FICO distribution 
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding < 600 THEN fbbid ELSE NULL END) underwritten_fico_below_600
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 600 
                        AND fico_onboarding < 650 THEN fbbid ELSE NULL END) underwritten_fico_600_649
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 650 
                        AND fico_onboarding < 700 THEN fbbid ELSE NULL END) underwritten_fico_650_699
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 700 
                        AND fico_onboarding < 750 THEN fbbid ELSE NULL END) underwritten_fico_700_749
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 750 THEN fbbid ELSE NULL END) underwritten_fico_above_750
            -- Approved FICO distribution
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding < 600 THEN fbbid ELSE NULL END) approved_fico_below_600
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 600 AND fico_onboarding < 650 THEN fbbid ELSE NULL END) approved_fico_600_649
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 650 AND fico_onboarding < 700 THEN fbbid ELSE NULL END) approved_fico_650_699
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 700 AND fico_onboarding < 750 THEN fbbid ELSE NULL END) approved_fico_700_749
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 750 THEN fbbid ELSE NULL END) approved_fico_above_750
            
            -- // See where this is used!!!!!!!!!!!!!
            -- , count(CASE WHEN is_pre_approval = 1 AND is_match_declined = 1 AND leads_fico < 600 THEN fbbid ELSE NULL END) PA_DECLINED_FICO_BELOW_600
            -- , count(CASE WHEN is_pre_approval = 1 AND is_match_declined = 1 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) PA_DECLINED_FICO_600_649
            -- , count(CASE WHEN is_pre_approval = 1 AND is_match_declined = 1 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) PA_DECLINED_FICO_650_699
            -- , count(CASE WHEN is_pre_approval = 1 AND is_match_declined = 1 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) PA_DECLINED_FICO_700_749
            -- , count(CASE WHEN is_pre_approval = 1 AND is_match_declined = 1 AND leads_fico >= 750 THEN fbbid ELSE NULL END) PA_DECLINED_FICO_ABOVE_750
        
            -- Leads revenue distribution
            , COUNT(CASE WHEN leads_annual_revenue IS NOT NULL AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) leads_rev_below_30k
            , count(CASE WHEN leads_annual_revenue IS NOT NULL AND leads_annual_revenue >= 30000 AND leads_annual_revenue < 100000 
                        THEN fbbid ELSE NULL END) leads_rev_30k_100k
            , count(CASE WHEN leads_annual_revenue IS NOT NULL  AND leads_annual_revenue >= 100000 AND leads_annual_revenue < 250000 
                        THEN fbbid ELSE NULL END) leads_rev_100k_250k
            , count(CASE WHEN leads_annual_revenue IS NOT NULL  AND leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) leads_rev_above_250k
        
            -- Pre-approved revenue distribution
            , COUNT(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) pre_approved_rev_below_30k
            , count(CASE WHEN is_pre_approval = 1  AND leads_annual_revenue >= 30000 AND leads_annual_revenue < 100000 
                        THEN fbbid ELSE NULL END) pre_approved_rev_30k_100k
            , count(CASE WHEN is_pre_approval = 1   AND leads_annual_revenue >= 100000 AND leads_annual_revenue < 250000 
                        THEN fbbid ELSE NULL END) pre_approved_rev_100k_250k
            , count(CASE WHEN is_pre_approval = 1   AND leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) pre_approved_rev_above_250k
        
            -- Not pre-approved revenue distribution
            , COUNT(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) not_pre_approved_rev_below_30k
            , count(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue >= 30000 AND leads_annual_revenue < 100000 
                        THEN fbbid ELSE NULL END) not_pre_approved_rev_30k_100k
            , count(CASE WHEN is_pre_approval = 0   AND leads_annual_revenue >= 100000 AND leads_annual_revenue < 250000 
                        THEN fbbid ELSE NULL END) not_pre_approved_rev_100k_250k
            , count(CASE WHEN is_pre_approval = 0   AND leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) not_pre_approved_rev_above_250k
            
            -- UW revenue distribution -- idk why they switch to customer annual revenue for UW
            , COUNT(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue < 30000 THEN fbbid ELSE NULL END) underwritten_rev_below_30k
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue >= 30000 AND calc_revenue < 100000 
                        THEN fbbid ELSE NULL END) underwritten_rev_30k_100k
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue >= 100000 AND calc_revenue < 250000 
                        THEN fbbid ELSE NULL END) underwritten_rev_100k_250k
            , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue >= 250000 THEN fbbid ELSE NULL END) underwritten_rev_above_250k
        
            -- Approved revenue distribution -- idk why they switch to customer annual revenue for UW
            , COUNT(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue < 30000 THEN fbbid ELSE NULL END) approved_rev_below_30k
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 30000 AND calc_revenue < 100000 
                        THEN fbbid ELSE NULL END) approved_rev_30k_100k
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 100000 AND calc_revenue < 250000 
                        THEN fbbid ELSE NULL END) approved_rev_100k_250k
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 250000 THEN fbbid ELSE NULL END) approved_rev_above_250k

            , sum(CASE WHEN partner_name_upd = 'Lendio' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'Nav' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'AtoB' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'Bluevine' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = '1West' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'sofi' AND is_match_approval = 1 THEN first_approved_credit_limit
                       -- WHEN partner_name = 'Cardiff' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'ZenBusiness' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'EverCommerce' AND is_match_approval = 1 THEN first_approved_credit_limit
                        -- WHEN partner_name = 'InvoiceSimple' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'Anansii' AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'BusinessLoans' AND is_approved = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'Cardiff' AND is_approved = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd = 'smallbusinessloans' AND is_approved = 1 THEN first_approved_credit_limit
                        ELSE 0 end) first_approved_credit_limit

            , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd in ('BusinessLoans', 'smallbusinessloans','Cardiff') AND is_approved = 1 THEN FIRST_APPROVED_CREDIT_LIMIT
                        ELSE 0 end) total_first_approved_credit_limit
        
            , sum(first_draw_amount) first_draw_amount
        
            , SUM(CASE WHEN leads_fico IS NOT NULL THEN is_match_declined ELSE 0 end) pre_approved_declined_fico_denom --- see if this is later used or not 
        
            , SUM (leads_annual_revenue) lead_revenue --All leads revenue
            , SUM(CASE WHEN leads_annual_revenue IS NOT NULL THEN 1 ELSE 0 end) lead_revenue_denom --All leads revenue denominator
            , SUM(CASE WHEN is_pre_approval = 1 THEN leads_annual_revenue ELSE 0 end) pre_approved_revenue --Pre-approved revenue
            , SUM(CASE WHEN leads_annual_revenue IS NOT NULL THEN is_pre_approval ELSE 0 end) pre_approved_revenue_denom --Pre-approved revenue denominator
            , SUM(CASE WHEN is_pre_approval = 0 THEN leads_annual_revenue ELSE 0 end) not_pre_approved_revenue --Not pre-approved revenue
            , SUM(CASE WHEN leads_annual_revenue IS NOT NULL AND is_pre_approval = 0 THEN 1 ELSE 0 end) not_pre_approved_revenue_denom --Not pre-approved revenue denominator
            , SUM(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 THEN calc_revenue ELSE 0 end) underwritten_revenue --UW revenue
            , SUM(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 AND calc_revenue 
                    IS NOT NULL THEN 1 ELSE 0 END) underwritten_revenue_denom --UW revenue denominator
            , SUM(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue IS NOT NULL 
                    THEN is_approved ELSE 0 end) pre_approved_approved_revenue_denom --Approved revenue denominator
            , SUM(CASE WHEN is_pre_approval = 1 AND is_approved=1 THEN leads_annual_revenue ELSE 0 end) pre_approved_approved_revenue --Approved revenue 
            
            -- , SUM(CASE WHEN is_pre_approval = 1 THEN leads_annual_revenue ELSE 0 end) pre_approved_revenue
            -- , SUM(CASE WHEN leads_annual_revenue IS NOT NULL THEN is_pre_approval ELSE 0 end) pre_approved_revenue_denom
            -- , SUM(CASE WHEN leads_annual_revenue IS NOT NULL THEN is_approved ELSE 0 end) pre_approved_approved_revenue_denom
            -- , SUM(CASE WHEN leads_annual_revenue IS NOT NULL THEN is_match_declined ELSE 0 end) pre_approved_declined_revenue_denom 
        
            , SUM(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 THEN fico_onboarding ELSE 0 end) approved_fico
            , SUM(CASE WHEN pre_approval_flag = 1 AND fico_onboarding is not null then is_approved ELSE 0 end) approved_fico_denom

            , SUM(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 THEN calc_revenue ELSE 0 end) approved_revenue
            , SUM(CASE WHEN pre_approval_flag = 1 AND calc_revenue is not null THEN is_approved ELSE 0 end) approved_revenue_denom
        
            FROM leads
            GROUP BY 1,2,3,4,5,6, 7
            ) HOR
        ON A.week_end_date = HOR.week_end_date
        AND A.partner_name = HOR.partner_name_upd
        AND A.channel_type_new = HOR.channel_type_new
        AND A.preapproval_filter = HOR.preapproval_filter
        AND A.customer_revenue = HOR.leads_annual_revenue_bucket
        AND A.BUCKET_GROUP = HOR.ob_bucket_group_filled
        AND A.industry_type = HOR.industry_type_filled

    ---------------------------------------------------------- Vertical Metrics ----------------------------------------------------------
    --- Leads stage     
        LEFT JOIN
            (SELECT lead_week_end_date AS week_end_date
                , partner_name_upd
                , channel_type_new
                , industry_type_filled
                , leads_annual_revenue_bucket
                , ob_bucket_group_filled
                , preapproval_filter
                , count(DISTINCT fbbid ) AS leads_vert
                , count(CASE WHEN preapproval_partner_flag IS NOT NULL THEN fbbid END) AS leads_vert_pa

                --Leads FICO and Revenue
                , sum(CASE WHEN leads_fico IS NOT NULL THEN leads_fico ELSE 0 end) leads_fico_vert
                , sum(CASE WHEN leads_fico IS NOT NULL THEN 1 ELSE 0 end) leads_fico_denom_vert
                
                , sum(CASE WHEN leads_annual_revenue IS NOT NULL THEN leads_annual_revenue ELSE 0 end) leads_revenue_vert
                , sum(CASE WHEN leads_annual_revenue IS NOT NULL THEN 1 ELSE 0 end) leads_revenue_denom_vert 
                
                , count(CASE WHEN leads_fico < 600 THEN fbbid ELSE NULL END) leads_fico_below_600_vert
                , count(CASE WHEN leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) leads_fico_600_649_vert
                , count(CASE WHEN leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) leads_fico_650_699_vert
                , count(CASE WHEN leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) leads_fico_700_749_vert
                , count(CASE WHEN leads_fico >= 750 THEN fbbid ELSE NULL END) leads_fico_above_750_vert

                , count(CASE WHEN leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) leads_rev_below_30k_vert
                , count(CASE WHEN leads_annual_revenue >= 30000 AND leads_annual_revenue < 100000 THEN fbbid ELSE NULL END) leads_rev_30k_100k_vert
                , count(CASE WHEN leads_annual_revenue >= 100000 AND leads_annual_revenue < 250000 THEN fbbid ELSE NULL END) leads_rev_100k_250k_vert
                , count(CASE WHEN leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) leads_rev_above_250k_vert
                
                FROM leads
                GROUP BY 1,2,3,4,5,6,7
                ) L
        ON A.week_end_date = L.week_end_date
        AND A.partner_name = L.partner_name_upd
        AND A.channel_type_new = L.channel_type_new
        AND A.preapproval_filter = L.preapproval_filter
        AND A.customer_revenue = L.leads_annual_revenue_bucket
        AND A.BUCKET_GROUP = L.ob_bucket_group_filled
        AND A.industry_type = L.industry_type_filled

    --- Preapproval stage
        LEFT JOIN 
            (SELECT prequal_week_end_date
                , partner_name_upd
            
                , channel_type_new
                , industry_type_filled
                , leads_annual_revenue_bucket
                , ob_bucket_group_filled
                , preapproval_filter
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL THEN is_pre_approval ELSE 0 end)  AS PRE_APPROVALS_VERT
                    
                , sum(CASE WHEN is_pre_approval = 1 THEN PRE_APPROVED_CREDIT_LIMIT ELSE 0 end) PRE_APPROVED_CREDIT_LIMIT_VERT
                , sum(CASE WHEN is_pre_approval = 1 THEN leads_fico
                            ELSE 0 end) PRE_APPROVED_FICO_VERT
                , sum(CASE WHEN leads_fico IS NOT NULL THEN IS_PRE_APPROVAL
                            ELSE 0 end) PRE_APPROVED_FICO_DENOM_VERT
                    
                , sum(CASE WHEN is_pre_approval = 1 THEN leads_annual_revenue ELSE 0 end) pre_approved_revenue_VERT
                , sum(CASE WHEN leads_annual_revenue IS NOT NULL THEN IS_PRE_APPROVAL ELSE 0 end) PRE_APPROVED_REVENUE_DENOM_VERT 
                    
                , count(CASE WHEN is_pre_approval = 1 AND leads_fico < 600 THEN fbbid ELSE NULL END) pre_approved_fico_below_600_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) pre_approved_fico_600_649_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) pre_approved_fico_650_699_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) pre_approved_fico_700_749_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_fico >= 750 THEN fbbid ELSE NULL END) pre_approved_fico_above_750_VERT
                    
                , count(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) pre_approved_rev_below_30k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue >= 30000 
                                AND leads_annual_revenue < 100000 THEN fbbid ELSE NULL END) pre_approved_rev_30k_100k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue >= 100000 
                                AND leads_annual_revenue < 250000 THEN fbbid ELSE NULL END) pre_approved_rev_100k_250k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) pre_approved_rev_above_250k_VERT

                --Not pre-approved FICO and Revenue
                , sum(CASE WHEN is_pre_approval = 0 THEN leads_fico ELSE 0 end) not_pre_approved_fico_vert
                , sum(CASE WHEN leads_fico IS NOT NULL AND is_pre_approval = 0 THEN 1 ELSE 0 end) not_pre_approved_fico_denom_vert
                
                , sum(CASE WHEN is_pre_approval = 0 THEN leads_annual_revenue ELSE 0 end) not_pre_approved_revenue_vert
                , sum(CASE WHEN is_pre_approval = 0 THEN 1 ELSE 0 end) not_pre_approved_revenue_denom_vert 
                
                , count(CASE WHEN is_pre_approval = 0 AND leads_fico < 600 THEN fbbid ELSE NULL END) not_pre_approved_fico_below_600_vert
                , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) not_pre_approved_fico_600_649_vert
                , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) not_pre_approved_fico_650_699_vert
                , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) not_pre_approved_fico_700_749_vert
                , count(CASE WHEN is_pre_approval = 0 AND leads_fico >= 750 THEN fbbid ELSE NULL END) not_pre_approved_fico_above_750_vert

                , count(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) not_pre_approved_rev_below_30k_VERT
                , count(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue >= 30000 
                                AND leads_annual_revenue < 100000 THEN fbbid ELSE NULL END) not_pre_approved_rev_30k_100k_VERT
                , count(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue >= 100000 
                                AND leads_annual_revenue < 250000 THEN fbbid ELSE NULL END) not_pre_approved_rev_100k_250k_VERT
                , count(CASE WHEN is_pre_approval = 0 AND leads_annual_revenue >= 250000 THEN fbbid ELSE NULL END) not_pre_approved_rev_above_250k_VERT

                
                FROM leads
                GROUP BY 1,2,3,4,5,6,7
                ) 
                PQ 
            ON A.week_end_date = PQ.prequal_week_end_date
            AND A.partner_name =  PQ.partner_name_upd
            AND A.channel_type_new = PQ.channel_type_new
            AND A.preapproval_filter = PQ.preapproval_filter
            AND A.customer_revenue = PQ.leads_annual_revenue_bucket
            AND A.BUCKET_GROUP = PQ.ob_bucket_group_filled
            AND A.industry_type = PQ.industry_type_filled

    --- Registration funnel
        LEFT JOIN
            (SELECT reg_start_week_end_date
                , partner_name_upd	
                , CHANNEL_TYPE_NEW
                , preapproval_filter
                , industry_type_filled
                , ob_bucket_group_filled
                , leads_annual_revenue_bucket
                , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN 1 ELSE 0 end) AS registrations_vert
                , SUM(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag=1 AND registration_time IS NOT NULL THEN 1 ELSE 0 end) AS registrations_vert_pa
                
                , SUM(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg7 ELSE 0 END) reg_7_vert
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                            AND registration_time IS NOT NULL THEN is_reg7_pa ELSE 0 END) reg_7_pa_vert
        
                , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg14 ELSE 0 END) reg_14_vert
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                            AND registration_time IS NOT NULL THEN is_reg14_pa ELSE 0 END) reg_14_pa_vert
        
                , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_reg28 ELSE 0 END) reg_28_vert
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1
                            AND registration_time IS NOT NULL THEN is_reg28_pa ELSE 0 END) reg_28_pa_vert
            FROM leads
            GROUP BY 1,2,3,4, 5,6,7
            ) R
        ON A.week_end_date = R.reg_start_week_end_date
        AND A.partner_name =  R.partner_name_upd
        AND A.channel_type_new = R.channel_type_new
        AND A.preapproval_filter = R.preapproval_filter
        AND A.customer_revenue = R.leads_annual_revenue_bucket
        AND A.BUCKET_GROUP = R.ob_bucket_group_filled
        AND A.industry_type = R.industry_type_filled

    --- CIP Connected    
        LEFT JOIN 
            (SELECT cip_connected_week_end_date
                    , partner_name_upd
                    , CHANNEL_TYPE_NEW
                    , ob_bucket_group_filled
                    , preapproval_filter
                    , industry_type_filled
                    , leads_annual_revenue_bucket
                    , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL 
                            AND cip_connected_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS cip_connected_vert
                    , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                            AND cip_connected_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS cip_connected_vert_pa
        
            FROM leads
            GROUP BY 1,2,3,4,5,6,7) CIP
        ON A.week_end_date = CIP.cip_connected_week_end_date
        AND A.partner_name =  CIP.partner_name_upd
        AND A.channel_type_new = CIP.channel_type_new
        AND A.preapproval_filter = CIP.preapproval_filter
        AND A.customer_revenue = CIP.leads_annual_revenue_bucket
        AND A.BUCKET_GROUP = CIP.ob_bucket_group_filled
        AND A.industry_type = CIP.industry_type_filled

    --- Underwritten
        LEFT JOIN 
            (SELECT reg_complete_week_end_date
                , partner_name_upd	
                , CHANNEL_TYPE_NEW
                , leads_annual_revenue_bucket
                , ob_bucket_group_filled
                , industry_type_filled
                , preapproval_filter
                
                , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_underwriting_new ELSE 0 end) AS underwritten_vert
                , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL and fraud_compliance_excl <> 1 THEN is_underwriting_new 
                            ELSE 0 end) AS underwritten_vert_excl_fraud
                , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN fraud_compliance_excl
                            ELSE 0 end) AS und_fraud_vert
            
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                            THEN is_underwriting_new ELSE 0 end) AS underwritten_vert_pa
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                            AND registration_time IS NOT NULL and fraud_compliance_excl <> 1 THEN is_underwriting_new 
                            ELSE 0 end) AS underwritten_vert_excl_fraud_pa
                , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL
                            THEN fraud_compliance_excl ELSE 0 end) AS und_fraud_vert_pa
            
                , sum(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 THEN fico_onboarding ELSE 0 end) underwritten_fico_vert
                , sum(CASE WHEN pre_approval_flag = 1 AND is_underwriting_new = 1 THEN calc_revenue ELSE 0 end) underwritten_revenue_vert
                , sum(CASE WHEN pre_approval_flag = 1 AND calc_revenue is not null and is_underwriting_new =1 then 1 ELSE 0 end) underwritten_revenue_denom_vert
                , sum(CASE WHEN pre_approval_flag = 1 AND fico_onboarding is not null and is_underwriting_new = 1 then 1 ELSE 0 end) underwritten_fico_denom_vert
            
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 
                            AND fico_onboarding < 600 THEN fbbid ELSE NULL END) underwritten_fico_below_600_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 600 
                            AND fico_onboarding < 650 THEN fbbid ELSE NULL END) underwritten_fico_600_649_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 650 
                            AND fico_onboarding < 700 THEN fbbid ELSE NULL END) underwritten_fico_650_699_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND fico_onboarding >= 700 
                            AND fico_onboarding < 750 THEN fbbid ELSE NULL END) underwritten_fico_700_749_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1
                            AND fico_onboarding >= 750 THEN fbbid ELSE NULL END) underwritten_fico_above_750_VERT
            
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue < 30000 THEN fbbid ELSE NULL END) underwritten_rev_below_30k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue >= 30000 
                            AND calc_revenue < 100000 THEN fbbid ELSE NULL END) underwritten_rev_30k_100k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 AND calc_revenue >= 100000 
                            AND calc_revenue < 250000 THEN fbbid ELSE NULL END) underwritten_rev_100k_250k_VERT
                , count(CASE WHEN is_pre_approval = 1 AND is_underwriting_new = 1 
                            AND calc_revenue >= 250000 THEN fbbid ELSE NULL END) underwritten_rev_above_250k_VERT

            FROM leads
            GROUP BY 1,2,3,4,5,6,7) UW
        ON A.week_end_date = UW.reg_complete_week_end_date
        AND A.partner_name =  UW.partner_name_upd
        AND A.channel_type_new = UW.channel_type_new
        AND A.preapproval_filter = UW.preapproval_filter
        AND A.customer_revenue = UW.leads_annual_revenue_bucket
        AND A.bucket_group = UW.ob_bucket_group_filled
        AND A.industry_type = UW.industry_type_filled

    --- Approval stage
    LEFT JOIN 
        (SELECT app_week_end_date
            , partner_name_upd
            , channel_type_new
            , preapproval_filter
            , industry_type_filled
            , ob_bucket_group_filled
            , leads_annual_revenue_bucket
            
            , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL AND app_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS approvals_vert
            , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 AND registration_time IS NOT NULL 
                        AND app_week_end_date IS NOT NULL THEN 1 ELSE 0 end) AS approvals_vert_pa

            , sum(is_match_credit_limit) credit_match_vert
            , sum(CASE WHEN is_match_approval = 1 THEN first_approved_credit_limit ELSE 0 end) approved_credit_limit_vert
            , sum(CASE WHEN is_match_approval = 1 THEN first_approved_credit_limit - pre_approved_credit_limit ELSE 0 end) delta_credit_limit_vert
            , sum(CASE WHEN is_match_approval = 1 AND is_match_credit_limit = 0 THEN first_approved_credit_limit - pre_approved_credit_limit
                        ELSE 0 end) delta_missed_cl_offers_vert
            , sum(CASE WHEN is_match_approval = 1 AND is_match_credit_limit = 0 THEN 1 ELSE 0 end) not_overall_match_vert

            -- , count(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 AND leads_fico < 600 THEN fbbid ELSE NULL END) pa_approved_fico_below_600_vert
            -- , count(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 AND leads_fico >= 600 AND leads_fico < 650 THEN fbbid ELSE NULL END) pa_approved_fico_600_649_vert
            -- , count(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 AND leads_fico >= 650 AND leads_fico < 700 THEN fbbid ELSE NULL END) pa_approved_fico_650_699_vert
            -- , count(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 AND leads_fico >= 700 AND leads_fico < 750 THEN fbbid ELSE NULL END) pa_approved_fico_700_749_vert
            -- , count(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 AND leads_fico >= 750 THEN fbbid ELSE NULL END) pa_approved_fico_above_750_vert

            -- , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND leads_annual_revenue < 30000 THEN fbbid ELSE NULL END) approved_rev_below_30k_VERT
            -- , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND leads_annual_revenue >= 30000 
            --              AND leads_annual_revenue < 100000 THEN fbbid ELSE NULL END) approved_rev_30k_100k_VERT
            -- , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND leads_annual_revenue >= 100000 
            --              AND leads_annual_revenue < 250000 THEN fbbid ELSE NULL END) approved_rev_100k_250k_VERT
            -- , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND leads_annual_revenue >= 250000 
            --              THEN fbbid ELSE NULL END) approved_rev_above_250k_VERT

            , sum(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 THEN leads_fico ELSE 0 end) approved_fico_vert
            , sum(CASE WHEN pre_approval_flag = 1 AND is_approved = 1 THEN leads_annual_revenue ELSE 0 end) approved_revenue_vert
            , sum(CASE WHEN is_pre_approval = 1 and leads_fico IS NOT NULL THEN is_approved ELSE 0 end) approved_fico_denom_vert
            , sum(CASE WHEN is_pre_approval = 1 and leads_annual_revenue IS NOT NULL THEN is_approved
                        ELSE 0 end) approved_revenue_denom_vert
                
                -- , sum(CASE WHEN is_pre_approval = 1 and leads_fico IS NOT NULL THEN is_match_declined ELSE 0 end) PRE_APPROVED_DECLINED_FICO_DENOM_VERT
                -- , sum(CASE WHEN is_pre_approval = 1 and leads_annual_revenue IS NOT NULL THEN is_match_declined ELSE 0 end) PRE_APPROVED_DECLINE_REVENUE_DENOM_VERT

            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 
                            AND fico_onboarding < 600 THEN fbbid ELSE NULL END) approved_fico_below_600_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 600 
                            AND fico_onboarding < 650 THEN fbbid ELSE NULL END) approved_fico_600_649_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 650 
                            AND fico_onboarding < 700 THEN fbbid ELSE NULL END) approved_fico_650_699_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND fico_onboarding >= 700     
                            AND fico_onboarding < 750 THEN fbbid ELSE NULL END) approved_fico_700_749_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 
                            AND fico_onboarding >= 750 THEN fbbid ELSE NULL END) approved_fico_above_750_VERT

            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue < 30000 
                            THEN fbbid ELSE NULL END) approved_rev_below_30k_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 30000 
                            AND calc_revenue < 100000 THEN fbbid ELSE NULL END) approved_rev_30k_100k_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 100000 
                            AND calc_revenue < 250000 THEN fbbid ELSE NULL END) approved_rev_100k_250k_VERT
            , count(CASE WHEN is_pre_approval = 1 AND is_approved = 1 AND calc_revenue >= 250000 
                            THEN fbbid ELSE NULL END) approved_rev_above_250k_VERT

            , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND is_match_approval = 1 THEN first_approved_credit_limit
                        WHEN partner_name_upd in ('BusinessLoans', 'smallbusinessloans','Cardiff') AND is_approved = 1 THEN FIRST_APPROVED_CREDIT_LIMIT
                        ELSE 0 end) total_first_approved_credit_limit_vert
                
            FROM leads
            GROUP BY 1,2,3,4,5,6,7) APP
        ON A.week_end_date = APP.app_week_end_date
        AND A.partner_name = APP.partner_name_upd
        AND A.channel_type_new = APP.channel_type_new
        AND A.preapproval_filter = APP.preapproval_filter
        AND A.customer_revenue = APP.leads_annual_revenue_bucket
        AND A.bucket_group = APP.ob_bucket_group_filled
        AND A.industry_type = APP.industry_type_filled

    LEFT JOIN 
        (SELECT ftd_week_end_date
            , partner_name_upd	
            , CHANNEL_TYPE_NEW
            , preapproval_filter
            , ob_bucket_group_filled
            , industry_type_filled
            , leads_annual_revenue_bucket
            
            , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftu ELSE 0 END) ftd_ever_vert
            , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                        AND registration_time IS NOT NULL THEN IS_FTU ELSE 0 END) ftd_ever_vert_pa
            
            , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd7 ELSE 0 END) ftd_7_vert
            , sum(CASE WHEN  preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                        AND registration_time IS NOT NULL THEN is_ftd7 ELSE 0 END) ftd_7_vert_pa
            
            , sum(CASE WHEN pre_approval_flag = 1 AND registration_time IS NOT NULL THEN is_ftd28 ELSE 0 END) ftd_28_vert
            , sum(CASE WHEN preapproval_partner_flag IS NOT NULL AND pre_approval_flag = 1 
                        AND registration_time IS NOT NULL THEN is_ftd28 ELSE 0 END) ftd_28_vert_pa
            
            , sum(first_draw_amount) first_draw_amount_vert

            FROM leads
            GROUP BY 1,2,3,4,5,6,7) FTD
        ON A.week_end_date = FTD.ftd_week_end_date
        AND A.partner_name = FTD.partner_name_upd
        AND A.channel_type_new = FTD.channel_type_new
        AND A.preapproval_filter = FTD.preapproval_filter
        AND A.customer_revenue = FTD.leads_annual_revenue_bucket
        AND A.bucket_group = FTD.ob_bucket_group_filled
        AND A.industry_type = FTD.industry_type_filled 
    -- Filtering to remove rows with no info to make tab generation quicker
    WHERE ( HOR.leads IS NOT NULL
        OR HOR.leads_pa IS NOT NULL
        OR HOR.pre_approvals IS NOT NULL
        OR HOR.registrations IS NOT NULL
        OR HOR.registrations_pa IS NOT NULL
        OR HOR.cip_connected IS NOT NULL
        OR HOR.cip_connected_pa IS NOT NULL
        OR HOR.underwritten IS NOT NULL
        OR HOR.underwritten_pa IS NOT NULL
        OR HOR.underwritten_excl_fraud IS NOT NULL
        OR HOR.underwritten_excl_fraud_pa IS NOT NULL
        OR HOR.und_fraud IS NOT NULL
        OR HOR.und_fraud_pa IS NOT NULL
        OR HOR.approvals IS NOT NULL
        OR HOR.approvals_pa IS NOT NULL
        OR HOR.approval_match IS NOT NULL
        OR HOR.approval_decline IS NOT NULL
        OR HOR.credit_match IS NOT NULL
        OR HOR.ftd_ever IS NOT NULL
        OR HOR.ftd_ever_pa IS NOT NULL
        OR HOR.ftd_7 IS NOT NULL
        OR HOR.ftd_7_pa IS NOT NULL
        OR HOR.ftd_28 IS NOT NULL
        OR HOR.ftd_28_pa IS NOT NULL
        OR HOR.reg_7 IS NOT NULL
        OR HOR.reg_7_pa IS NOT NULL
        OR HOR.reg_14 IS NOT NULL
        OR HOR.reg_14_pa IS NOT NULL
        OR HOR.reg_28 IS NOT NULL
        OR HOR.reg_28_pa IS NOT NULL
        OR HOR.email_1_received IS NOT NULL
        OR HOR.email_2_received IS NOT NULL
        OR HOR.email_3_received IS NOT NULL
        OR HOR.email_4_received IS NOT NULL
        OR HOR.email_5_received IS NOT NULL
        OR HOR.email_6_received IS NOT NULL
        OR HOR.email_received IS NOT NULL
        OR HOR.email_1_opened IS NOT NULL
        OR HOR.email_2_opened IS NOT NULL
        OR HOR.email_3_opened IS NOT NULL
        OR HOR.email_4_opened IS NOT NULL
        OR HOR.email_5_opened IS NOT NULL
        OR HOR.email_6_opened IS NOT NULL
        OR HOR.email_opened IS NOT NULL
        OR HOR.pre_approved_credit_limit IS NOT NULL
        OR HOR.approved_credit_limit IS NOT NULL
        OR HOR.delta_credit_limit IS NOT NULL
        OR HOR.delta_missed_cl_offers IS NOT NULL
        OR HOR.not_overall_match IS NOT NULL
        OR HOR.lead_fico IS NOT NULL
        OR HOR.lead_fico_denom IS NOT NULL
        OR HOR.pre_approved_fico IS NOT NULL
        OR HOR.pre_approved_fico_denom IS NOT NULL
        OR HOR.not_pre_approved_fico IS NOT NULL
        OR HOR.not_pre_approved_fico_denom IS NOT NULL
        OR HOR.underwritten_fico IS NOT NULL
        OR HOR.underwritten_fico_denom IS NOT NULL
        OR HOR.leads_fico_below_600 IS NOT NULL
        OR HOR.leads_fico_600_649 IS NOT NULL
        OR HOR.leads_fico_650_699 IS NOT NULL
        OR HOR.leads_fico_700_749 IS NOT NULL
        OR HOR.leads_fico_above_750 IS NOT NULL
        OR HOR.pre_approved_fico_below_600 IS NOT NULL
        OR HOR.pre_approved_fico_600_649 IS NOT NULL
        OR HOR.pre_approved_fico_650_699 IS NOT NULL
        OR HOR.pre_approved_fico_700_749 IS NOT NULL
        OR HOR.pre_approved_fico_above_750 IS NOT NULL
        OR HOR.not_pre_approved_fico_below_600 IS NOT NULL
        OR HOR.not_pre_approved_fico_600_649 IS NOT NULL
        OR HOR.not_pre_approved_fico_650_699 IS NOT NULL
        OR HOR.not_pre_approved_fico_700_749 IS NOT NULL
        OR HOR.not_pre_approved_fico_above_750 IS NOT NULL
        OR HOR.underwritten_fico_below_600 IS NOT NULL
        OR HOR.underwritten_fico_600_649 IS NOT NULL
        OR HOR.underwritten_fico_650_699 IS NOT NULL
        OR HOR.underwritten_fico_700_749 IS NOT NULL
        OR HOR.underwritten_fico_above_750 IS NOT NULL
        OR HOR.approved_fico_below_600 IS NOT NULL
        OR HOR.approved_fico_600_649 IS NOT NULL
        OR HOR.approved_fico_650_699 IS NOT NULL
        OR HOR.approved_fico_700_749 IS NOT NULL
        OR HOR.approved_fico_above_750 IS NOT NULL
        OR HOR.leads_rev_below_30k IS NOT NULL
        OR HOR.leads_rev_30k_100k IS NOT NULL
        OR HOR.leads_rev_100k_250k IS NOT NULL
        OR HOR.leads_rev_above_250k IS NOT NULL
        OR HOR.pre_approved_rev_below_30k IS NOT NULL
        OR HOR.pre_approved_rev_30k_100k IS NOT NULL
        OR HOR.pre_approved_rev_100k_250k IS NOT NULL
        OR HOR.pre_approved_rev_above_250k IS NOT NULL
        OR HOR.not_pre_approved_rev_below_30k IS NOT NULL
        OR HOR.not_pre_approved_rev_30k_100k IS NOT NULL
        OR HOR.not_pre_approved_rev_100k_250k IS NOT NULL
        OR HOR.not_pre_approved_rev_above_250k IS NOT NULL
        OR HOR.underwritten_rev_below_30k IS NOT NULL
        OR HOR.underwritten_rev_30k_100k IS NOT NULL
        OR HOR.underwritten_rev_100k_250k IS NOT NULL
        OR HOR.underwritten_rev_above_250k IS NOT NULL
        OR HOR.approved_rev_below_30k IS NOT NULL
        OR HOR.approved_rev_30k_100k IS NOT NULL
        OR HOR.approved_rev_100k_250k IS NOT NULL
        OR HOR.approved_rev_above_250k IS NOT NULL
        OR HOR.first_approved_credit_limit IS NOT NULL
        OR HOR.first_draw_amount IS NOT NULL
        OR HOR.pre_approved_declined_fico_denom IS NOT NULL
        OR HOR.lead_revenue IS NOT NULL
        OR HOR.lead_revenue_denom IS NOT NULL
        OR HOR.pre_approved_revenue IS NOT NULL
        OR HOR.pre_approved_revenue_denom IS NOT NULL
        OR HOR.not_pre_approved_revenue IS NOT NULL
        OR HOR.not_pre_approved_revenue_denom IS NOT NULL
        OR HOR.underwritten_revenue IS NOT NULL
        OR HOR.underwritten_revenue_denom IS NOT NULL
        OR HOR.pre_approved_approved_revenue IS NOT NULL
        OR HOR.pre_approved_approved_revenue_denom IS NOT NULL
        OR HOR.approved_fico IS NOT NULL
        OR HOR.approved_fico_denom IS NOT NULL
        OR HOR.approved_revenue IS NOT NULL
        OR HOR.approved_revenue_denom IS NOT NULL
        OR HOR.total_first_approved_credit_limit IS NOT NULL
        OR L.leads_vert IS NOT NULL
        OR L.leads_vert_pa IS NOT NULL
        OR L.leads_fico_vert IS NOT NULL
        OR L.leads_fico_denom_vert IS NOT NULL
        OR L.leads_revenue_vert IS NOT NULL
        OR L.leads_revenue_denom_vert IS NOT NULL
        OR L.leads_fico_below_600_vert IS NOT NULL
        OR L.leads_fico_600_649_vert IS NOT NULL
        OR L.leads_fico_650_699_vert IS NOT NULL
        OR L.leads_fico_700_749_vert IS NOT NULL
        OR L.leads_fico_above_750_vert IS NOT NULL
        OR L.leads_rev_below_30k_vert IS NOT NULL
        OR L.leads_rev_30k_100k_vert IS NOT NULL
        OR L.leads_rev_100k_250k_vert IS NOT NULL
        OR L.leads_rev_above_250k_vert IS NOT NULL
        OR PQ.pre_approvals_vert IS NOT NULL
        OR PQ.PRE_APPROVED_CREDIT_LIMIT_VERT IS NOT NULL
        OR PQ.not_pre_approved_fico_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_denom_vert IS NOT NULL
        OR PQ.not_pre_approved_revenue_vert IS NOT NULL
        OR PQ.not_pre_approved_revenue_denom_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_below_600_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_600_649_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_650_699_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_700_749_vert IS NOT NULL
        OR PQ.not_pre_approved_fico_above_750_vert IS NOT NULL
        OR PQ.pre_approved_fico_vert IS NOT NULL
        OR PQ.pre_approved_fico_denom_vert IS NOT NULL
        OR PQ.pre_approved_revenue_vert IS NOT NULL
        OR PQ.pre_approved_revenue_denom_vert IS NOT NULL
        OR PQ.pre_approved_fico_below_600_vert IS NOT NULL
        OR PQ.pre_approved_fico_600_649_vert IS NOT NULL
        OR PQ.pre_approved_fico_650_699_vert IS NOT NULL
        OR PQ.pre_approved_fico_700_749_vert IS NOT NULL
        OR PQ.pre_approved_fico_above_750_vert IS NOT NULL
        OR PQ.pre_approved_rev_below_30k_VERT IS NOT NULL
        OR PQ.pre_approved_rev_30k_100k_VERT IS NOT NULL
        OR PQ.pre_approved_rev_100k_250k_VERT IS NOT NULL
        OR PQ.pre_approved_rev_above_250k_VERT IS NOT NULL
        OR PQ.not_pre_approved_rev_below_30k_VERT IS NOT NULL
        OR PQ.not_pre_approved_rev_30k_100k_VERT IS NOT NULL
        OR PQ.not_pre_approved_rev_100k_250k_VERT IS NOT NULL
        OR PQ.not_pre_approved_rev_above_250k_VERT IS NOT NULL
        OR R.registrations_vert IS NOT NULL
        OR R.registrations_vert_pa IS NOT NULL
        OR R.reg_7_vert IS NOT NULL
        OR R.reg_7_pa_vert IS NOT NULL
        OR R.reg_14_vert IS NOT NULL
        OR R.reg_14_pa_vert IS NOT NULL
        OR R.reg_28_vert IS NOT NULL
        OR R.reg_28_pa_vert IS NOT NULL
        OR CIP.cip_connected_vert IS NOT NULL
        OR CIP.cip_connected_vert_pa IS NOT NULL
        OR UW.underwritten_vert IS NOT NULL
        OR UW.underwritten_vert_excl_fraud IS NOT NULL
        OR UW.und_fraud_vert IS NOT NULL
        OR UW.underwritten_vert_pa IS NOT NULL
        OR UW.underwritten_vert_excl_fraud_pa IS NOT NULL
        OR UW.und_fraud_vert_pa IS NOT NULL
        OR UW.underwritten_fico_vert IS NOT NULL
        OR UW.underwritten_revenue_vert IS NOT NULL
        OR UW.underwritten_revenue_denom_vert IS NOT NULL
        OR UW.underwritten_fico_denom_vert IS NOT NULL
        OR UW.underwritten_fico_below_600_VERT IS NOT NULL
        OR UW.underwritten_fico_600_649_VERT IS NOT NULL
        OR UW.underwritten_fico_650_699_VERT IS NOT NULL
        OR UW.underwritten_fico_700_749_VERT IS NOT NULL
        OR UW.underwritten_fico_above_750_VERT IS NOT NULL
        OR UW.underwritten_rev_below_30k_VERT IS NOT NULL
        OR UW.underwritten_rev_30k_100k_VERT IS NOT NULL
        OR UW.underwritten_rev_100k_250k_VERT IS NOT NULL
        OR UW.underwritten_rev_above_250k_VERT IS NOT NULL
        OR APP.approvals_vert IS NOT NULL
        OR APP.approvals_vert_pa IS NOT NULL
        OR APP.credit_match_vert IS NOT NULL
        OR APP.approved_credit_limit_vert IS NOT NULL
        OR APP.delta_credit_limit_vert IS NOT NULL
        OR APP.delta_missed_cl_offers_vert IS NOT NULL
        OR APP.not_overall_match_vert IS NOT NULL
        OR APP.approved_fico_vert IS NOT NULL
        OR APP.approved_fico_denom_vert IS NOT NULL
        OR APP.approved_revenue_vert IS NOT NULL
        OR APP.approved_revenue_denom_vert IS NOT NULL
        OR APP.approved_fico_below_600_VERT IS NOT NULL
        OR APP.approved_fico_600_649_VERT IS NOT NULL
        OR APP.approved_fico_650_699_VERT IS NOT NULL
        OR APP.approved_fico_700_749_VERT IS NOT NULL
        OR APP.approved_fico_above_750_VERT IS NOT NULL
        OR APP.approved_rev_below_30k_VERT IS NOT NULL
        OR APP.approved_rev_30k_100k_VERT IS NOT NULL
        OR APP.approved_rev_100k_250k_VERT IS NOT NULL
        OR APP.approved_rev_above_250k_VERT IS NOT NULL
        OR APP.total_first_approved_credit_limit_vert IS NOT NULL
        OR FTD.ftd_ever_vert IS NOT NULL
        OR FTD.ftd_ever_vert_pa IS NOT NULL
        OR FTD.ftd_7_vert IS NOT NULL
        OR FTD.ftd_7_vert_pa IS NOT NULL
        OR FTD.ftd_28_vert IS NOT NULL
        OR FTD.ftd_28_vert_pa IS NOT NULL
        OR FTD.first_draw_amount_vert IS NOT NULL)
    )
    ;



    select distinct partner_name, channel_type_new
from INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1_new_td;



select distinct partner_name, channel_type_new
from INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_leads_td;





select distinct channel, partner, channel_type_new, evercommerce_flow, partner_name_upd
from INDUS.PUBLIC.leads_base_table_td;




select *
from INDUS.PUBLIC.leads_agg_td
where week_end_date = '2025-08-20'
and partner_name = 'EverCommerce';