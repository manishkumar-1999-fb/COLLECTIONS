



--CREATE OR REPLACE VIEW ANALYTICS.CREDIT.eg_key_metrics_funnel_structure AS
CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1 AS



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

, channel_table AS (
SELECT 'Direct' channel UNION SELECT 'Partner' channel UNION SELECT 'Other' channel
)

, tier_table AS (
SELECT 'A' tier UNION SELECT 'B' tier UNION SELECT 'C' tier UNION SELECT 'D' tier UNION SELECT 'F' tier
)

, sub_product_table AS (
SELECT 'Line Of Credit' sub_product UNION SELECT 'Term Loan' sub_product UNION SELECT 'Pay' sub_product UNION SELECT 'Credit Builder' sub_product UNION SELECT 'No Selection' sub_product UNION SELECT 'mca'
)

, risk_bucket_table AS (
SELECT 'No Bucket' risk_bucket UNION SELECT 'OB: 1-3' risk_bucket UNION SELECT 'OB: 4-5' risk_bucket UNION SELECT 'OB: 6-8' risk_bucket UNION SELECT 'OB: 9-10' risk_bucket UNION SELECT 'OB: 11-12' risk_bucket UNION SELECT 'OB: 13+' risk_bucket 
)

, reg_client_table AS (
SELECT 'Mobile' reg_client UNION select 'Desktop' reg_client UNION select 'Unknown' reg_client
)
/*
, partner_table AS (
SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner
)
*/

-- 7 Feb - Business loans added
-- , partner_table AS (
-- SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Sofi' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'BusinessLoans' partner UNION SELECT 'AtoB' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner
-- )

-- 23 Aug 2024 - Moneyjet, Small Business Loans, QuickBridge, and National Funding added
-- , partner_table AS (
-- SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Sofi' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'BusinessLoans' partner UNION SELECT 'AtoB' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner UNION SELECT 'Moneyjet' partner UNION SELECT 'Small Business Loans' partner UNION SELECT 'National Funding' partner UNION SELECT 'QuickBridge' partner
-- )

--28 Aug 2024 - National Funding Super added
, partner_table AS (
SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Sofi' partner UNION SELECT 'Cardiff' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'BusinessLoans' partner UNION SELECT 'AtoB' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner UNION SELECT 'National Funding Super' partner UNION SELECT 'Bluevine' partner UNION SELECT 'ZenBusiness' partner UNION SELECT 'Anansii' partner UNION SELECT 'Guesty' partner UNION SELECT 'Cantaloupe' partner UNION SELECT 'Joist' partner UNION SELECT 'Autobooks' partner UNION SELECT '1West' partner UNION SELECT 'Franpos'
)

, termunits_table AS (
SELECT 'Direct'  termunits UNION SELECT 'Intuit' termunits UNION SELECT 'Large Partners' termunits UNION SELECT 'Marketplaces' termunits UNION SELECT 'Platform Partners' termunits UNION SELECT 'Terminated Partners' termunits
)

/*
, intuit_flow AS (
SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
)
*/

-- 7 Feb - Business loans added
-- , intuit_flow AS (
-- SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Sofi' partner, 'not intuit' intuit_flow UNION SELECT 'Lendio' partner, 'not intuit' intuit_flow UNION SELECT 'Fundera' partner, 'not intuit' intuit_flow UNION SELECT 'BusinessLoans' partner, 'not intuit' intuit_flow UNION SELECT 'AtoB' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
-- )

-- 23 Aug 2024 - Moneyjet, Small Business Loans, QuickBridge, and National Funding added
-- , intuit_flow AS (
-- SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Sofi' partner, 'not intuit' intuit_flow UNION SELECT 'Lendio' partner, 'not intuit' intuit_flow UNION SELECT 'Fundera' partner, 'not intuit' intuit_flow UNION SELECT 'BusinessLoans' partner, 'not intuit' intuit_flow UNION SELECT 'AtoB' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'Small Business Loans' partner, 'not intuit' intuit_flow UNION SELECT 'Moneyjet' partner, 'not intuit' intuit_flow UNION SELECT 'National Funding' partner, 'not intuit' intuit_flow UNION SELECT 'QuickBridge' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
-- )
-- 28 Aug 2024 - National Funding Super added
, intuit_flow AS (
SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Cantaloupe' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Sofi' partner,'not intuit' intuit_flow UNION SELECT 'Cardiff' partner, 'not intuit' intuit_flow UNION SELECT 'Lendio' partner, 'not intuit' intuit_flow UNION SELECT 'Fundera' partner, 'not intuit' intuit_flow UNION SELECT 'BusinessLoans' partner, 'not intuit' intuit_flow UNION SELECT 'AtoB' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'National Funding Super' partner, 'not intuit' intuit_flow UNION SELECT 'Bluevine' partner,'not intuit' intuit_flow UNION SELECT 'ZenBusiness' partner,'not intuit' intuit_flow UNION SELECT 'Joist' partner, 'not intuit' intuit_flow UNION SELECT 'Franpos' partner ,'not intuit' intuit_flow UNION SELECT 'Autobooks' partner , 'not intuit' intuit_flow UNION SELECT '1West' partner,  'not intuit' intuit_flow UNION SELECT 'Anansii' partner, 'not intuit' intuit_flow UNION SELECT 'Guesty' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
)

/*
, nav_flow AS (
SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'Clover' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
)
*/

-- 7 Feb - Business loans added

-- , nav_flow AS (
-- SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Sofi' partner, 'not nav' nav_flow UNION SELECT 'Lendio' partner, 'not nav' nav_flow UNION SELECT 'Fundera' partner, 'not nav' nav_flow UNION SELECT 'BusinessLoans' partner, 'not nav' nav_flow UNION SELECT 'AtoB' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION SELECT 'Nav' partner, 'Nav Pre-approval' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'CTA' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
-- )

-- 23 Aug 2024 - Moneyjet, Small Business Loans, QuickBridge, and National Funding added

-- , nav_flow AS (
-- SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Sofi' partner, 'not nav' nav_flow UNION SELECT 'Lendio' partner, 'not nav' nav_flow UNION SELECT 'Fundera' partner, 'not nav' nav_flow UNION SELECT 'BusinessLoans' partner, 'not nav' nav_flow UNION SELECT 'AtoB' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'Moneyjet' partner, 'not nav' nav_flow UNION SELECT 'Small Business Loans' partner, 'not nav' nav_flow UNION SELECT 'National Funding' partner, 'not nav' nav_flow UNION SELECT 'QuickBridge' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION SELECT 'Nav' partner, 'Nav Pre-approval' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'CTA' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
-- )

-- 28 Aug 2024 - National Funding Super added
, nav_flow AS (
SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Sofi' partner,'not nav' nav_flow UNION SELECT 'Cardiff' partner, 'not nav' nav_flow UNION SELECT 'Lendio' partner,'not nav' nav_flow UNION SELECT 'Cantaloupe' partner, 'not nav' nav_flow UNION SELECT 'Fundera' partner, 'not nav' nav_flow UNION SELECT 'BusinessLoans' partner, 'not nav' nav_flow UNION SELECT 'AtoB' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'National Funding Super' partner, 'not nav' nav_flow UNION SELECT 'Bluevine' partner,'not nav' nav_flow UNION SELECT 'ZenBusiness' partner,'not nav' nav_flow UNION SELECT 'Joist' partner, 'not nav' nav_flow UNION SELECT 'Franpos' partner,'not nav' nav_flow UNION SELECT 'Autobooks' partner, 'not nav' nav_flow UNION SELECT '1West' partner, 'not nav' nav_flow UNION SELECT 'Anansii' partner, 'not nav' nav_flow UNION SELECT 'Guesty' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION SELECT 'Nav' partner, 'Nav Pre-approval' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'CTA' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
)

-- 7 Feb - Business loans added
-- , lendio_flow AS (
-- SELECT 'Freshbooks' partner, 'not lendio' lendio_flow UNION SELECT 'Intuit' partner, 'not lendio' lendio_flow UNION SELECT 'Housecall Pro' partner, 'not lendio' lendio_flow UNION SELECT 'IFS' partner, 'not lendio' lendio_flow UNION SELECT 'Forbes Advisors' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Brokers' partner, 'not lendio' lendio_flow UNION SELECT 'Direct' partner, 'not lendio' lendio_flow UNION SELECT 'Other Partners' partner, 'not lendio' lendio_flow  UNION SELECT 'Fundera' partner, 'not lendio' lendio_flow UNION SELECT 'BusinessLoans' partner, 'not lendio' lendio_flow UNION SELECT 'AtoB' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Partners' partner, 'not lendio' lendio_flow UNION SELECT 'Other' partner, 'not lendio' lendio_flow UNION SELECT 'Nav' partner, 'not lendio' lendio_flow UNION SELECT 'Sofi' partner, 'not lendio' lendio_flow UNION SELECT 'Lendio' partner, 'Non-Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Direct Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Lendio Embedded' lendio_flow
-- )

-- 23 Aug 2024 - Moneyjet, Small Business Loans, QuickBridge, and National Funding added
-- , lendio_flow AS (
-- SELECT 'Freshbooks' partner, 'not lendio' lendio_flow UNION SELECT 'Intuit' partner, 'not lendio' lendio_flow UNION SELECT 'Housecall Pro' partner, 'not lendio' lendio_flow UNION SELECT 'IFS' partner, 'not lendio' lendio_flow UNION SELECT 'Forbes Advisors' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Brokers' partner, 'not lendio' lendio_flow UNION SELECT 'Direct' partner, 'not lendio' lendio_flow UNION SELECT 'Other Partners' partner, 'not lendio' lendio_flow  UNION SELECT 'Fundera' partner, 'not lendio' lendio_flow UNION SELECT 'BusinessLoans' partner, 'not lendio' lendio_flow UNION SELECT 'AtoB' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Partners' partner, 'not lendio' lendio_flow UNION SELECT 'Other' partner, 'not lendio' lendio_flow UNION SELECT 'Nav' partner, 'not lendio' lendio_flow UNION SELECT 'Sofi' partner, 'not lendio' lendio_flow UNION SELECT 'Moneyjet' partner, 'not lendio' lendio_flow UNION SELECT 'Small Business Loans' partner, 'not lendio' lendio_flow UNION SELECT 'National Funding' partner, 'not lendio' lendio_flow UNION SELECT 'QuickBridge' partner, 'not lendio' lendio_flow UNION SELECT 'Lendio' partner, 'Non-Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Direct Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Lendio Embedded' lendio_flow
-- )

-- 28 Aug 2024 - National Funding Super added
, lendio_flow AS (
SELECT 'Freshbooks' partner, 'not lendio' lendio_flow UNION SELECT 'Intuit' partner, 'not lendio' lendio_flow UNION SELECT 'Housecall Pro' partner, 'not lendio' lendio_flow UNION SELECT 'IFS' partner, 'not lendio' lendio_flow UNION SELECT 'Forbes Advisors' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Brokers' partner, 'not lendio' lendio_flow UNION SELECT 'Direct' partner, 'not lendio' lendio_flow UNION SELECT 'Other Partners' partner, 'not lendio' lendio_flow  UNION SELECT 'Fundera' partner,'not lendio' lendio_flow UNION SELECT 'Cardiff' partner, 'not lendio' lendio_flow UNION SELECT 'BusinessLoans' partner, 'not lendio' lendio_flow UNION SELECT 'AtoB' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Partners' partner, 'not lendio' lendio_flow UNION SELECT 'Other' partner, 'not lendio' lendio_flow UNION SELECT 'Nav' partner, 'not lendio' lendio_flow UNION SELECT 'Sofi' partner, 'not lendio' lendio_flow UNION SELECT 'National Funding Super' partner, 'not lendio' lendio_flow UNION SELECT 'Bluevine' partner,'not lendio' lendio_flow UNION SELECT 'Cantaloupe' partner,'not lendio' lendio_flow UNION SELECT 'ZenBusiness' partner,'not lendio' lendio_flow UNION SELECT 'Joist' partner,'not lendio' lendio_flow UNION SELECT 'Franpos' partner,'not lendio' lendio_flow UNION SELECT 'Autobooks' partner, 'not lendio' lendio_flow UNION SELECT '1West' partner, 'not lendio' lendio_flow UNION SELECT 'Anansii' partner, 'not lendio' lendio_flow UNION SELECT 'Guesty' partner, 'not lendio' lendio_flow UNION SELECT 'Lendio' partner, 'Non-Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Direct Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Lendio Embedded' lendio_flow
)

-- 28 Aug - National Funding Super added
, national_funding_flow AS (
SELECT 'Freshbooks' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Intuit' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Bluevine' partner,'Not National Funding' national_funding_flow UNION SELECT 'Cardiff' partner,'Not National Funding' national_funding_flow UNION SELECT 'Cantaloupe' partner,'Not National Funding' national_funding_flow UNION SELECT 'ZenBusiness' partner,'Not National Funding' national_funding_flow UNION SELECT 'Joist' partner,'Not National Funding' national_funding_flow UNION SELECT 'Franpos' partner,'Not National Funding' national_funding_flow UNION SELECT 'Autobooks' partner,'Not National Funding' national_funding_flow UNION SELECT '1West' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Anansii' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Guesty' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Housecall Pro' partner, 'Not National Funding' national_funding_flow UNION SELECT 'IFS' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Forbes Advisors' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Terminated Brokers' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Direct' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Other Partners' partner, 'Not National Funding' national_funding_flow  UNION SELECT 'Fundera' partner, 'Not National Funding' national_funding_flow UNION SELECT 'BusinessLoans' partner, 'Not National Funding' national_funding_flow UNION SELECT 'AtoB' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Terminated Partners' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Other' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Nav' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Sofi' partner, 'Not National Funding' national_funding_flow UNION SELECT 'National Funding Super' partner, 'Moneyjet' national_funding_flow UNION SELECT 'National Funding Super' partner, 'Small Business Loans' national_funding_flow UNION SELECT 'National Funding Super' partner, 'National Funding' national_funding_flow UNION SELECT 'National Funding Super' partner, 'QuickBridge' national_funding_flow UNION SELECT 'Lendio' partner, 'Not National Funding' national_funding_flow
)

SELECT a.week_end_date
, ct.channel
, tu.termunits
, tt.tier
, spt.sub_product
, rbt.risk_bucket bucket_group
, rct.reg_client
, pt.partner
, if_.intuit_flow
, nf.nav_flow
, lf.lendio_flow
, nff.national_funding_flow



FROM a

CROSS JOIN channel_table ct
CROSS JOIN termunits_table tu
CROSS JOIN tier_table tt
CROSS JOIN sub_product_table spt
CROSS JOIN risk_bucket_table rbt
CROSS JOIN reg_client_table rct
CROSS JOIN partner_table pt
LEFT JOIN intuit_flow if_ 
ON pt.partner = if_.partner
LEFT JOIN nav_flow nf
ON pt.partner = nf.partner

--17 Jan addition
LEFT JOIN lendio_flow lf
ON pt.partner = lf.partner

LEFT JOIN national_funding_flow nff
ON pt.partner = nff.partner

UNION

SELECT b.week_end_date
, ct.channel
, tu.termunits
, tt.tier
, spt.sub_product
, rbt.risk_bucket bucket_group
, rct.reg_client
, pt.partner
, if_.intuit_flow
, nf.nav_flow
, lf.lendio_flow
, nff.national_funding_flow

FROM b

CROSS JOIN channel_table ct
CROSS JOIN termunits_table tu
CROSS JOIN tier_table tt
CROSS JOIN sub_product_table spt
CROSS JOIN risk_bucket_table rbt
CROSS JOIN reg_client_table rct
CROSS JOIN partner_table pt
LEFT JOIN intuit_flow if_ 
ON pt.partner = if_.partner
LEFT JOIN nav_flow nf
ON pt.partner = nf.partner

--17 Jan addition
LEFT JOIN lendio_flow lf
ON pt.partner = lf.partner

LEFT JOIN national_funding_flow nff
ON pt.partner = nff.partner

ORDER BY 1 DESC,2,3,4,5,6,7
;

CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_key_metrics_funnel_structure AS (
SELECT *, 1 AS ONE
FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure_1);




--------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------FUNNEL_INDUS----------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------
-------------------------last updated on 31st may -----------------

--USE WAREHOUSE DS_WH;
CREATE OR REPLACE TABLE INDUS.PUBLIC.INDUS_KEY_METRICS_FUNNEL_AGG AS 
(WITH funnel AS (

SELECT DISTINCT cd.fbbid

, f.channel
, f.TERMUNITS
, f.partner
, f.intuit_flow
, f.national_funding_flow
, f.nav_flow
, f.lendio_flow
, f.tier
, f.sub_product
, f.reg_client
--sum(CASE WHEN risk_bucket IS NULL THEN 0 ELSE originated_amount END) orig_bucket_not_null
, CASE 
	WHEN cd.is_approved = 1 THEN f.ob_dal_bucket
	ELSE f.ob_risk_bucket_first
END risk_bucket
/*, CASE 
	WHEN cd.is_approved = 1 THEN f.ob_risk_bucket_approved
	ELSE f.ob_risk_bucket_first
END risk_bucket
*/
/*, CASE 
	WHEN cd.is_approved = 1 THEN f.ob_bucket_group_approved
	ELSE f.ob_bucket_group_first
END bucket_group
*/
-- 7 Change for OB DAL bucket_group

, ob_bucket_group_dal as bucket_group


, cd.registration_time
, cd.registration_time::date reg_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', reg_date+4)::date+2
	WHEN datediff('day', reg_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', reg_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', reg_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', reg_date+4)::date+2
END reg_start_week_end_date

, cd.is_cip_connected
, cd.cip_connected_time::date cip_connected_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', cip_connected_date+4)::date+2
	WHEN datediff('day', cip_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', cip_connected_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', cip_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', cip_connected_date+4)::date+2
END cip_connected_week_end_date

, cd.is_connected
, cd.first_connected_time::date first_connected_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_connected_date+4)::date+2
	WHEN datediff('day', first_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_connected_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', first_connected_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', first_connected_date+4)::date+2
END first_connected_week_end_date

, cd.is_registration_flow_completed
, cd.registration_flow_completed_time::date registration_flow_completed_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', registration_flow_completed_date+4)::date+2
	WHEN datediff('day', registration_flow_completed_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', registration_flow_completed_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', registration_flow_completed_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', registration_flow_completed_date+4)::date+2
END reg_complete_week_end_date

, CASE WHEN cd.FIRST_RISK_REVIEW_TIME IS NULL THEN 0 ELSE 1 END AS is_risk_review
, cd.first_risk_review_time::date risk_review_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', risk_review_date+4)::date+2
	WHEN datediff('day', risk_review_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', risk_review_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', risk_review_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', risk_review_date+4)::date+2
END risk_review_week_end_date

, cd.is_underwriting is_underwriting_old
---------30th May Changes-----------
, CASE 
	WHEN cd.is_underwriting = 1 THEN 1 
	-- WHEN cd.current_credit_status_reason in ('Onboarding dynamic decision reject') then 1
		WHEN cd.first_rejected_reason in ('Onboarding dynamic decision reject') then 1
	ELSE 0
END AS is_underwriting_new
-------------------------------------
, cd.first_decision_time::date first_dec_date
, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_dec_date+4)::date+2
	WHEN datediff('day', first_dec_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_dec_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', first_dec_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', first_dec_date+4)::date+2
END first_dec_week_end_date

, MONTH(cd.registration_time::date) - pq.IN_BUSINESS_SINCE_MONTH + (YEAR(cd.registration_time::date) - pq.IN_BUSINESS_SINCE_YEAR)*12 pq_months_in_business
, CASE WHEN pq.IN_BUSINESS_SINCE_MONTH IS NOT NULL THEN 1 ELSE 0 END has_pq_months_in_business
, COALESCE(first_account_size_accounting_software,first_account_size_fi,0) * 12 calc_revenue
, CASE WHEN first_account_size_accounting_software IS NOT NULL THEN 1 WHEN first_account_size_fi IS NOT NULL THEN 1 ELSE 0 END has_calc_revenue

, cd.first_approved_time
, cd.first_approved_time::date first_approved_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_approved_date+4)::date+2
	WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_approved_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', first_approved_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', first_approved_date+4)::date+2
END app_week_end_date

, cd.is_approved

/*
, CASE 
	WHEN cd.is_underwriting = 1 AND cd.is_approved = 0 THEN 1
	WHEN cd.is_underwriting = 1 AND cd.is_approved = 1 THEN 0
	WHEN cd.is_underwriting = 1 AND cd.is_approved IS NULL THEN 1
	WHEN cd.is_underwriting = 0 THEN NULL 
END AS is_rejected
*/

--- 29 Feb add for Rejection Reason

, FEB.REJECTION_REASON 

, CASE 
	WHEN FEB.REJECTION_REASON IS NOT NULL THEN FEB.REJECTION_REASON
	WHEN FEB.REJECTION_REASON IS NULL and cd.CURRENT_CREDIT_STATUS ='rejected' THEN 'Other Rejections'
	WHEN FEB.REJECTION_REASON IS NULL and cd.CURRENT_CREDIT_STATUS !='rejected' THEN 'Not Rejected'
	else 'Check cases'
	end as REJECTION_REASON_2

, CASE 
	WHEN REJECTION_REASON_2 = 'Not Rejected' THEN 0
	--WHEN FEB.REJECTION_REASON is Null and cd.CURRENT_CREDIT_STATUS !='rejected' THEN 0
	--WHEN FEB.REJECTION_REASON is Null and cd.CURRENT_CREDIT_STATUS ='rejected' THEN 1
	ELSE 1
	END AS is_rejected



/*
, CASE 
	WHEN is_rejected = 0 THEN NULL 
	WHEN is_rejected IS NULL THEN NULL 
	WHEN cd.state IN ('SD', 'NM') THEN 'State'
	WHEN cd.state = 'CO' and cd.first_aligned_bucket > 6 then 'State'
	WHEN cd.LAST_IS_APPROVED_BY_DATA_RULES = 0 THEN 'Data Rules'
	WHEN cd.fico_onboarding < 600 THEN 'FICO < 600'
	WHEN cd.LAST_IS_APPROVED_BY_CREDIT_RULES = 0 THEN 'Credit Rules'
	ELSE 'Policy'
END AS rejected_reason

*/

, cd.first_rejected_reason

, cd.is_ftu
, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) <= 7 THEN 1 ELSE 0 END AS is_ftd_0_7
, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) BETWEEN 8 AND 28 THEN 1 ELSE 0 END AS is_ftd_8_28
, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) BETWEEN 29 AND 60 THEN 1 ELSE 0 END AS is_ftd_29_60
, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time) >= 61 THEN 1 ELSE 0 END AS is_ftd_61_

, cd.first_draw_time
, cd.first_draw_time::date first_draw_date

, CASE 
	WHEN dayofweek(current_date()) = 3 THEN DATE_TRUNC('WEEK', first_draw_date+4)::date+2
	WHEN datediff('day', first_draw_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 AND datediff('day', first_draw_date, current_date()) <= 0 THEN NULL 
	WHEN datediff('day', first_draw_date, DATE_TRUNC('WEEK',current_date()+4)::date-5) < 0 THEN current_date()-1
	ELSE DATE_TRUNC('WEEK', first_draw_date+4)::date+2
END ftd_week_end_date

, cd.fico_onboarding fico
, cd.first_approved_credit_limit
, cd.first_draw_amount fda
, dacd.credit_limit as credit_limit_at_ftd

, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<8 THEN 1 ELSE 0 END AS is_ftd7
, CASE WHEN datediff('day',cd.first_approved_time,cd.first_draw_time)<29 THEN 1 ELSE 0 END AS is_ftd28


-------------------------------------------------------------------------------------------------------- Additional part of code for horizontal metrics (DUNCAN)
-----------30th May Changes----------
,  case when cd.is_underwriting = 1 then 1
		--  when cd.current_credit_status_reason in ('Onboarding dynamic decision reject') then 1
		WHEN cd.first_rejected_reason in ('Onboarding dynamic decision reject') then 1
	else 0 end is_uw_new_definition

, nvl(cd.first_approved_time, cd.first_rejected_time) underwritten_time


-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------


--------------------------------------------------HORIZONTAL METRICS ADDITIONAL CODE (PRIYANSH)-----------------------------------------------------------


,cd.CIP_CONNECTED_TIME
,cd.FIRST_CONNECTED_TIME	
,cd.REGISTRATION_FLOW_COMPLETED_TIME
,cd.FIRST_DECISION_TIME		
,cd.FIRST_REJECTED_TIME
,cd.current_credit_status
,cd.current_credit_status_reason


, cd.current_credit_status_start_time
, (DATEADD('day', 3, DATE_TRUNC('WEEK', DATEADD('day', -3, CD.registration_time::DATE)))::DATE) AS WEEK_START
, DATEADD('day', 6, WEEK_START)  AS WEEK_END,

--------INELIGIBLE ACCOUNT FLAG-----------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, current_timestamp())/24 <1  then 1
else 0
end as "INELIGIBLE_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, current_timestamp())/24 <7  then 1
else 0
end as "INELIGIBLE_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, current_timestamp())/24 <30  then 1
else 0
end as "INELIGIBLE_30DAY",
-------------------


--------ERROR FLAG-----------

CASE
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NOT NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NOT NULL AND cd.FIRST_DECISION_TIME IS NOT NULL THEN 
		CASE
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME  AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) <= cd.REGISTRATION_FLOW_COMPLETED_TIME  AND cd.REGISTRATION_FLOW_COMPLETED_TIME < cd.FIRST_DECISION_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME  AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) <= cd.REGISTRATION_FLOW_COMPLETED_TIME  AND cd.REGISTRATION_FLOW_COMPLETED_TIME = cd.FIRST_DECISION_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) <= cd.FIRST_DECISION_TIME  AND cd.FIRST_DECISION_TIME <= cd.REGISTRATION_FLOW_COMPLETED_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.FIRST_DECISION_TIME  AND cd.FIRST_DECISION_TIME <= dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME )<= cd.REGISTRATION_FLOW_COMPLETED_TIME  THEN 0
			ELSE 1
		END
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NOT NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NOT NULL AND cd.FIRST_DECISION_TIME IS NULL THEN 
		CASE
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) < cd.REGISTRATION_FLOW_COMPLETED_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) = cd.REGISTRATION_FLOW_COMPLETED_TIME THEN 0
			ELSE 1
		END
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NOT NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NULL AND cd.FIRST_DECISION_TIME IS NOT NULL THEN 
		CASE
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) < cd.FIRST_DECISION_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME AND dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) = cd.FIRST_DECISION_TIME THEN 0
			WHEN cd.REGISTRATION_TIME <= cd.FIRST_DECISION_TIME AND cd.FIRST_DECISION_TIME <= dateadd(HOUR , -1 ,cd.CIP_CONNECTED_TIME ) THEN 0
			ELSE 1
		END
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NOT NULL AND cd.FIRST_DECISION_TIME IS NOT NULL THEN 1
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NOT NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NULL AND cd.FIRST_DECISION_TIME IS NULL THEN 
		CASE 
			WHEN cd.REGISTRATION_TIME <= cd.CIP_CONNECTED_TIME THEN 0
			ELSE 1
		END
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NOT NULL AND cd.FIRST_DECISION_TIME IS NULL THEN 1
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS NULL AND cd.FIRST_DECISION_TIME IS NOT NULL THEN 
		CASE 
			WHEN cd.REGISTRATION_TIME <= FINAL_DECISION_TIME  THEN 0
			ELSE 1
		END
	WHEN cd.REGISTRATION_TIME IS NOT NULL AND cd.CIP_CONNECTED_TIME IS NULL AND cd.REGISTRATION_FLOW_COMPLETED_TIME IS  NULL AND cd.FIRST_DECISION_TIME IS NULL THEN 0
	ELSE 1 
END AS ERROR_FLAG,	


--------ERROR FLAG 2-----------

case 
when INELIGIBLE_1DAY = 0 THEN ERROR_FLAG
else 0
end as "ERROR_FLAG2_1DAY",
case 
when INELIGIBLE_7DAY = 0 THEN ERROR_FLAG
else 0
end as "ERROR_FLAG2_7DAY",
case 
when INELIGIBLE_30DAY = 0 THEN ERROR_FLAG
else 0
end as "ERROR_FLAG2_30DAY", 



------ A1 [CIP CONNECTED TIME]-------------
case 
when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND (DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 >1.04 OR DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 is NULL) then 1
else 0
end as "CIP_NOT_CONNECTED_1DAY_A1",
case 
when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND (DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 >7.04 OR DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 is NULL) then 1
else 0
end as "CIP_NOT_CONNECTED_7DAY_A1",
case 
when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND (DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 >30.04 OR DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.CIP_CONNECTED_TIME)/24 is NULL) then 1
else 0
end as "CIP_NOT_CONNECTED_30DAY_A1",

----------FLOW COMPLETE TIME---------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 <=1 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 is NOT NULL then 1
else 0
end as "REGISTRATION_FLOW_COMPLETE_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 <=7 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 is NOT NULL then 1
else 0
end as "REGISTRATION_FLOW_COMPLETE_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 <=30 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.REGISTRATION_FLOW_COMPLETED_TIME)/24 is NOT NULL then 1
else 0
end as "REGISTRATION_FLOW_COMPLETE_30DAY",

--------DECISION TIME-----------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 <=1 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 is NOT NULL then 1
else 0
end as "DECISIONED_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 <=7 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 is NOT NULL then 1
else 0
end as "DECISIONED_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 <=30 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_DECISION_TIME)/24 is NOT NULL then 1
else 0
end as "DECISIONED_30DAY",


--------REJECTED TIME-----------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 <=1 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 is NOT NULL then 1
else 0
end as "REJECTED_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 <=7 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 is NOT NULL then 1
else 0
end as "REJECTED_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 <=30 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 is NOT NULL then 1
else 0
end as "REJECTED_30DAY",
case
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 <=31 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_REJECTED_TIME)/24 is NOT NULL then 1
else 0
end as "REJECTED_30DAY_V2",
-------------------

--------APPROVED TIME-----------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 <=1 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 is NOT NULL then 1
else 0
end as "APPROVED_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 <=7 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 is NOT NULL then 1
else 0
end as "APPROVED_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 <=30 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.FIRST_APPROVED_TIME)/24 is NOT NULL then 1
else 0
end as "APPROVED_30DAY",
-------------------

--------LAST REJECTED TIME LOGIC-----------
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 <=1 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 is NOT NULL AND CD.LAST_REJECTED_TIME >= CD.REGISTRATION_FLOW_COMPLETED_TIME then 1
else 0
end as "LAST_REJECTED_1DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 <=7 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 is NOT NULL AND CD.LAST_REJECTED_TIME >= CD.REGISTRATION_FLOW_COMPLETED_TIME then 1
else 0
end as "LAST_REJECTED_7DAY",
case 
when DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 <=30 AND DATEDIFF(HOUR, CD.REGISTRATION_TIME, CD.LAST_REJECTED_TIME)/24 is NOT NULL AND CD.LAST_REJECTED_TIME >= CD.REGISTRATION_FLOW_COMPLETED_TIME then 1
else 0
end as "LAST_REJECTED_30DAY",
-------------------




--------E-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 1 AND APPROVED_1DAY = 0 AND LAST_REJECTED_1DAY = 1 AND CD.current_credit_status_reason = 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_REJECT_PRE_UW_1DAY_E",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 1 AND APPROVED_7DAY = 0 AND LAST_REJECTED_7DAY = 1 AND CD.current_credit_status_reason = 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_REJECT_PRE_UW_7DAY_E",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 1 AND APPROVED_30DAY = 0 AND LAST_REJECTED_30DAY = 1 AND CD.current_credit_status_reason = 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_REJECT_PRE_UW_30DAY_E",
-------------------

--------G-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 1 AND APPROVED_1DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_ACCEPT_1DAY_G",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 1 AND APPROVED_7DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_ACCEPT_7DAY_G",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 1 AND APPROVED_30DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_ACCEPT_30DAY_G",
---------------------------------------------

--------F-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 1 AND APPROVED_1DAY = 0 AND LAST_REJECTED_1DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_REJECT_1DAY_F",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 1 AND APPROVED_7DAY = 0 AND LAST_REJECTED_7DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_REJECT_7DAY_F",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 1 AND APPROVED_30DAY = 0 AND LAST_REJECTED_30DAY = 1 AND CD.current_credit_status_reason != 'Onboarding dynamic decision reject' THEN 1
ELSE 0
END AS "RISK_REVIEW_REJECT_30DAY_F",
------------------------------------------------

-------D--------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 1 AND REGISTRATION_COMPLETE_REJECT_PRE_UW_1DAY_E = 0 AND RISK_REVIEW_REJECT_1DAY_F = 0 AND RISK_REVIEW_ACCEPT_1DAY_G = 0 THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_1DAY_D",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 1 AND REGISTRATION_COMPLETE_REJECT_PRE_UW_7DAY_E = 0 AND RISK_REVIEW_REJECT_7DAY_F = 0 AND RISK_REVIEW_ACCEPT_7DAY_G = 0 THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_7DAY_D",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 1 AND REGISTRATION_COMPLETE_REJECT_PRE_UW_30DAY_E = 0 AND RISK_REVIEW_REJECT_30DAY_F = 0 AND RISK_REVIEW_ACCEPT_30DAY_G = 0 THEN 1
ELSE 0
END AS "REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_30DAY_D",
-------------------

--------A2-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 0 AND REJECTED_1DAY = 0 THEN 1
ELSE 0
END AS "CIP_CONNECTED_REGISTRATION_INCOMPLETE_1DAY_A2",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 0 AND REJECTED_7DAY = 0 THEN 1
ELSE 0
END AS "CIP_CONNECTED_REGISTRATION_INCOMPLETE_7DAY_A2",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 0 AND REJECTED_30DAY_V2 = 0 THEN 1
ELSE 0
END AS "CIP_CONNECTED_REGISTRATION_INCOMPLETE_30DAY_A2",


--------B+C-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 0 AND REJECTED_1DAY = 1 THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_REJECT_1DAY_B_C",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 0 AND REJECTED_7DAY = 1 THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_REJECT_7DAY_B_C",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 0 AND REJECTED_30DAY_V2 = 1 THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_REJECT_30DAY_B_C",


--------C-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 0 AND REJECTED_1DAY = 1 AND CD.current_credit_status_reason like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_AUTO_REJECT_1DAY_C",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 0 AND REJECTED_7DAY = 1 AND CD.current_credit_status_reason like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_AUTO_REJECT_7DAY_C",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 0 AND REJECTED_30DAY_V2 = 1 AND CD.current_credit_status_reason like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_AUTO_REJECT_30DAY_C",

--------B-----------------
CASE when ERROR_FLAG2_1DAY = 0 AND INELIGIBLE_1DAY = 0 AND CIP_NOT_CONNECTED_1DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_1DAY = 0 AND REJECTED_1DAY = 1 AND CD.current_credit_status_reason NOT like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_1DAY_B",
CASE when ERROR_FLAG2_7DAY = 0 AND INELIGIBLE_7DAY = 0 AND CIP_NOT_CONNECTED_7DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_7DAY = 0 AND REJECTED_7DAY = 1 AND CD.current_credit_status_reason NOT like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_7DAY_B",
CASE when ERROR_FLAG2_30DAY = 0 AND INELIGIBLE_30DAY = 0 AND CIP_NOT_CONNECTED_30DAY_A1 = 0 AND REGISTRATION_FLOW_COMPLETE_30DAY = 0 AND REJECTED_30DAY_V2 = 1 AND CD.current_credit_status_reason NOT like 'Didn%t complete flow' THEN 1
ELSE 0
END AS "REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_30DAY_B"


-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------

FROM indus."PUBLIC".customers_data_indus cd

--CHANGE MADE ON 2024-03-12 FOR CREDIT LIMIT AT THE TIME OF FIRST DRAW

LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd 
on cd.fbbid = dacd.fbbid 
and cd.first_draw_time::date = dacd.edate

--LEFT JOIN ANALYTICS.CREDIT.eg_key_metrics_filters f 
LEFT JOIN INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS f 


ON cd.fbbid = f.fbbid
AND f.edate = f.min_edate

--LEFT JOIN indus."PUBLIC".pre_qual_users_indus pq
--ON cd.fbbid = pq.fbbid


LEFT JOIN (
select * from 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY last_modified_time DESC) AS row_num FROM indus."PUBLIC".pre_qual_users_indus a)
where row_num = 1) pq
ON cd.fbbid = pq.fbbid


LEFT JOIN indus."PUBLIC".approved_customers_data_indus acd 
ON cd.fbbid = acd.fbbid

-- 27Feb add for Rejection Reasons

LEFT JOIN indus.public.feb_report AS Feb
ON cd.fbbid = feb.fbbid

WHERE TRUE 
--AND registration_date >= '2020-01-01'
AND is_test = 0
--AND cd.SUB_PRODUCT <> 'Credit Builder'
--AND is_rejected = 1
--AND rejected_reason = 'Other'

)

SELECT a.*
, reg.registrations
, cip.cip_connections
, con.connections
, flo.reg_flow_completed
, risk.risk_reviewed
, und.underwritten_old
, und.underwritten
, und.underwritten_not_null
, und.underwritten_pq_months_in_business
, und.underwritten_has_pq_months_in_business
, und.underwritten_calc_revenue
, und.underwritten_has_calc_revenue
, deci.decisions
, app.approvals
, app.approvals_not_null
, deci.rejections
--, deci.state_rejections
, deci.data_rejections
, deci.fico_599_rejections
, deci.credit_rejections
, deci.fraud_rejections
--, deci.policy_rejections
, deci.policy_model_rejections
, deci.other_rejections
, ftd.ftds
, ftd.ftds_0_7
, ftd.ftds_8_28
, ftd.ftds_29_60
, ftd.ftds_61_
, app.ftds_app
, app.ftds_7 
, app.ftds_28 
, und.sum_fico_und
, und.sum_risk_und
, app.sum_fico_app
, app.sum_risk_app
, app.app_credit_limit AS app_credit_limit 
, ftd.ftd_credit_limit
, ftd.first_draw_amount


/*
, registrations_horizontal_1day
, underwritten_horizontal_1day
, reg_cip_incomplete_horizontal_a1_1day
, reg_cip_complete_horizontal_a2_1day
, app_incomplete_rej_other_reason_horizontal_b_1day
, app_incomplete_auto_rej_horizontal_c_1day
, flow_complete_not_uw_horizontal_d_1day
, flow_complete_rej_horizontal_e_1day
, risk_review_horizontal_f_1day

, registrations_horizontal_7day
, underwritten_horizontal_7day
, reg_cip_incomplete_horizontal_a1_7day
, reg_cip_complete_horizontal_a2_7day
, app_incomplete_rej_other_reason_horizontal_b_7day
, app_incomplete_auto_rej_horizontal_c_7day
, flow_complete_not_uw_horizontal_d_7day
, flow_complete_rej_horizontal_e_7day
, risk_review_horizontal_f_7day

, registrations_horizontal_30day
, underwritten_horizontal_30day
, reg_cip_incomplete_horizontal_a1_30day
, reg_cip_complete_horizontal_a2_30day
, app_incomplete_rej_other_reason_horizontal_b_30day
, app_incomplete_auto_rej_horizontal_c_30day
, flow_complete_not_uw_horizontal_d_30day
, flow_complete_rej_horizontal_e_30day
, risk_review_horizontal_f_30day
*/

, CIP_NOT_CONNECTED_1DAY_A1
, CIP_CONNECTED_REGISTRATION_INCOMPLETE_1DAY_A2
, REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_1DAY_B
, REGISTRATION_INCOMPLETE_AUTO_REJECT_1DAY_C
, REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_1DAY_D
, REGISTRATION_COMPLETE_REJECT_PRE_UW_1DAY_E
, RISK_REVIEW_REJECT_1DAY_F
, RISK_REVIEW_ACCEPT_1DAY_G
, INELIGIBLE_1DAY
, ERROR_FLAG2_1DAY

, CIP_NOT_CONNECTED_7DAY_A1
, CIP_CONNECTED_REGISTRATION_INCOMPLETE_7DAY_A2
, REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_7DAY_B
, REGISTRATION_INCOMPLETE_AUTO_REJECT_7DAY_C
, REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_7DAY_D
, REGISTRATION_COMPLETE_REJECT_PRE_UW_7DAY_E
, RISK_REVIEW_REJECT_7DAY_F
, RISK_REVIEW_ACCEPT_7DAY_G
, INELIGIBLE_7DAY
, ERROR_FLAG2_7DAY

, CIP_NOT_CONNECTED_30DAY_A1
, CIP_CONNECTED_REGISTRATION_INCOMPLETE_30DAY_A2
, REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_30DAY_B
, REGISTRATION_INCOMPLETE_AUTO_REJECT_30DAY_C
, REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_30DAY_D
, REGISTRATION_COMPLETE_REJECT_PRE_UW_30DAY_E
, RISK_REVIEW_REJECT_30DAY_F
, RISK_REVIEW_ACCEPT_30DAY_G
, INELIGIBLE_30DAY
, ERROR_FLAG2_30DAY

--FROM ANALYTICS.CREDIT.eg_key_metrics_funnel_structure a 
FROM INDUS.PUBLIC.INDUS_key_metrics_funnel_structure a 


---------------------------------------------------------------------------------------------------------------- Horizontal metrics

LEFT JOIN (
SELECT WEEK_END
, channel
,TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
 

-- 1day metrics
, sum(CIP_NOT_CONNECTED_1DAY_A1) as CIP_NOT_CONNECTED_1DAY_A1
, sum(CIP_CONNECTED_REGISTRATION_INCOMPLETE_1DAY_A2) as CIP_CONNECTED_REGISTRATION_INCOMPLETE_1DAY_A2
, sum(REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_1DAY_B) as REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_1DAY_B
, sum(REGISTRATION_INCOMPLETE_AUTO_REJECT_1DAY_C) as REGISTRATION_INCOMPLETE_AUTO_REJECT_1DAY_C
, sum(REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_1DAY_D) as REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_1DAY_D
, sum(REGISTRATION_COMPLETE_REJECT_PRE_UW_1DAY_E) as REGISTRATION_COMPLETE_REJECT_PRE_UW_1DAY_E
, sum(RISK_REVIEW_REJECT_1DAY_F) as RISK_REVIEW_REJECT_1DAY_F
, sum(RISK_REVIEW_ACCEPT_1DAY_G) as RISK_REVIEW_ACCEPT_1DAY_G
, sum(INELIGIBLE_1DAY) as INELIGIBLE_1DAY
, sum(ERROR_FLAG2_1DAY) as ERROR_FLAG2_1DAY


-- 7 day metrics
, sum(CIP_NOT_CONNECTED_7DAY_A1) as CIP_NOT_CONNECTED_7DAY_A1
, sum(CIP_CONNECTED_REGISTRATION_INCOMPLETE_7DAY_A2) as CIP_CONNECTED_REGISTRATION_INCOMPLETE_7DAY_A2
, sum(REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_7DAY_B) as REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_7DAY_B
, sum(REGISTRATION_INCOMPLETE_AUTO_REJECT_7DAY_C) as REGISTRATION_INCOMPLETE_AUTO_REJECT_7DAY_C
, sum(REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_7DAY_D) as REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_7DAY_D
, sum(REGISTRATION_COMPLETE_REJECT_PRE_UW_7DAY_E) as REGISTRATION_COMPLETE_REJECT_PRE_UW_7DAY_E
, sum(RISK_REVIEW_REJECT_7DAY_F) as RISK_REVIEW_REJECT_7DAY_F
, sum(RISK_REVIEW_ACCEPT_7DAY_G) as RISK_REVIEW_ACCEPT_7DAY_G
, sum(INELIGIBLE_7DAY) as INELIGIBLE_7DAY
, sum(ERROR_FLAG2_7DAY) as ERROR_FLAG2_7DAY



-- 30 day metrics
, sum(CIP_NOT_CONNECTED_30DAY_A1) as CIP_NOT_CONNECTED_30DAY_A1
, sum(CIP_CONNECTED_REGISTRATION_INCOMPLETE_30DAY_A2) as CIP_CONNECTED_REGISTRATION_INCOMPLETE_30DAY_A2
, sum(REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_30DAY_B) as REGISTRATION_INCOMPLETE_OTHER_REASON_REJECT_30DAY_B
, sum(REGISTRATION_INCOMPLETE_AUTO_REJECT_30DAY_C) as REGISTRATION_INCOMPLETE_AUTO_REJECT_30DAY_C
, sum(REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_30DAY_D) as REGISTRATION_COMPLETE_NOT_UNDERWRITTEN_30DAY_D
, sum(REGISTRATION_COMPLETE_REJECT_PRE_UW_30DAY_E) as REGISTRATION_COMPLETE_REJECT_PRE_UW_30DAY_E
, sum(RISK_REVIEW_REJECT_30DAY_F) as RISK_REVIEW_REJECT_30DAY_F
, sum(RISK_REVIEW_ACCEPT_30DAY_G) as RISK_REVIEW_ACCEPT_30DAY_G
, sum(INELIGIBLE_30DAY) as INELIGIBLE_30DAY
, sum(ERROR_FLAG2_30DAY) as ERROR_FLAG2_30DAY


FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) horizontal
ON a.week_end_date = horizontal.WEEK_END
AND a.channel = horizontal.channel
AND a.TERMUNITS = horizontal.TERMUNITS
AND a.partner = horizontal.partner
AND a.intuit_flow = horizontal.intuit_flow
AND a.national_funding_flow = horizontal.national_funding_flow
AND a.nav_flow = horizontal.nav_flow
AND a.lendio_flow = horizontal.lendio_flow
AND a.tier = horizontal.tier
AND a.sub_product = horizontal.sub_product
AND a.bucket_group = horizontal.bucket_group
AND a.reg_client = horizontal.reg_client
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------OTHER METRICS------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

LEFT JOIN (
SELECT reg_start_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) registrations
FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) reg
ON a.week_end_date = reg.reg_start_week_end_date
AND a.channel = reg.channel
AND a.TERMUNITS = reg.TERMUNITS
AND a.partner = reg.partner
AND a.intuit_flow = reg.intuit_flow
AND a.national_funding_flow = reg.national_funding_flow
AND a.nav_flow = reg.nav_flow
AND a.lendio_flow = reg.lendio_flow
AND a.tier = reg.tier
AND a.sub_product = reg.sub_product
AND a.bucket_group = reg.bucket_group
AND a.reg_client = reg.reg_client

LEFT JOIN (
SELECT cip_connected_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) cip_connections
FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) cip
ON a.week_end_date = cip.cip_connected_week_end_date
AND a.channel = cip.channel
AND a.TERMUNITS = cip.TERMUNITS
AND a.partner = cip.partner
AND a.intuit_flow = cip.intuit_flow
AND a.national_funding_flow = cip.national_funding_flow
AND a.nav_flow = cip.nav_flow
AND a.lendio_flow = cip.lendio_flow
AND a.tier = cip.tier
AND a.sub_product = cip.sub_product
AND a.bucket_group = cip.bucket_group
AND a.reg_client = cip.reg_client

LEFT JOIN (
SELECT first_connected_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) connections
FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) con
ON a.week_end_date = con.first_connected_week_end_date
AND a.TERMUNITS = con.TERMUNITS
AND a.channel = con.channel
AND a.partner = con.partner
AND a.intuit_flow = con.intuit_flow
AND a.national_funding_flow = con.national_funding_flow
AND a.nav_flow = con.nav_flow
AND a.lendio_flow = con.lendio_flow
AND a.tier = con.tier
AND a.sub_product = con.sub_product
AND a.bucket_group = con.bucket_group
AND a.reg_client = con.reg_client


LEFT JOIN (
SELECT reg_complete_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) reg_flow_completed
FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) flo
ON a.week_end_date = flo.reg_complete_week_end_date
AND a.TERMUNITS = flo.TERMUNITS
AND a.channel = flo.channel
AND a.partner = flo.partner
AND a.intuit_flow = flo.intuit_flow
AND a.national_funding_flow = flo.national_funding_flow
AND a.nav_flow = flo.nav_flow
AND a.lendio_flow = flo.lendio_flow
AND a.tier = flo.tier
AND a.sub_product = flo.sub_product
AND a.bucket_group = flo.bucket_group
AND a.reg_client = flo.reg_client


LEFT JOIN (
SELECT risk_review_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) risk_reviewed
FROM funnel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) risk
ON a.week_end_date = risk.risk_review_week_end_date
AND a.channel = risk.channel
AND a.TERMUNITS = risk.TERMUNITS
AND a.partner = risk.partner
AND a.intuit_flow = risk.intuit_flow
AND a.national_funding_flow = risk.national_funding_flow
AND a.nav_flow = risk.nav_flow
AND a.lendio_flow = risk.lendio_flow
AND a.tier = risk.tier
AND a.sub_product = risk.sub_product
AND a.bucket_group = risk.bucket_group
AND a.reg_client = risk.reg_client

LEFT JOIN (
SELECT reg_complete_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
--sum(CASE WHEN risk_bucket IS NULL THEN 0 ELSE originated_amount END) orig_bucket_not_null
, sum(is_underwriting_old) underwritten_old
, count(DISTINCT fbbid) underwritten
, COUNT(DISTINCT CASE WHEN risk_bucket IS NOT NULL AND risk_bucket != 0 THEN fbbid END) AS underwritten_not_null
, sum(pq_months_in_business) underwritten_pq_months_in_business
, sum(has_pq_months_in_business) underwritten_has_pq_months_in_business
, sum(calc_revenue) underwritten_calc_revenue
, sum(has_calc_revenue) underwritten_has_calc_revenue
, sum(fico) sum_fico_und
, sum(risk_bucket) sum_risk_und
FROM funnel
WHERE is_underwriting_new = 1 -- Distinguishes flow completed FROM underwritten (new logic)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) und
ON a.week_end_date = und.reg_complete_week_end_date
AND a.channel = und.channel
AND a.TERMUNITS = und.TERMUNITS
AND a.partner = und.partner
AND a.intuit_flow = und.intuit_flow
AND a.national_funding_flow = und.national_funding_flow
AND a.nav_flow = und.nav_flow
AND a.lendio_flow = und.lendio_flow
AND a.tier = und.tier
AND a.sub_product = und.sub_product
AND a.bucket_group = und.bucket_group
AND a.reg_client = und.reg_client

/*
LEFT JOIN (
SELECT first_dec_week_end_date
, channel
, partner
, intuit_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, count(DISTINCT fbbid) decisions
, sum(is_rejected) rejections
, sum(CASE WHEN rejected_reason = 'State' THEN 1 ELSE 0 END) state_rejections
, sum(CASE WHEN rejected_reason = 'Data Rules' THEN 1 ELSE 0 END) data_rejections
, sum(CASE WHEN rejected_reason = 'FICO < 600' THEN 1 ELSE 0 END) fico_599_rejections
, sum(CASE WHEN rejected_reason = 'Credit Rules' THEN 1 ELSE 0 END) credit_rejections
, sum(CASE WHEN rejected_reason = 'Fraud Rules' THEN 1 ELSE 0 END) fraud_rejections
, sum(CASE WHEN rejected_reason = 'Policy' THEN 1 ELSE 0 END) policy_rejections
FROM funnel
WHERE is_registration_flow_completed = 1
GROUP BY 1,2,3,4,5,6,7,8,9) deci
ON a.week_end_date = deci.first_dec_week_end_date
AND a.channel = deci.channel
AND a.partner = deci.partner
AND a.intuit_flow = deci.intuit_flow
AND a.nav_flow = deci.nav_flow
AND a.lendio_flow = deci.lendio_flow
AND a.tier = deci.tier
AND a.sub_product = deci.sub_product
AND a.bucket_group = deci.bucket_group
*/

---- 29 Feb add for Rejection Reason


LEFT JOIN (
SELECT first_dec_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) decisions
, sum(is_rejected) rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'FICO Sub 600' THEN 1 ELSE 0 END) fico_599_rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'Data Rule Reject' THEN 1 ELSE 0 END) data_rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'Credit Rule Reject' THEN 1 ELSE 0 END) credit_rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'Policy Rejection/Model Rejection' THEN 1 ELSE 0 END) policy_model_rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'Fraud Rejection' THEN 1 ELSE 0 END) fraud_rejections
, sum(CASE WHEN REJECTION_REASON_2 = 'Other Rejections' THEN 1 ELSE 0 END) other_rejections
FROM funnel
WHERE is_registration_flow_completed = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) deci
ON a.week_end_date = deci.first_dec_week_end_date
AND a.channel = deci.channel
AND a.TERMUNITS = deci.TERMUNITS
AND a.partner = deci.partner
AND a.intuit_flow = deci.intuit_flow
AND a.national_funding_flow = deci.national_funding_flow
AND a.nav_flow = deci.nav_flow
AND a.lendio_flow = deci.lendio_flow
AND a.tier = deci.tier
AND a.sub_product = deci.sub_product
AND a.bucket_group = deci.bucket_group
AND a.reg_client = deci.reg_client

LEFT JOIN (
SELECT app_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, count(DISTINCT fbbid) approvals
, COUNT(DISTINCT CASE WHEN risk_bucket IS NOT NULL AND risk_bucket != 0 THEN fbbid END) AS approvals_not_null
, sum(is_ftu) ftds_app
, sum(is_ftd7) ftds_7
, sum(is_ftd28) ftds_28
, sum(fico) sum_fico_app
, sum(risk_bucket) sum_risk_app
, sum(first_approved_credit_limit) app_credit_limit
FROM funnel
WHERE TRUE 
AND is_approved = 1
-----------------31st May Change----------------
AND sub_product <> 'Credit Builder'
---------------------------------------------------------
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) app
ON a.week_end_date = app.app_week_end_date
AND a.channel = app.channel
AND a.TERMUNITS = app.TERMUNITS
AND a.partner = app.partner
AND a.intuit_flow = app.intuit_flow
AND a.national_funding_flow = app.national_funding_flow
AND a.nav_flow = app.nav_flow
AND a.lendio_flow = app.lendio_flow
AND a.tier = app.tier
AND a.sub_product = app.sub_product
AND a.bucket_group = app.bucket_group
AND a.reg_client = app.reg_client
-----------------31st May Change----------------
AND app.sub_product <> 'Credit Builder'
-----------------------------------------------
/*LEFT JOIN (
SELECT
	week_end_date
	, channel
	, TERMUNITS
	, partner
	, intuit_flow
	, national_funding_flow
	, nav_flow
	, lendio_flow
	, tier
	, sub_product
	, bucket_group
	, reg_client
	, V_NEW_EXP_INC
FROM INDUS.PUBLIC.INDUS_KEY_METRICS_AUW_AGG_BU
) AUW
ON a.week_end_date = AUW.week_end_date
AND a.channel = AUW.channel
AND a.TERMUNITS = AUW.TERMUNITS
AND a.partner = AUW.partner
AND a.intuit_flow = AUW.intuit_flow
AND a.national_funding_flow = AUW.national_funding_flow
AND a.nav_flow = AUW.nav_flow
AND a.lendio_flow = AUW.lendio_flow
AND a.tier = AUW.tier
AND a.sub_product = AUW.sub_product
AND a.bucket_group = AUW.bucket_group
AND a.reg_client = AUW.reg_client
*/

LEFT JOIN (
SELECT ftd_week_end_date
, channel
, TERMUNITS
, partner
, intuit_flow
, national_funding_flow
, nav_flow
, lendio_flow
, tier
, sub_product
, bucket_group
, reg_client
, sum(is_ftu) ftds
, sum(is_ftd_0_7) ftds_0_7
, sum(is_ftd_8_28) ftds_8_28
, sum(is_ftd_29_60) ftds_29_60
, sum(is_ftd_61_) ftds_61_
--change on 12-03-2024 for avg utilization at first draw
--, sum(first_approved_credit_limit) ftd_credit_limit
, sum(credit_limit_at_ftd) ftd_credit_limit 
, sum(fda) first_draw_amount
FROM funnel
WHERE TRUE
-----------------31st May Change----------------
AND sub_product <> 'Credit Builder'
------------------------------------------------
AND is_ftu = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) ftd
ON a.week_end_date = ftd.ftd_week_end_date
AND a.channel = ftd.channel
AND a.TERMUNITS = ftd.TERMUNITS
AND a.partner = ftd.partner
AND a.intuit_flow = ftd.intuit_flow
AND a.national_funding_flow = ftd.national_funding_flow
AND a.nav_flow = ftd.nav_flow
AND a.lendio_flow = ftd.lendio_flow
AND a.tier = ftd.tier
AND a.sub_product = ftd.sub_product
AND a.bucket_group = ftd.bucket_group
AND a.reg_client = ftd.reg_client
-----------------31st May Change----------------
AND ftd.sub_product <> 'Credit Builder'
-----------------------------------------------


WHERE TRUE 
AND (reg.registrations IS NOT NULL 
OR cip.cip_connections IS NOT NULL 
OR con.connections IS NOT NULL 
OR flo.reg_flow_completed IS NOT NULL 
OR risk.risk_reviewed IS NOT NULL 
OR und.underwritten_old IS NOT NULL 
OR und.underwritten IS NOT NULL 
OR und.underwritten_not_null IS NOT NULL 
OR und.underwritten_pq_months_in_business IS NOT NULL 
OR und.underwritten_has_pq_months_in_business IS NOT NULL 
OR und.underwritten_calc_revenue IS NOT NULL 
OR und.underwritten_has_calc_revenue IS NOT NULL 
OR deci.decisions IS NOT NULL 
OR app.approvals IS NOT NULL 
OR app.approvals_not_null IS NOT NULL 
OR deci.rejections IS NOT NULL 
--OR deci.state_rejections IS NOT NULL 
OR deci.data_rejections IS NOT NULL 
OR deci.fico_599_rejections IS NOT NULL 
OR deci.credit_rejections IS NOT NULL 
OR deci.fraud_rejections IS NOT NULL 
--OR deci.policy_rejections IS NOT NULL 
OR deci.policy_model_rejections IS NOT NULL
OR deci.other_rejections IS NOT NULL
OR ftd.ftds IS NOT NULL 
OR ftd.ftds_0_7 IS NOT NULL 
OR ftd.ftds_8_28 IS NOT NULL 
OR ftd.ftds_29_60 IS NOT NULL 
OR ftd.ftds_61_ IS NOT NULL 
OR app.ftds_app IS NOT NULL 
OR app.ftds_7  IS NOT NULL 
OR app.ftds_28  IS NOT NULL 
OR und.sum_fico_und IS NOT NULL 
OR und.sum_risk_und IS NOT NULL 
OR app.sum_fico_app IS NOT NULL 
OR app.sum_risk_app IS NOT NULL 
OR app.app_credit_limit IS NOT NULL 
OR ftd.ftd_credit_limit IS NOT NULL 
OR ftd.first_draw_amount IS NOT NULL)

--ORDER BY 1 DESC, 2, 3, 4, 5, 6



--GROUP BY 1 ORDER BY 1 DESC 

);




--DISTINCT CASE WHEN risk_bucket IS NOT NULL AND risk_bucket != 0 THEN fbbid END

/*SELECT 
    week_end_date, 
    SUM(sum_risk_app) , SUM(approvals_not_null) , sum(approvals) ,
    SUM(sum_risk_und) , SUM(underwritten_not_null) ,sum(underwritten),
    SUM(sum_risk_app) / SUM(approvals_not_null) AS risk_app_ratio,
    SUM(sum_risk_und) / SUM(underwritten_not_null) AS risk_und_ratio
    from INDUS.PUBLIC.INDUS_KEY_METRICS_FUNNEL_AGG group by 1 order by week_end_date desc;*/













