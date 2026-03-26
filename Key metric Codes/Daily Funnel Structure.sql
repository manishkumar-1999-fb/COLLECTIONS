create or replace view INDUS.PUBLIC.INDUS_KEY_METRICS_FUNNEL_STRUCTURE_DAILY(
	CURR_DATE,
	CHANNEL,
	TIER,
	SUB_PRODUCT,
	BUCKET_GROUP,
	PARTNER,
	INTUIT_FLOW,
	NAV_FLOW,
	LENDIO_FLOW,
	NATIONAL_FUNDING_FLOW
) as


WITH RECURSIVE a AS (
	  SELECT DATEADD('day',-40,current_date()) AS curr_date
	  UNION ALL
	  SELECT DATEADD(DAY, 1, curr_date)
	  FROM a
	  WHERE curr_date < current_date()
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

/*
, partner_table AS (
SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner
)
*/

-- , partner_table AS (
-- SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner
-- )


-- /*
-- , intuit_flow AS (
-- SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
-- )
-- */
-- , intuit_flow AS (
-- SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Lendio' partner, 'not intuit' intuit_flow UNION SELECT 'Fundera' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
-- )


-- /*
-- , nav_flow AS (
-- SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'Clover' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
-- )
-- */

-- , nav_flow AS (
-- SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Lendio' partner, 'not nav' nav_flow UNION SELECT 'Fundera' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'CTA' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
-- )


-- --17 Jan addition
-- , lendio_flow AS (
-- SELECT 'Freshbooks' partner, 'not lendio' lendio_flow UNION SELECT 'Intuit' partner, 'not lendio' lendio_flow UNION SELECT 'Housecall Pro' partner, 'not lendio' lendio_flow UNION SELECT 'IFS' partner, 'not lendio' lendio_flow UNION SELECT 'Forbes Advisors' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Brokers' partner, 'not lendio' lendio_flow UNION SELECT 'Direct' partner, 'not lendio' lendio_flow UNION SELECT 'Other Partners' partner, 'not lendio' lendio_flow  UNION SELECT 'Fundera' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Partners' partner, 'not lendio' lendio_flow UNION SELECT 'Other' partner, 'not lendio' lendio_flow UNION SELECT 'Nav' partner, 'not lendio' lendio_flow UNION SELECT 'Lendio' partner, 'Non-sales' lendio_flow UNION SELECT 'Lendio' partner, 'Direct-sales' lendio_flow UNION SELECT 'Lendio' partner, 'Embedded' lendio_flow


-- 7 Feb - Business loans added
-- , partner_table AS (
-- SELECT 'Intuit' partner UNION SELECT 'Freshbooks' partner UNION SELECT 'Nav' partner UNION SELECT 'Housecall Pro' partner UNION SELECT 'IFS' partner UNION SELECT 'Forbes Advisors' partner UNION SELECT 'Terminated Brokers' partner UNION SELECT 'Direct' partner UNION SELECT 'Sofi' partner UNION SELECT 'Lendio' partner UNION SELECT 'Fundera' partner UNION SELECT 'BusinessLoans' partner UNION SELECT 'AtoB' partner UNION SELECT 'Other Partners' partner UNION SELECT 'Terminated Partners' partner UNION SELECT 'Other' partner
-- )

-- 28 Aug - National Funding Super Added
, partner_table AS (
SELECT 'Intuit' partner UNION SELECT 
'Freshbooks' partner UNION SELECT 
'Nav' partner UNION SELECT 
'Housecall Pro' partner UNION SELECT 
'IFS' partner UNION SELECT 
'Forbes Advisors' partner UNION SELECT 
'Terminated Brokers' partner UNION SELECT 
'Direct' partner UNION SELECT 
'Sofi' partner UNION SELECT 
'Cardiff' partner UNION SELECT 
'Lendio' partner UNION SELECT 
'Fundera' partner UNION SELECT 
'BusinessLoans' partner UNION SELECT 
'AtoB' partner UNION SELECT 
'Other Partners' partner UNION SELECT 
'Terminated Partners' partner UNION SELECT 
'Other' partner UNION SELECT 
'National Funding Super' partner UNION SELECT 
'Bluevine' partner UNION SELECT 
'ZenBusiness' partner UNION SELECT 
'Anansii' partner UNION SELECT 
'Guesty' partner UNION SELECT 
'Cantaloupe' partner UNION SELECT 
'Joist' partner UNION SELECT 
'1West' partner UNION SELECT 
'Autobooks' partner UNION SELECT 
'Franpos'
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

-- 28 Aug - National Funding Super Added
, intuit_flow AS (
SELECT 'Freshbooks' partner, 'not intuit' intuit_flow UNION SELECT 'Cantaloupe' partner,'not intuit' intuit_flow UNION SELECT 'Franpos' partner,'not intuit' intuit_flow UNION SELECT 'Autobooks' partner, 'not intuit' intuit_flow UNION SELECT 'Nav' partner, 'not intuit' intuit_flow UNION SELECT 'Housecall Pro' partner, 'not intuit' intuit_flow UNION SELECT 'IFS' partner, 'not intuit' intuit_flow UNION SELECT 'Forbes Advisors' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Brokers' partner, 'not intuit' intuit_flow UNION SELECT 'Direct' partner, 'not intuit' intuit_flow UNION SELECT 'Other Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Sofi' partner,'not intuit' intuit_flow UNION SELECT 'Cardiff' partner, 'not intuit' intuit_flow UNION SELECT 'Lendio' partner, 'not intuit' intuit_flow UNION SELECT 'Fundera' partner, 'not intuit' intuit_flow UNION SELECT 'BusinessLoans' partner, 'not intuit' intuit_flow UNION SELECT 'AtoB' partner, 'not intuit' intuit_flow UNION SELECT 'Terminated Partners' partner, 'not intuit' intuit_flow UNION SELECT 'Other' partner, 'not intuit' intuit_flow UNION SELECT 'National Funding Super' partner, 'not intuit' intuit_flow UNION SELECT 'Bluevine' partner,'not intuit' intuit_flow UNION SELECT 'ZenBusiness' partner,'not intuit' intuit_flow UNION SELECT 'Joist' partner, 'not intuit' intuit_flow UNION SELECT '1West' partner, 'not intuit' intuit_flow UNION SELECT 'Anansii' partner, 'not intuit' intuit_flow UNION SELECT 'Guesty' partner, 'not intuit' intuit_flow UNION SELECT 'Intuit' partner, 'Other SSO' intuit_flow UNION SELECT 'Intuit' partner, 'Email Campaign' intuit_flow UNION SELECT 'Intuit' partner, 'AppCenter' intuit_flow UNION SELECT 'Intuit' partner, 'Marketplace' intuit_flow
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
SELECT 'Freshbooks' partner, 'not nav' nav_flow UNION SELECT 'Intuit' partner, 'not nav' nav_flow UNION SELECT '1West' partner, 'not nav' nav_flow UNION SELECT 'Housecall Pro' partner, 'not nav' nav_flow UNION SELECT 'IFS' partner, 'not nav' nav_flow UNION SELECT 'Forbes Advisors' partner, 'not nav' nav_flow UNION SELECT 'Terminated Brokers' partner, 'not nav' nav_flow UNION SELECT 'Direct' partner, 'not nav' nav_flow UNION SELECT 'Other Partners' partner, 'not nav' nav_flow UNION SELECT 'Sofi' partner,'not nav' nav_flow UNION SELECT 'Cardiff' partner, 'not nav' nav_flow UNION SELECT 'Lendio' partner,'not nav' nav_flow UNION SELECT 'Cantaloupe' partner,'not nav' nav_flow UNION SELECT 'Franpos' partner,'not nav' nav_flow UNION SELECT 'Autobooks' partner, 'not nav' nav_flow UNION SELECT 'Fundera' partner, 'not nav' nav_flow UNION SELECT 'BusinessLoans' partner, 'not nav' nav_flow UNION SELECT 'AtoB' partner, 'not nav' nav_flow UNION SELECT 'Terminated Partners' partner, 'not nav' nav_flow UNION SELECT 'Other' partner, 'not nav' nav_flow UNION SELECT 'National Funding Super' partner, 'not nav' nav_flow UNION SELECT 'Bluevine' partner,'not nav' nav_flow UNION SELECT 'ZenBusiness' partner,'not nav' nav_flow UNION SELECT 'Joist' partner, 'not nav' nav_flow UNION SELECT 'Anansii' partner, 'not nav' nav_flow UNION SELECT 'Guesty' partner, 'not nav' nav_flow UNION SELECT 'Nav' partner, 'Nav Expansion Test' nav_flow UNION SELECT 'Nav' partner, 'Nav Pre-approval' nav_flow UNION SELECT 'Nav' partner, 'Logged In' nav_flow UNION SELECT 'Nav' partner, 'Logged Out' nav_flow UNION SELECT 'Nav' partner, 'CTA' nav_flow UNION SELECT 'Nav' partner, 'Mobile' nav_flow UNION SELECT 'Nav' partner, 'Other' nav_flow
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
SELECT 'Freshbooks' partner, 'not lendio' lendio_flow UNION SELECT 'Intuit' partner, 'not lendio' lendio_flow UNION SELECT '1West' partner, 'not lendio' lendio_flow UNION SELECT 'Housecall Pro' partner, 'not lendio' lendio_flow UNION SELECT 'IFS' partner, 'not lendio' lendio_flow UNION SELECT 'Forbes Advisors' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Brokers' partner, 'not lendio' lendio_flow UNION SELECT 'Direct' partner, 'not lendio' lendio_flow UNION SELECT 'Other Partners' partner, 'not lendio' lendio_flow  UNION SELECT 'Fundera' partner, 'not lendio' lendio_flow UNION SELECT 'BusinessLoans' partner, 'not lendio' lendio_flow UNION SELECT 'AtoB' partner, 'not lendio' lendio_flow UNION SELECT 'Terminated Partners' partner, 'not lendio' lendio_flow UNION SELECT 'Other' partner, 'not lendio' lendio_flow UNION SELECT 'Nav' partner, 'not lendio' lendio_flow UNION SELECT 'Sofi' partner,'not lendio' lendio_flow UNION SELECT 'Cardiff' partner, 'not lendio' lendio_flow UNION SELECT 'National Funding Super' partner, 'not lendio' lendio_flow UNION SELECT 'Bluevine' partner,'not lendio' lendio_flow UNION SELECT 'Cantaloupe' partner,'not lendio' lendio_flow UNION SELECT 'Franpos' partner,'not lendio' lendio_flow UNION SELECT 'Autobooks' partner,'not lendio' lendio_flow UNION SELECT 'ZenBusiness' partner,'not lendio' lendio_flow UNION SELECT 'Joist' partner, 'not lendio' lendio_flow UNION SELECT 'Anansii' partner, 'not lendio' lendio_flow UNION SELECT 'Guesty' partner, 'not lendio' lendio_flow UNION SELECT 'Lendio' partner, 'Non-Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Direct Sales' lendio_flow UNION SELECT 'Lendio' partner, 'Lendio Embedded' lendio_flow
)

-- 28 Aug - National Funding Super added
, national_funding_flow AS (
SELECT 'Freshbooks' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Intuit' partner, 'Not National Funding' national_funding_flow UNION SELECT '1West' partner,'Not National Funding' national_funding_flow UNION SELECT 'Franpos' partner,'Not National Funding' national_funding_flow UNION SELECT 'Autobooks' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Bluevine' partner,'Not National Funding' national_funding_flow UNION SELECT 'Cantaloupe' partner,'Not National Funding' national_funding_flow UNION SELECT 'ZenBusiness' partner,'Not National Funding' national_funding_flow UNION SELECT 'Joist' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Anansii' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Guesty' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Housecall Pro' partner, 'Not National Funding' national_funding_flow UNION SELECT 'IFS' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Forbes Advisors' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Terminated Brokers' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Direct' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Other Partners' partner, 'Not National Funding' national_funding_flow  UNION SELECT 'Fundera' partner, 'Not National Funding' national_funding_flow UNION SELECT 'BusinessLoans' partner, 'Not National Funding' national_funding_flow UNION SELECT 'AtoB' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Terminated Partners' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Other' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Nav' partner, 'Not National Funding' national_funding_flow UNION SELECT 'Sofi' partner,'Not National Funding' national_funding_flow UNION SELECT 'Cardiff' partner, 'Not National Funding' national_funding_flow UNION SELECT 'National Funding Super' partner, 'Moneyjet' national_funding_flow UNION SELECT 'National Funding Super' partner, 'Small Business Loans' national_funding_flow UNION SELECT 'National Funding Super' partner, 'National Funding' national_funding_flow UNION SELECT 'National Funding Super' partner, 'QuickBridge' national_funding_flow UNION SELECT 'Lendio' partner, 'Not National Funding' national_funding_flow
)


SELECT a.curr_date
, ct.channel
, tt.tier
, spt.sub_product
, rbt.risk_bucket bucket_group
, pt.partner
, if_.intuit_flow
, nf.nav_flow
, lf.lendio_flow
, nff.national_funding_flow



FROM a

CROSS JOIN channel_table ct
CROSS JOIN tier_table tt
CROSS JOIN sub_product_table spt
CROSS JOIN risk_bucket_table rbt
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
;