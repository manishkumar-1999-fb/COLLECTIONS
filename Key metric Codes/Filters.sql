/*Rules to use: 

1. To view the code, open the document in view mode
2. If you make a change in the filters code and get it peer reviewed, only then change the code in here.
3. The latest version of the filters code to be maintained here at all times.*/




CREATE OR REPLACE VIEW INDUS.PUBLIC.INDUS_KEY_METRICS_FILTERS_v2 AS (  
SELECT cd.fbbid
	, cd.registration_time::date reg_date
	, CASE WHEN dacd.edate IS NULL THEN cd.registration_time::date 
	       ELSE dacd.edate
		   END edate
	---25TH MAR'25	
	, DACD.first_approved_time::date APP_DATE
	, CASE 
		WHEN cd.first_approved_time IS NULL THEN 0
		ELSE CD.first_approved_credit_limit END 
		ICL
	---	   
	, CASE WHEN dacd.edate IS NULL THEN cd.registration_time::date 
		   ELSE min(dacd.edate) OVER (PARTITION BY cd.fbbid)
		   END min_edate
	---		   
	, CASE WHEN DATEDIFF('day',cd.first_approved_time::date,dacd.edate) < 60 THEN 'New Customer'
		   WHEN DATEDIFF('day',cd.first_approved_time::date,dacd.edate) >= 60 THEN 'Existing Customer'
		   WHEN cd.first_approved_time IS NULL THEN 'Not Approved'
		   END new_cust_filter
	----
	---25TH MAR'25
	, CASE
    WHEN cd.first_approved_time IS NULL THEN 'Not Approved'
    WHEN DATEDIFF('day', cd.first_approved_time::date, dacd.edate) < 60 THEN '1-60 days'
    WHEN DATEDIFF('day', cd.first_approved_time::date, dacd.edate) < 180 THEN '60-180 days'
    WHEN DATEDIFF('day', cd.first_approved_time::date, dacd.edate) < 365 THEN '180-365 days'
    WHEN DATEDIFF('day', cd.first_approved_time::date, dacd.edate) < (3 * 365) THEN '1-3 years'
    WHEN DATEDIFF('day', cd.first_approved_time::date, dacd.edate) < (5 * 365) THEN '3-5 years'
    ELSE '5+ years'
END AS TENURE_BUCKET

	, CASE 
			WHEN cd.REGISTRATION_CLIENT ilike 'mobile%' THEN 'Mobile'
			WHEN cd.REGISTRATION_CLIENT = 'desktop' THEN 'Desktop'
			ELSE 'Unknown'
		END reg_client
		
	, CASE 
			WHEN apa.PARTNER_NAME IS NOT NULL THEN 'Partner'
			--WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('brokers','affiliates','marketplace','bd partners','intuit','partners','freshbooks')) THEN 'Partner'
			WHEN (apa.PARTNER_NAME IS NULL AND cd.partner_attribution IN ('non-partner')) THEN 'Direct'
            ELSE 'Other' 
		END AS channel		
	
	-- , CASE 
	-- 		WHEN lower(cd.lt_source) = 'intuit' THEN 'Intuit'
	-- 		WHEN lower(apa.partner_name) IN ('intuit') THEN 'Intuit'
	-- 		WHEN apa.partner_name IN ('Jobber','Zoho','Clio','Freshbooks','Wave') THEN 'Large Partners'
	-- 		WHEN apa.partner_name IN ('Lendio', 'Nav', 'Bluevine', 'SoFi', 'Businessloans' ,'National Funding', 'Forbes Advisors', '1West', 'Credibly', 'Become') THEN 'Marketplaces'
	-- 		WHEN apa.partner_name IN ('Housecall Pro','Guesty','Anansii','ZenBusiness','Contractor Foreman','Tailor Brands','Small Business Loans','Nerdwallet','AtoB','Fundera','QuickBridge','PTN','Lendingtree','Stripe','Moneyjet','Synchrony','Jaydee Ventures','Arc') THEN 'Platform Partners'
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('freshbooks')) THEN 'Large Partners'
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('affiliates','marketplace','bd partners','partners','brokers')) THEN 'Terminated Partners'
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('direct mail','direct paid','organic', 'content','fundboxpay')) then 'Direct'
	-- 		ELSE 'Platform Partners' 
	-- 	END AS TERMUNITS

    , CASE
	WHEN lower(apa.partner_name) IN ('intuit') THEN 'Intuit'
--	WHEN lower(apa.partner_name) IN ('autobooks') THEN 'Autobooks'
	WHEN lower(cd.partner_attribution) IN ('intuit') THEN 'Intuit'
	WHEN lower(cd.partner_attribution) IN ('lendio+1260','1west', 'nav','nav+1250','bluevine','sofi','forbes+advisors+4205','become') THEN 'Marketplaces'
	WHEN lower(cd.partner_attribution) IN ('freshbooks','wave', 'wave 4242', 'clio', 'jobber', 'zoho', 'zoho 1330', 'housecall pro 2897') THEN 'Platforms'
    WHEN cd.registration_campaign_source IN ('Franpos') THEN 'Platforms'
    WHEN apa.partner_name IN ('Small Business Loans','Moneyjet', 'QuickBridge', 'Lendio', 'Nav', 'Bluevine', 'SoFi', 'Businessloans', 'Forbes Advisors', 'National Funding', '1West', 'Credibly', 'Viably', 'Creoate', 'TurboDash', 'Become','Cardiff') THEN 'Marketplaces'
    WHEN apa.partner_name IN ('Freshbooks', 'Wave', 'Clio', 'Jobber', 'Zoho', 'Housecall Pro', 'Guesty', 'Anansii', 'ZenBusiness', 'Cantaloupe', 'Joist','InvoiceSimple', 'Franpos','Autobooks') THEN 'Platforms'
    WHEN (apa.PARTNER_NAME IS NULL AND lower(cd.registration_campaign_id) IN (
        '1800bizfund llc',
            '1st shield inc.',
            '4 pillar consulting llc',
            'abel commercial funding',
            'abm financial group',
            'accel business capital, inc.',
            'ad practitioners, llc',
            'alignable',
            'alignable_community_group',
            'aweber',
            'baker solutions inc',
            'best company',
            'best+company+1502',
            'better impressions',
            'betterimpression',
            'bridge consolidation',
            'bronxlyn enterprises inc',
            'business capitall llc',
            'businessloans.com direct',
            'capfront',
            'caredata llc',
            'central pacific bank',
            'centralpacificbank',
            'cheddar',
            'clear link llc',
            'creative business development, llc',
            'credit master ny inc',
            'credit suite',
            'dmr consulting group inc dba us fund source',
            'dotdash',
            'factor finders, llc',
            'finder',
            'first rate business services',
            'getty advance ii llc',
            'govdocfiling',
            'highland hill capital, llc',
            'inqmatic',
            'marc waring ventures llc',
            'marc+waring+ventures+llc+1202',
            'merchant crm inc',
            'merchant-maverick',
            'merchantmaverick',
            'moonshot',
            'mpf inc.',
            'natural intelligence',
            'onpoint solutions',
            'peach state solutions',
            'prosperous future consulting',
            'provider network direct',
            'ranked media, inc. dba merchant maverick',
            'roadsync',
            'rsn consultants inc',
            'score capital',
            'skip',
            'skip+4088',
            'smb compass',
            'spark llc',
            'storz power',
            'supermoney',
            'therapynotes',
            'walgatti, llc',
            'advance funds network',
            'american capital express',
            'aj gaglio corp/ dba: all access financial',
            'algoseller ltd. dba lending express',
            'become (lending express)',
            'become-ec',
            'become-lp',
            'ally merchant services llc dba all merchant funding',
            'american financial partners',
            'american merchant financial services, llc',
            'amerifi capital group llc',
            'aquinas capital funding',
            'arg business loans llc',
            'big think capital',
            'brickell capital finance',
            'broadway advance llc',
            'business funding 4 you',
            'byzloan corp',
            'cap gap solutions',
            'capfront',
            'capital for business',
            'capital infusion llc',
            'capital soluxions',
            'capital source group llc',
            'cardinal equity group',
            'circadian funding',
            'common funding',
            'consolidated funding, inc.',
            'core business capital corp',
            'crestmont capital llc',
            'crown funding source, llc',
            'dealstruck capital, llc',
            'deer capital usa dba coast to coast funding',
            'direct capital source, inc.',
            'emerald hills capital management llc',
            'factor finders, llc',
            'finance factory',
            'fortisfi',
            'forwardline financial',
            'general merchant funding',
            'global business source - fyncap',
            'go cap advance, inc.',
            'gold capital fund',
            'green door funding llc',
            'halcyon capital',
            'high top funding',
            'highland hill capital, llc',
            'hodler capital group',
            'ifunddaily llc',
            'imperial advance, llc',
            'instant capital',
            'jaydee ventures llc',
            'jct group llc',
            'lend on capital',
            'lendflow, inc.',
            'lending tree',
            'lendver, llc',
            'lendzi',
            'liberty capital solutions, inc.',
            'main street finance group, llc',
            'maverick lending',
            'merchant source inc',
            'momentum business capital',
            'money man 4 business',
            'national business capital & services',
            'online capital',
            'orange financial llc',
            'painted horse financial, llc',
            'platform funding llc',
            'platinum advances llc',
            'premium merchant funding 18, llc',
            'premium merchant funding team 1',
            'premium merchant funding team 3',
            'premium merchant funding- team nacho',
            'quick fast capital',
            'quick funding solutions llc',
            'rapid advance',
            'redwood growth',
            'reil capital llc',
            'rok fi llc',
            'score capital',
            'secure capital solutions',
            'sellyoumoney, llc',
            'simply funded',
            'small biz lender',
            'smartbiz',
            'south end capital',
            'south end capital corporation',
            'south shore funding',
            'strategic capital',
            'strategic funding kapitus',
            'tillful',
            'trinity finance',
            'united capital source inc',
            'upfront capital inc',
            'wcp financial llc',
            'we fund capital, llc',
            'westwood funding solutions',
            'white bridge funding group llc',
            'yarrow financial'


    )) THEN 'Terminated Brokers and Partners'
    WHEN (apa.PARTNER_NAME IS NULL AND cd.partner_attribution IN ('non-partner')) THEN 'Direct'
    ELSE 'Other Partners'
END AS TERMUNITS
	
	-- , CASE 
	-- 		WHEN lower(cd.lt_source) = 'intuit' THEN 'Intuit'
	-- 		WHEN lower(apa.partner_name) IN ('intuit') THEN 'Intuit'
	-- 		WHEN apa.partner_name IN ('Moneyjet','Small Business Loans','National Funding','QuickBridge') THEN 'National Funding Super'
	-- 	    WHEN apa.partner_name = 'Businessloans' THEN 'BusinessLoans'
	-- 	    WHEN apa.partner_name = 'SoFi' THEN 'Sofi'
	-- 		WHEN apa.partner_name IN ('Jobber','Tailor Brands','Nerdwallet','Fundera','Become','Zoho','Clio','PTN','Lendingtree','Stripe','Synchrony','Arc') THEN 'Other Partners'
	-- 	    WHEN apa.partner_name IS NOT NULL THEN apa.partner_name
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('affiliates','marketplace','bd partners','partners')) THEN 'Terminated Partners'
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL = 'brokers') THEN 'Terminated Brokers'
	-- 		WHEN (apa.PARTNER_NAME IS NULL AND cd.LT_ACQUISITION_CHANNEL in ('direct mail','direct paid','organic', 'content','fundboxpay')) then 'Direct'
	-- 		ELSE 'Other' 
	-- 	END AS partner

        
    , CASE 
            WHEN lower(cd.partner_attribution) IN ('intuit') THEN 'Intuit'
            WHEN lower(cd.partner_attribution) IN ('lendio+1260') THEN 'Lendio'
            WHEN lower(cd.partner_attribution) IN ('1west') THEN '1West'
            WHEN lower(cd.partner_attribution) IN ('nav','nav+1250') THEN 'Nav'
            WHEN lower(cd.partner_attribution) IN ('bluevine') THEN 'Bluevine'
            WHEN lower(cd.partner_attribution) IN ('sofi') THEN 'SoFi'
            WHEN lower(cd.partner_attribution) IN ('forbes+advisors+4205') THEN 'Forbes Advisors'
            WHEN lower(cd.partner_attribution) IN ('become') THEN 'Become'
            WHEN lower(cd.partner_attribution) IN ('freshbooks') THEN 'Freshbooks'
            WHEN lower(cd.partner_attribution) IN ('wave', 'wave 4242') THEN 'Wave'
            WHEN lower(cd.partner_attribution) IN ('clio') THEN 'Clio'
            WHEN lower(cd.partner_attribution) IN ('zoho', 'zoho 1330') THEN 'Zoho'
            WHEN lower(cd.partner_attribution) IN ('housecall pro 2897') THEN 'Housecall Pro'
			WHEN cd.registration_campaign_source IN ('Franpos') THEN 'Franpos'
			WHEN lower(apa.partner_name) IN ('intuit') THEN 'Intuit'
            WHEN lower(apa.partner_name) IN ('national funding','small business loans') THEN 'National Funding'
            WHEN lower(apa.partner_name) IN ('invoicesimple','joist') THEN 'EverCommerce'
            WHEN apa.partner_name IN ('Moneyjet','QuickBridge','Anansii','Become','Bluevine','Businessloans','Clio','Cantaloupe', 'Creoate','Forbes Advisors','Freshbooks','Guesty','Housecall Pro','Jobber','Lendio','Nav','SoFi','TurboDash','ZenBusiness','Zoho', 'Credibly','1West','Viably','Wave','EverCommerce','Franpos','Autobooks','Cardiff') THEN apa.partner_name
			WHEN (apa.PARTNER_NAME IS NULL AND lower(cd.registration_campaign_id) in (
            '1800bizfund llc',
            '1st shield inc.',
            '4 pillar consulting llc',
            'abel commercial funding',
            'abm financial group',
            'accel business capital, inc.',
            'ad practitioners, llc',
            'alignable',
            'alignable_community_group',
            'aweber',
            'baker solutions inc',
            'best company',
            'best+company+1502',
            'better impressions',
            'betterimpression',
            'bridge consolidation',
            'bronxlyn enterprises inc',
            'business capitall llc',
            'businessloans.com direct',
            'capfront',
            'caredata llc',
            'central pacific bank',
            'centralpacificbank',
            'cheddar',
            'clear link llc',
            'creative business development, llc',
            'credit master ny inc',
            'credit suite',
            'dmr consulting group inc dba us fund source',
            'dotdash',
            'factor finders, llc',
            'finder',
            'first rate business services',
            'getty advance ii llc',
            'govdocfiling',
            'highland hill capital, llc',
            'inqmatic',
            'marc waring ventures llc',
            'marc+waring+ventures+llc+1202',
            'merchant crm inc',
            'merchant-maverick',
            'merchantmaverick',
            'moonshot',
            'mpf inc.',
            'natural intelligence',
            'onpoint solutions',
            'peach state solutions',
            'prosperous future consulting',
            'provider network direct',
            'ranked media, inc. dba merchant maverick',
            'roadsync',
            'rsn consultants inc',
            'score capital',
            'skip',
            'skip+4088',
            'smb compass',
            'spark llc',
            'storz power',
            'supermoney',
            'therapynotes',
            'walgatti, llc')) THEN 'Terminated Partners'
			WHEN (apa.PARTNER_NAME IS NULL AND lower(cd.registration_campaign_id) in (
            'advance funds network',
            'american capital express',
            'aj gaglio corp/ dba: all access financial',
            'algoseller ltd. dba lending express',
            'become (lending express)',
            'become-ec',
            'become-lp',
            'ally merchant services llc dba all merchant funding',
            'american financial partners',
            'american merchant financial services, llc',
            'amerifi capital group llc',
            'aquinas capital funding',
            'arg business loans llc',
            'big think capital',
            'brickell capital finance',
            'broadway advance llc',
            'business funding 4 you',
            'byzloan corp',
            'cap gap solutions',
            'capfront',
            'capital for business',
            'capital infusion llc',
            'capital soluxions',
            'capital source group llc',
            'cardinal equity group',
            'circadian funding',
            'common funding',
            'consolidated funding, inc.',
            'core business capital corp',
            'crestmont capital llc',
            'crown funding source, llc',
            'dealstruck capital, llc',
            'deer capital usa dba coast to coast funding',
            'direct capital source, inc.',
            'emerald hills capital management llc',
            'factor finders, llc',
            'finance factory',
            'fortisfi',
            'forwardline financial',
            'general merchant funding',
            'global business source - fyncap',
            'go cap advance, inc.',
            'gold capital fund',
            'green door funding llc',
            'halcyon capital',
            'high top funding',
            'highland hill capital, llc',
            'hodler capital group',
            'ifunddaily llc',
            'imperial advance, llc',
            'instant capital',
            'jaydee ventures llc',
            'jct group llc',
            'lend on capital',
            'lendflow, inc.',
            'lending tree',
            'lendver, llc',
            'lendzi',
            'liberty capital solutions, inc.',
            'main street finance group, llc',
            'maverick lending',
            'merchant source inc',
            'momentum business capital',
            'money man 4 business',
            'national business capital & services',
            'online capital',
            'orange financial llc',
            'painted horse financial, llc',
            'platform funding llc',
            'platinum advances llc',
            'premium merchant funding 18, llc',
            'premium merchant funding team 1',
            'premium merchant funding team 3',
            'premium merchant funding- team nacho',
            'quick fast capital',
            'quick funding solutions llc',
            'rapid advance',
            'redwood growth',
            'reil capital llc',
            'rok fi llc',
            'score capital',
            'secure capital solutions',
            'sellyoumoney, llc',
            'simply funded',
            'small biz lender',
            'smartbiz',
            'south end capital',
            'south end capital corporation',
            'south shore funding',
            'strategic capital',
            'strategic funding kapitus',
            'tillful',
            'trinity finance',
            'united capital source inc',
            'upfront capital inc',
            'wcp financial llc',
            'we fund capital, llc',
            'westwood funding solutions',
            'white bridge funding group llc',
            'yarrow financial')) THEN 'Terminated Brokers'
			WHEN (apa.PARTNER_NAME IS NULL AND cd.partner_attribution IN ('non-partner')) THEN 'Direct'
			ELSE COALESCE(apa.partner_name, 'Other')
		END AS partner
	
	, CASE 
			WHEN apa.partner_name = 'Moneyjet' THEN 'Moneyjet'
			-- WHEN apa.partner_name = 'Small Business Loans' THEN 'Small Business Loans API'
			-- WHEN apa.partner_name = 'National Funding' THEN 'Non-API'
            WHEN apa.partner_name IN ('Small Business Loans') THEN 'National Funding (Small Business Loans API)'
            WHEN apa.partner_name IN ('National Funding') THEN 'National Funding (Non-API)'
			WHEN apa.partner_name = 'QuickBridge' THEN 'QuickBridge'
			ELSE 'Not National Funding'
		END AS national_funding_flow


    , CASE 
			
            WHEN apa.partner_name IN ('InvoiceSimple') THEN 'InvoiceSimple'
            WHEN apa.partner_name IN ('Joist') THEN 'Joist'
			ELSE 'Not Evercommerce'
		END AS evercommerce_flow
	
	, CASE 
			WHEN partner <> 'Lendio' THEN 'not lendio'
			WHEN lead.PARTNER_LOAN_PRODUCT_ID = 372 THEN 'Direct Sales'
			WHEN lead.PARTNER_LOAN_PRODUCT_ID = 374 THEN 'Lendio Embedded'
			WHEN lead.partner_source IN ('lendio-embedded','lendio-embedded-growth','lendio-embedded-core') THEN 'Lendio Embedded'
			ELSE 'Non-Sales'
		END AS lendio_flow
	
	
	, CASE
			WHEN partner <> 'Intuit' THEN 'not intuit'
			WHEN cd.REGISTRATION_CAMPAIGN_NAME IS NULL THEN 'Other SSO'
			WHEN LEFT(lower(cd.REGISTRATION_CAMPAIGN_NAME),3) = 'dr_' THEN 'Email Campaign'
			WHEN cd.REGISTRATION_CAMPAIGN_NAME = 'intuit_appcenter' THEN 'AppCenter'
			WHEN SBF.SUB_CHANNEL = 'Marketplace' THEN 'Marketplace'
			WHEN SBF.SUB_CHANNEL = 'Direct Mail' THEN 'Direct Mail'
			ELSE 'Other SSO'
		END AS intuit_flow
	
	
	, CASE 
			WHEN partner <> 'Nav' THEN 'not nav'
			WHEN lower(cd.PARTNER_ATTRIBUTION) = 'nav expansion test 4245'	THEN 'Nav Expansion Test'
			WHEN lower(lead.partner_name) = 'nav'					THEN 'Nav Pre-approval'  
			WHEN RIGHT(LEFT(psi.partner_sub_id_1,6),1) = '0' 		THEN 'Logged In'
			WHEN RIGHT(LEFT(psi.partner_sub_id_1,6),1) = '1'		THEN 'Logged Out'
			WHEN RIGHT(LEFT(psi.partner_sub_id_1,6),1) = '2' 		THEN 'CTA'
			WHEN RIGHT(LEFT(psi.partner_sub_id_1,6),1) = '3' 		THEN 'Mobile'
			WHEN RIGHT(LEFT(psi.partner_sub_id_1,6),1) = '4' 		THEN 'Mobile'
			ELSE 'Other'
		END AS nav_flow
	
	
	, CASE 
			WHEN cd.FICO_ONBOARDING >=700 AND (greatest(ifnull(cd.FIRST_ACCOUNT_SIZE_ACCOUNTING_SOFTWARE,0),ifnull(cd.FIRST_ACCOUNT_SIZE_FI,0))*12)>=500000 THEN 'A' 
			WHEN cd.FICO_ONBOARDING >=650 AND (greatest(ifnull(cd.FIRST_ACCOUNT_SIZE_ACCOUNTING_SOFTWARE,0),ifnull(cd.FIRST_ACCOUNT_SIZE_FI,0))*12)>=250000 THEN 'B'
			WHEN cd.FICO_ONBOARDING >=600 AND (greatest(ifnull(cd.FIRST_ACCOUNT_SIZE_ACCOUNTING_SOFTWARE,0),ifnull(cd.FIRST_ACCOUNT_SIZE_FI,0))*12)>=100000 THEN 'C'
			WHEN cd.FICO_ONBOARDING >=600 AND (greatest(ifnull(cd.FIRST_ACCOUNT_SIZE_ACCOUNTING_SOFTWARE,0),ifnull(cd.FIRST_ACCOUNT_SIZE_FI,0))*12)>= 30000 THEN 'D'
			ELSE 'F' 
		END AS tier
	
	, IFNULL(cd.sub_product, 'No Selection') sub_product
    
    
    --- first model output (if not approved)    
    , IFNULL(CASE 
        	WHEN cd.first_model_name NOT ILIKE '%DAC%' THEN cd.first_aligned_bucket
        	WHEN cd.first_model_name ILIKE '%DAC_V4_P0%' THEN P0_cdc_scoring.bucket
            WHEN cd.first_model_name ILIKE '%DAC_V4_DD%' THEN DD_cdc_scoring.bucket
            WHEN cd.first_model_name ILIKE '%DAC_V3%' THEN DD_CDC_SCORING.BUCKET
        END, 0) AS ob_risk_bucket_first

    , CASE 
        	WHEN ob_risk_bucket_first BETWEEN 1 AND 4 THEN 'OB: 1-4'
        	WHEN ob_risk_bucket_first BETWEEN 5 AND 7 THEN 'OB: 5-7'
        	WHEN ob_risk_bucket_first BETWEEN 8 AND 10 THEN 'OB: 8-10'
        	WHEN ob_risk_bucket_first BETWEEN 11 AND 12 THEN 'OB: 11-12'
        	WHEN ob_risk_bucket_first >= 13 THEN 'OB: 13+'
        	ELSE 'No Bucket' 
        END ob_bucket_group_first

	-- Actual OB (approved model output)
	, Coalesce(cd.First_approved_aligned_bucket, cd.Current_aligned_bucket) as ob_bucket_actual
	, CASE 
			WHEN ob_bucket_actual BETWEEN 1 AND 4 THEN 'OB: 1-4'
			WHEN ob_bucket_actual BETWEEN 5 AND 7 THEN 'OB: 5-7'
			WHEN ob_bucket_actual BETWEEN 8 AND 10 THEN 'OB: 8-10'
			WHEN ob_bucket_actual BETWEEN 11 AND 12 THEN 'OB: 11-12'
			WHEN ob_bucket_actual >= 13 THEN 'OB: 13+'
			ELSE 'No Bucket' 
		END ob_bucket_group_actual
	
/*	-- DAC v7 retroscored OB scores
	, DV7.DALV7_BUCKET as ob_bucket_retro
	, CASE 
			WHEN ob_bucket_retro BETWEEN 1 AND 4 THEN 'OB: 1-4'
			WHEN ob_bucket_retro BETWEEN 5 AND 7 THEN 'OB: 5-7'
			WHEN ob_bucket_retro BETWEEN 8 AND 10 THEN 'OB: 8-10'
			WHEN ob_bucket_retro BETWEEN 11 AND 12 THEN 'OB: 11-12'
			WHEN ob_bucket_retro >= 13 THEN 'OB: 13+'
			ELSE 'No Bucket' 
		END ob_bucket_group_retro */
	-- DAC v7 retroscored OB scores
	, EGL.EAGLET_BUCKET as ob_bucket_retro
	, CASE 
			WHEN ob_bucket_retro BETWEEN 1 AND 4 THEN 'OB: 1-4'
			WHEN ob_bucket_retro BETWEEN 5 AND 7 THEN 'OB: 5-7'
			WHEN ob_bucket_retro BETWEEN 8 AND 10 THEN 'OB: 8-10'
			WHEN ob_bucket_retro BETWEEN 11 AND 12 THEN 'OB: 11-12'
			WHEN ob_bucket_retro >= 13 THEN 'OB: 13+'
			ELSE 'No Bucket' 
		END ob_bucket_group_retro
	
	
	-- ongoing model output
	, dacd.risk_credit_review_aligned_bucket og_bucket_actual
	, dacd.MODEL_CREDIT_SCORE_JSON:MODEL_RUN_START_TIME::TIMESTAMP AS MODEL_RUN_START_TIME
	, dacd_prev.risk_credit_review_model_score og_risk_score_old_prev
	, dacd_prev.risk_credit_review_aligned_bucket og_risk_bucket_old_prev
	, CASE 
			WHEN og_bucket_actual BETWEEN 1 AND 4 THEN 'OG: 1-4'
			WHEN og_bucket_actual BETWEEN 5 AND 7 THEN 'OG: 5-7'
			WHEN og_bucket_actual BETWEEN 8 AND 10 THEN 'OG: 8-10'
			WHEN og_bucket_actual BETWEEN 11 AND 12 THEN 'OG: 11-12'
			WHEN og_bucket_actual >= 13 THEN 'OG: 13+'
			ELSE 'No Bucket' 
		END og_bucket_group_actual
	
	, CASE 
	WHEN og_risk_bucket_old_prev BETWEEN 1 AND 4 THEN 'OG: 1-4'
	WHEN og_risk_bucket_old_prev BETWEEN 5 AND 7 THEN 'OG: 5-7'
	WHEN og_risk_bucket_old_prev BETWEEN 8 AND 10 THEN 'OG: 8-10'
	WHEN og_risk_bucket_old_prev BETWEEN 11 AND 12 THEN 'OG: 11-12'
	WHEN og_risk_bucket_old_prev >= 13 THEN 'OG: 13+'
	ELSE 'No Bucket' 
	END og_bucket_group_old_prev
	
	-- OG2.2 retroscored
	, OG2.OG_BUCKET AS og_bucket_retro
	, CASE 
			WHEN og_bucket_retro BETWEEN 1 AND 4 THEN 'OG: 1-4'
			WHEN og_bucket_retro BETWEEN 5 AND 7 THEN 'OG: 5-7'
			WHEN og_bucket_retro BETWEEN 8 AND 10 THEN 'OG: 8-10'
			WHEN og_bucket_retro BETWEEN 11 AND 12 THEN 'OG: 11-12'
			WHEN og_bucket_retro >= 13 THEN 'OG: 13+'
			ELSE 'No Bucket' 
		END og_bucket_group_retro
	
	, CASE 
		    WHEN (dacd.edate IS NULL AND cd.registration_time::date < '2024-09-08') OR (dacd.edate IS NOT NULL AND dacd.edate < '2024-09-08') THEN og_bucket_group_retro
		    ELSE og_bucket_group_actual
		END AS og_bucket_group
	, CASE 
		    WHEN (dacd.edate IS NULL AND cd.registration_time::date < '2024-09-08') OR (dacd.edate IS NOT NULL AND dacd.edate < '2024-09-08') THEN og_bucket_retro
		    ELSE og_bucket_actual
		END AS og_bucket
	,CASE 
    WHEN (dacd.edate IS NULL AND cd.registration_time::date < '2024-09-08') 
         OR (dacd.edate IS NOT NULL AND dacd.edate < '2024-09-08') THEN og_bucket_group_retro
    ELSE og_bucket_group_old_prev
	END AS og_bucket_group_prev

    , dacd.is_test_user AS is_test
    , case
		WHEN left(cd.industry_naics_code,2)=23 THEN 'Construction'
		WHEN left(cd.industry_naics_code,2) in (4,42,44,45) THEN 'Retail & Wholesale Trade'
		WHEN left(cd.industry_naics_code,2) in (48,49) THEN 'Transportation & Warehousing'
		WHEN left(cd.industry_naics_code,2)=56 THEN 'ASWR'
		WHEN left(cd.industry_naics_code,2) in (53,72) THEN 'Real Estate, Rental & Hospitality'
		WHEN left(cd.industry_naics_code,1)=5 THEN 'Professional Services'
		else 'Others'
		END industry_type
	
	/*,CASE WHEN cd.first_account_size_accounting_software = 0 THEN NULL ELSE cd.first_account_size_accounting_software END AS first_account_size_accounting_software_INT
	,CASE WHEN cd.first_account_size_fi = 0 THEN NULL ELSE cd.first_account_size_fi END AS first_account_size_fi_INT
	, COALESCE(first_account_size_accounting_software_INT, first_account_size_fi_INT) * 12 customer_annual_revenue*/

	, revenue.biz_rev * 12 customer_annual_revenue
	
	, CASE WHEN customer_annual_revenue>=0 and customer_annual_revenue<=500000 THEN '$0 - $500K'
           WHEN customer_annual_revenue>500000 and customer_annual_revenue<=1500000 THEN '$500K - $1.5M'
           WHEN customer_annual_revenue>1500000 THEN '> $1.5M'
--           WHEN customer_annual_revenue>1000000 THEN '> $1M'
           ELSE 'Other/No Data'
           END AS customer_annual_revenue_group
	, CASE 
			WHEN ICL = 0 THEN 'Not Approved'
			WHEN ICL < 5000 THEN '$0 - $5K'
		   WHEN ICL < 15000 THEN '$5K - $15K'
		   WHEN ICL < 30000 THEN '$15K - $30K'
		   WHEN ICL < 60000 THEN '$30K - $60K'
		   WHEN ICL < 100000 THEN '$60K - $100K'
		   ELSE '>$100K'
		END AS icl_bucket
    , dacd.CREDIT_SCORE_JSON:"VantageScore 4.0"."score" AS VANTAGE4
	, COALESCE (dacd.risk_review_fico_score, dacd.fico_onboarding) AS fico
	
	
FROM indus."PUBLIC".customers_data_indus cd

LEFT JOIN (
    SELECT *
    FROM ANALYTICS.CREDIT.eg_adjusted_payout_attribution_v2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fbbid ORDER BY partner_name) = 1
) apa
ON cd.fbbid = apa.fbbid


LEFT JOIN INDUS.PUBLIC.INTUIT_SUBFLOW SBF
ON CD.FBBID = SBF.FBBID

LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd
ON dacd.fbbid = cd.fbbid

LEFT JOIN indus.public.revenue_new_ar revenue
on dacd.fbbid = revenue.fbbid
and dacd.edate = revenue.edate


----------
LEFT JOIN (
SELECT identifier_id
, bucket
, score					
FROM (
SELECT *
, CASE 	WHEN score <= 0.0585 THEN 6	WHEN score <= 0.0907 THEN 7	WHEN score <= 0.1026 THEN 8	WHEN score <= 0.1219 THEN 9 WHEN score <= 0.1338 THEN 11 WHEN score <= 0.145 THEN 12 WHEN score <= 0.155 THEN 14 WHEN score <= 0.1675 THEN 15 WHEN score <= 1.000 THEN 16 ELSE NULL 
END AS bucket
, row_number() OVER (PARTITION BY IDentifier_id ORDER BY id DESC) AS date_order					
FROM indus."PUBLIC".scoring_model_scores_indus					
WHERE model_name IN ('DirectDrawOnboardingFIConnectFicoBizaggsMLEEnsemblePredictorV9')
) WHERE date_order = 1
) dd_cdc_scoring
ON cd.fbbid = dd_cdc_scoring.identifier_id


LEFT JOIN (
SELECT identifier_id
, bucket
, score					
FROM (
SELECT *
,  CASE WHEN score <= 0.025 THEN 2 WHEN score <= 0.04 THEN 3 WHEN score <= 0.055 THEN 4 WHEN score <= 0.07 THEN 6 WHEN score <= 0.080 THEN 7 WHEN score <= 0.095 THEN 9 WHEN score <= 0.105 THEN 11 WHEN score <= 0.120 THEN 12 WHEN score <= 0.130 THEN 13	 WHEN score <= 0.140 THEN 14 WHEN score <= 0.170 THEN 15 WHEN score <= 1.000 THEN 16 ELSE NULL
END AS bucket
, row_number() OVER (PARTITION BY IDentifier_id ORDER BY id DESC) AS date_order					
FROM indus."PUBLIC".scoring_model_scores_indus				
WHERE model_name IN ('InvoicingOnboardingFicoExperianFIConnectMLEEnsemblePredictorV12')
) WHERE date_order = 1
) P0_cdc_scoring
ON cd.fbbid = P0_cdc_scoring.identifier_id

----------

LEFT JOIN (
SELECT DISTINCT hoa.fbbid, psi.PARTNER_SUB_ID_1, psi.PARTNER_SUB_ID_2, psi.PARTNER_SUB_ID_3, psi.PARTNER_SUB_ID_4, psi.PARTNER_SUB_ID_5 FROM CDC_V2.OUTBOUND_REPORTING.HASOFFERS_ATTRIBUTION hoa
LEFT JOIN CDC_V2.OUTBOUND_REPORTING.PARTNER_SUB_IDS psi ON hoa.id = psi.hasoffers_attribution_id 
WHERE (psi.PARTNER_SUB_ID_1 IS NOT NULL OR psi.PARTNER_SUB_ID_2 IS NOT NULL OR psi.PARTNER_SUB_ID_3 IS NOT NULL OR psi.PARTNER_SUB_ID_4 IS NOT NULL OR psi.PARTNER_SUB_ID_5 IS NOT NULL)
) psi 
ON psi.fbbid = cd.fbbid

LEFT JOIN 
(SELECT DISTINCT fbbid, partner_name, PARTNER_LOAN_PRODUCT_ID, partner_source
FROM ANALYTICS.CREDIT.eg_leads_data) lead
ON lead.fbbid = cd.fbbid

/*LEFT JOIN ANALYTICS.CREDIT.OB_UNIFIED_DALV7_ALL_SCORE_FEATURES DV7
ON cd.fbbid = DV7.fbbid */
--ADDED EAGLET
LEFT JOIN ANALYTICS.CREDIT.EAGLET_KEY_METRICS_SCORES EGL
ON CD.FBBID = EGL.FBBID

LEFT JOIN ANALYTICS.CREDIT.OG_MODEL_SCORES_RETROSCORED_V2_2 OG2
ON dacd.fbbid = OG2.fbbid
AND dacd.edate = OG2.edate

LEFT JOIN BI.PUBLIC.DAILY_APPROVED_CUSTOMERS_DATA dacd_prev
ON dacd.fbbid = dacd_prev.fbbid
AND dacd_prev.edate = DATEADD(DAY, -1, dacd.edate)
);

