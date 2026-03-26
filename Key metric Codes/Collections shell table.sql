-- Create the table with Channel values
CREATE OR REPLACE TABLE indus.PUBLIC.existing_customers_collect (
    new_cust_filter VARCHAR(50)
);
-- Insert Channel values including NULL
INSERT INTO indus.PUBLIC.existing_customers_collect (new_cust_filter )
VALUES
    ('Existing Customer');
    
    
-- SELECT * FROM indus.PUBLIC.existing_customers_collect
   
   -- Create the table with OB Bucket Group values
CREATE OR replace TABLE indus.PUBLIC.OG_Bucket_Group_collect_new (
    bucket_group VARCHAR(20)
);

INSERT INTO indus.PUBLIC.OG_Bucket_Group_collect_new (bucket_group )
VALUES
    ('OG: 1-3'),
    ('OG: 4-5'),
    ('OG: 6-8'),
    ('OG: 9-10'),
    ('OG: 11+');
    
-- SELECT * FROM indus.PUBLIC.OG_Bucket_Group_collect_new 

-----------------------------------------------------------------------------------------

-- Create the table with Channel values
CREATE OR REPLACE TABLE indus.PUBLIC.new_customers_collect (
    new_cust_filter VARCHAR(50)
);
-- Insert Channel values including NULL
INSERT INTO indus.PUBLIC.new_customers_collect (new_cust_filter)
VALUES
    ('New Customer');
    
    
-- SELECT * FROM indus.PUBLIC.New_customers_collect
   
   -- Create the table with OB Bucket Group values
CREATE OR replace TABLE indus.PUBLIC.OB_Bucket_Group_collect_new (
    bucket_group VARCHAR(20)
);

INSERT INTO indus.PUBLIC.OB_Bucket_Group_collect_new (bucket_group )
VALUES
    ('OB: 1-3'),
    ('OB: 4-5'),
    ('OB: 6-8'),
    ('OB: 9-10'),
    ('OB: 11+');
    
-- SELECT * FROM indus.PUBLIC.OB_Bucket_Group_collect_new 

---------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE indus."PUBLIC".existing_og_collect_cross as 
SELECT t1.new_cust_filter,
    t2.Bucket_Group
FROM
    (SELECT DISTINCT new_cust_filter FROM indus.PUBLIC.existing_customers_collect) AS t1
CROSS JOIN
    (SELECT DISTINCT Bucket_Group FROM indus.PUBLIC.OG_Bucket_Group_collect_new) AS t2;

-- SELECT * FROM indus."PUBLIC".existing_og_collect_cross

------------------------------------------------------------------------------


CREATE OR REPLACE TABLE indus."PUBLIC".new_ob_collect_cross as 
SELECT t1.new_cust_filter,
    t2.Bucket_Group
FROM
    (SELECT DISTINCT new_cust_filter FROM indus.PUBLIC.new_customers_collect) AS t1
CROSS JOIN
    (SELECT DISTINCT Bucket_Group FROM indus.PUBLIC.OB_Bucket_Group_collect_new ) AS t2;

-- SELECT * FROM indus."PUBLIC".new_ob_collect_cross

---------------------------------------------------------------------------------

CREATE OR REPLACE TABLE indus."PUBLIC".cust_filt_bucket_grp_cross_collect AS
SELECT new_cust_filter, bucket_group
FROM indus."PUBLIC".existing_og_collect_cross
UNION
SELECT new_cust_filter, bucket_group
FROM indus."PUBLIC".new_ob_collect_cross;

-- SELECT * FROM indus."PUBLIC".cust_filt_bucket_grp_cross_collect

--------------------------------------------------------------------------------------------------

-- Create the table with Channel values
CREATE OR REPLACE TABLE indus.PUBLIC.partner_desc_table (
    Partner VARCHAR(50)
);
-- Insert Channel values including NULL
INSERT INTO indus.PUBLIC.partner_desc_table (Partner)
VALUES
    ('Intuit'),
    ('Freshbooks'),
    ('Nav'),
    ('Housecall Pro'),
    ('Forbes Advisors'),
    ('Sofi'),
    ('Lendio'),
    ('BusinessLoans'),
    ('Other Partners'),
    ('Terminated Partners'),
    ('Terminated Brokers'),
    ('Direct'),
    ('Other');
   
-- SELECT * FROM indus."PUBLIC".partner_desc_table

---------------------------------------------------------------------------------------------------

-- Create the table with dates
CREATE OR REPLACE VIEW indus.PUBLIC.collections_week_end_date AS
(WITH RECURSIVE a AS (
SELECT DATEFROMPARTS(2020, 12, 30) AS week_end
UNION ALL
SELECT DATEADD(DAY, 7, week_end)
FROM a
WHERE week_end <= current_date()
)
SELECT *
FROM a );

-- SELECT top 10*
-- FROM indus."PUBLIC".collections_week_end_date
-- ORDER BY week_end DESC 

----------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW indus.PUBLIC.COLLECTIONS_SHELL_Table_AGG_final  as
SELECT
    t1.week_end,
    t2.partner,
    t3.new_cust_filter,
    t3.bucket_group
FROM
    (SELECT DISTINCT week_end FROM indus."PUBLIC".collections_week_end_date) AS t1
CROSS JOIN
    (SELECT DISTINCT partner FROM indus."PUBLIC".partner_desc_table) AS t2
CROSS JOIN
    (SELECT DISTINCT new_cust_filter, bucket_group FROM indus."PUBLIC".cust_filt_bucket_grp_cross_collect) AS t3;
    
-- SELECT * FROM indus.PUBLIC.COLLECTIONS_SHELL_Table_AGG_final
-- WHERE WEEK_END = '2024-03-06'
-- ORDER BY 1 desc,2 desc,3 DESC 
    
    
    