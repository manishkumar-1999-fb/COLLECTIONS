--MODEL_NAME
CREATE OR REPLACE VIEW ANALYTICS.CREDIT.EAGLET_KEY_METRICS_SCORES AS (


WITH SIMTABLE AS (

    SELECT COALESCE(B.FBBID, A.FBBID) AS FBBID, COALESCE(B.SCORE, A.SCORE) AS SCORE

    FROM(
    SELECT IDENTIFIER_ID AS FBBID, SCORE
    FROM DATA_SCIENCE.RISK_FIRST_DECISION_SIMULATION.MODEL_SCORES
    WHERE MODEL_NAME = 'smm-eaglet-SV-dac-v9-9-v3'
    QUALIFY row_number() OVER (PARTITION BY IDENTIFIER_ID ORDER BY CREATED_TIME DESC) = 1)A
    
    FULL OUTER JOIN(
    SELECT IDENTIFIER_ID AS FBBID, SCORE
    FROM DATA_SCIENCE.RISK_FIRST_APPROVAL_SIMULATION.MODEL_SCORES
    WHERE MODEL_NAME = 'smm-eaglet-SV-dac-v9-9-v3'
    QUALIFY row_number() OVER (PARTITION BY IDENTIFIER_ID ORDER BY CREATED_TIME DESC) = 1
    )B
    ON A.FBBID= B.FBBID
)

, PRODTABLE AS(
    SELECT IDENTIFIER_ID AS FBBID, SCORE
    FROM cdc_v2.scoring.scoring_model_scores
    WHERE MODEL_NAME = 'smm-eaglet-SV-dac-v9-9-v3'
    QUALIFY row_number() OVER (PARTITION BY IDENTIFIER_ID ORDER BY CREATED_TIME DESC) = 1
)
select coalesce(PRD.fbbid,SIM.FBBID) AS FBBID,
coalesce(PRD.score,SIM.score) as eaglet_score
,(CASE  WHEN eaglet_score >= 0 AND eaglet_score <= 0.010 THEN 1
        WHEN eaglet_score > 0.010 AND eaglet_score <= 0.020 THEN 2
        WHEN eaglet_score > 0.020 AND eaglet_score <= 0.030 THEN 3
        WHEN eaglet_score > 0.030 AND eaglet_score <= 0.040 THEN 4
        WHEN eaglet_score > 0.040 AND eaglet_score <= 0.050 THEN 5
        WHEN eaglet_score > 0.050 AND eaglet_score <= 0.060 THEN 6
        WHEN eaglet_score > 0.060 AND eaglet_score <= 0.070 THEN 7
        WHEN eaglet_score > 0.070 AND eaglet_score <= 0.080 THEN 8
        WHEN eaglet_score > 0.080 AND eaglet_score <= 0.090 THEN 9
        WHEN eaglet_score > 0.090 AND eaglet_score <= 0.100 THEN 10
        WHEN eaglet_score > 0.100 AND eaglet_score <= 0.125 THEN 11
        WHEN eaglet_score > 0.125 AND eaglet_score <= 0.150 THEN 12
        WHEN eaglet_score > 0.150 AND eaglet_score <= 0.175 THEN 13
        WHEN eaglet_score > 0.175 AND eaglet_score <= 0.200 THEN 14
        WHEN eaglet_score > 0.200 AND eaglet_score <= 1     THEN 15
        ELSE NULL
        END) AS EAGLET_BUCKET

from SIMTABLE SIM
left join PRODTABLE PRD 
on PRD.fbbid = SIM.fbbid

);