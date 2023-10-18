
SELECT 
    animal_type AS "Animal Type",
    COUNT(DISTINCT animal_id) AS "Number of Animals"
FROM
    animal_dim_info
GROUP BY
    animal_type ;
    
   
SELECT 
    COUNT(DISTINCT animal_id) AS "Number of Animals with Multiple Outcomes"
FROM
    (
        SELECT 
            animal_id,
            COUNT(*) AS num_outcomes
        FROM
            animal_dim_info
        GROUP BY
            animal_id
        HAVING
            COUNT(*) > 1
    ) AS subquery;
    
   
   
SELECT MONTHH, COUNT(*) AS TOTAL_OUTCOMES
FROM outcome_fact
JOIN timing_dim ON outcome_fact.TIME_DIM_KEY = timing_dim.TIME_DIM_KEY
GROUP BY MONTHH
ORDER BY TOTAL_OUTCOMES DESC
LIMIT 5;


WITH CatAges AS (
    SELECT 
        ANIMAL_ID,
        ANIMAL_DIM_KEY,
        CASE 
            WHEN AGE(DOB) < INTERVAL '1 year' THEN 'Kitten'
            WHEN AGE(DOB) BETWEEN INTERVAL '1 year' AND INTERVAL '10 years' THEN 'Adult'
            ELSE 'Senior'
        END AS CatAge
    FROM animal_dim_info
    WHERE ANIMAL_TYPE = 'Cat'
)
SELECT 
    CatAge,
    COUNT(DISTINCT CatAges.ANIMAL_ID) AS TOTAL_CATS,
    ROUND(100.0 * COUNT(DISTINCT CatAges.ANIMAL_ID) / SUM(COUNT(DISTINCT CatAges.ANIMAL_ID)) OVER(), 2) AS PERCENTAGE
FROM CatAges
JOIN outcome_fact ON CatAges.ANIMAL_DIM_KEY = outcome_fact.ANIMAL_DIM_KEY
GROUP BY CatAge;



SELECT 
    TIMESTMP, 
    COUNT(*) OVER (ORDER BY TIMESTMP ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUMULATIVE_TOTAL
FROM outcome_fact
JOIN animal_dim_info ON outcome_fact.ANIMAL_DIM_KEY = animal_dim_info.ANIMAL_DIM_KEY
GROUP BY TIMESTMP
ORDER BY TIMESTMP;