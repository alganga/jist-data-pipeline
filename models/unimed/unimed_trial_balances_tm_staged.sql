 WITH DistinctGls AS (
    SELECT DISTINCT
        gl_code_id
    FROM {{ source('unimed', 'trial_balances_tm') }} 
),

AllDatesAndGLs AS (
    SELECT 
        gl_code_id,
        ref_date
    FROM DistinctGls
    CROSS JOIN (SELECT DISTINCT ref_date FROM {{ source('unimed', 'trial_balances_tm') }} ) AS GLs
),

BaseData AS (
    SELECT 
        A.gl_code_id,
        A.ref_date,
        C.cat_id,
        C.cat_name,
        C.subcat_id,
        C.subcat_name,
        C.group_id,
        C.group_name,
        C.gl_code_name,
        C.debit,
        C.credit,
        C.country_number,
        C.country_name
    FROM {{ source('unimed', 'trial_balances_tm') }} C
    RIGHT JOIN AllDatesAndGLs A
    ON C.gl_code_id = A.gl_code_id AND C.ref_date = A.ref_date
),

JoinData AS (
    SELECT 
        T1.gl_code_id,
        T1.ref_date,
        T2.ref_date AS ref_date_2,
        T1.cat_id,
        T1.cat_name,
        T1.subcat_id,
        T1.subcat_name,
        T1.group_id,
        T1.group_name,
        T1.gl_code_name,
        T1.debit::FLOAT,
        T1.credit::FLOAT,
        T1.country_number,
        T1.country_name,
        T2.cat_id AS cat_id_2,
        T2.cat_name AS cat_name_2,
        T2.subcat_id AS subcat_id_2,
        T2.subcat_name AS subcat_name_2,
        T2.group_id AS group_id_2,
        T2.group_name AS group_name_2,
        T2.gl_code_name AS gl_code_name_2,
        T2.debit::FLOAT AS debit_2,
        T2.credit::FLOAT AS credit_2,
        T2.country_number AS country_number_2,
        T2.country_name AS country_name_2,
        MAX(T2.ref_date) OVER (PARTITION BY T1.gl_code_id, T1.ref_date) AS max_ref_date
    FROM BaseData T1
    JOIN {{ source('unimed', 'trial_balances_tm') }} T2
    ON T1.gl_code_id = T2.gl_code_id
    AND T1.ref_date >= T2.ref_date
),
Final AS (
    SELECT
        COALESCE(T3.cat_id, T3.cat_id_2) AS cat_id,
        COALESCE(T3.cat_name, T3.cat_name_2) AS cat_name,
        COALESCE(T3.subcat_id, T3.subcat_id_2) AS subcat_id,
        COALESCE(T3.subcat_name, T3.subcat_name_2) AS subcat_name,
        COALESCE(T3.group_id, T3.group_id_2) AS group_id,
        COALESCE(T3.group_name, T3.group_name_2) AS group_name,
        T3.ref_date,
        T3.gl_code_id,
        COALESCE(T3.gl_code_name, T3.gl_code_name_2) AS gl_code_name,
        COALESCE(T3.debit, 0) AS debit,
        COALESCE(T3.credit, 0) AS credit,
        COALESCE(T3.country_number, T3.country_number_2) AS country_number,
        COALESCE(T3.country_name, T3.country_name_2) AS country_name
    FROM JoinData T3
    WHERE T3.ref_date_2 = max_ref_date
)
SELECT * FROM Final
