WITH SALES_HOURLY_BASE AS (
    SELECT
        T1.*,
        entity_period_id,   
        EXTRACT(YEAR FROM T1.POS_DATETIME) AS YEAR,
        EXTRACT(MONTH FROM T1.POS_DATETIME) AS MONTH,
        EXTRACT(DAY FROM T1.POS_DATETIME) AS DAY,
        EXTRACT(HOUR FROM T1.POS_DATETIME) AS HOUR
    FROM(
        SELECT
            e.entity_id,
            e.entity_name,
            e.concept_id,
            e.concept_name,
            e.concept,
            s."POS_DATE" as pos_date,
            s."POS_DATE"::date + (INTERVAL '15 MINUTES' * s."TIME_SLOT") AS pos_datetime,
            s."SALES_QUANTITY" as sales_quantity,
            s."SALES_VALUE" as sales_value
        FROM {{source('xrg', 'sales_hourly')}} as s
        LEFT JOIN {{source('xrg', 'entity_config')}} e
            ON CAST(SUBSTRING(s."LOCATION_CODE"::varchar, 1, LENGTH(s."LOCATION_CODE"::varchar)-2) AS INT)  = CAST(e.source_entity_id AS INT)
        WHERE s.is_deleted = FALSE
    ) T1
    LEFT JOIN {{source('xrg', 'entity_period_config')}} epc
        ON T1.entity_id = epc.entity_id
        AND EXTRACT(YEAR FROM T1.pos_datetime) = epc.year
        AND EXTRACT(MONTH FROM T1.pos_datetime) = epc.month
),

DAY_OF_WEEK AS(
    SELECT
        T1.*,
        CASE 
            WHEN day_of_week IN (1, 2, 3) THEN 'Monday - Wednesday'
            WHEN day_of_week IN (4, 5, 6, 7) THEN 'Thursday - Sunday'
        END AS day_of_week_bracket
    FROM(
    SELECT
        *,
        CASE
            WHEN EXTRACT(DOW FROM pos_datetime) = 0 THEN 7
            ELSE EXTRACT(DOW FROM pos_datetime)
        END AS day_of_week
    FROM SALES_HOURLY_BASE
    ) T1
),

SALES_HOUR_BRACKET AS(
    SELECT
        *,
        CASE
            WHEN hour > 12 THEN CONCAT(CAST(hour -12 AS VARCHAR), 'pm - ', CAST(hour - 11 AS VARCHAR), 'pm')
            WHEN hour = 12 THEN CONCAT(CAST(hour AS VARCHAR), 'pm - ', CAST(hour - 11 AS VARCHAR), 'pm')
            WHEN hour = 11 THEN CONCAT(CAST(hour AS VARCHAR), 'am - ', CAST(hour + 1 AS VARCHAR), 'pm')
            WHEN hour = 0 THEN CONCAT(CAST(hour + 12 AS VARCHAR), 'am - ', CAST(hour + 1 AS VARCHAR), 'am')
            ELSE CONCAT(CAST(hour AS VARCHAR), 'am - ', CAST(hour + 1 AS VARCHAR), 'am')
        END AS hour_bracket
    FROM DAY_OF_WEEK
)

SELECT
    entity_period_id,
    entity_id,
    entity_name,
    concept_id,
    concept_name,
    concept,
    pos_date,
    year,
    month,
    day,
    hour,
    day_of_week,
    day_of_week_bracket,
    hour_bracket,
    SUM(sales_quantity) AS sales_quantity,
    SUM(sales_value) AS sales_value
FROM SALES_HOUR_BRACKET
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14