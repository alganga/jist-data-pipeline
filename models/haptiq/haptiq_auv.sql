-- This CTE calculates the revenue base for the Pink Taco AUV model.
WITH REVENUE_BASE AS (   
    SELECT 
        period,
        concept_id,
        concept_name,
        entity_id,
        entity_name,
        category_id,
        category_name,
        metric_order,
        submetric_order,
        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
        '20' || SPLIT_PART(period, '-', 2) AS year,
        COALESCE(NULLIF(-1 * SUM(ending_balance), -0), 0) AS ending_balance
    FROM 
    	{{ref('haptiq_target_trial_balance')}}
    WHERE 
        report_id = 3000       
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

ENTITY_COUNT AS (
    SELECT
        year,
        month,
        COUNT(DISTINCT entity_id) AS entity_count
    FROM REVENUE_BASE
    GROUP BY year, month
),

AUV_CALCULATION AS (
    SELECT
        rev.year,
        rev.month,
        rev.category_name,
        SUM(rev.ending_balance) OVER (PARTITION BY rev.year ORDER BY rev.month) AS ytd_revenue,
        AVG(entity.entity_count) OVER (PARTITION BY rev.year ORDER BY rev.month) AS avg_entity_count_ytd,
        rev.ending_balance AS mtd_revenue,
        entity.entity_count AS entity_count_mtd
    FROM (
        SELECT 
            year,
            month,
            category_name,
            sum(ending_balance) as ending_balance
        FROM REVENUE_BASE
        WHERE category_name = 'Revenue'
        GROUP BY 1, 2, 3
        ) rev
    LEFT JOIN ENTITY_COUNT entity
        ON rev.year = entity.year AND rev.month = entity.month
    WHERE rev.category_name = 'Revenue'
)

--Final Select
SELECT
    year,
    month,
    category_name,
    ytd_revenue / NULLIF(avg_entity_count_ytd, 0) AS auv_ytd,
    mtd_revenue / NULLIF(entity_count_mtd, 0) AS auv_mtd
FROM AUV_CALCULATION