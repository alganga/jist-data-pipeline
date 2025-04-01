WITH Monthly_Counts AS (
    SELECT 
        concept_id,
        '20' || SPLIT_PART(period, '-', 2) AS year,
        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
        COUNT(DISTINCT entity_id) AS monthly_distinct_entity_count
    FROM 
        {{ref('xrg_target_trial_balance')}}
    WHERE 
        report_id = 3000
    GROUP BY 
        concept_id, year, month
),

Yearly_Counts AS (
    SELECT 
        concept_id,
        '20' || SPLIT_PART(period, '-', 2) AS year,
        COUNT(DISTINCT entity_id) AS yearly_distinct_entity_count
    FROM 
        {{ref('xrg_target_trial_balance')}}
    WHERE 
        report_id = 3000
    GROUP BY 
        concept_id, year
),

Auv_Calculation AS(

	SELECT
		T1.concept_id,
		T1.concept_name,
		T1.year,
		T1.month,
		T1.ytd_revenue/NULLIF(Y.yearly_distinct_entity_count, 0) AS concept_auv_ytd,
		T1.mtd_revenue/NULLIF(M.monthly_distinct_entity_count, 0) AS concept_auv_mtd
	FROM(
		SELECT
			concept_id,
			concept_name,
			'20' || SPLIT_PART(period, '-', 2) AS year,
	        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
	        COALESCE(NULLIF(-1 * SUM(ending_balance), -0), 0) - COALESCE(NULLIF(-1 * SUM(beginning_balance), -0), 0) AS mtd_revenue,
	        COALESCE(NULLIF(-1 * SUM(ending_balance), -0), 0) AS ytd_revenue
		FROM {{ref('xrg_target_trial_balance')}}
		WHERE report_id = 3000 AND category_id = 2
		GROUP BY 1, 2, 3, 4
	) T1
	LEFT JOIN Yearly_Counts Y 
		ON T1.concept_id = Y.concept_id
		AND T1.year = Y.year
	LEFT JOIN Monthly_Counts M
		ON T1.concept_id = M.concept_id
		AND T1.year = M.year
		AND T1.month = M.month
)

-- Final Select
SELECT
	epc.entity_period_id,
	auv.concept_id,
	auv.concept_auv_ytd,
	auv.concept_auv_mtd
FROM Auv_calculation auv
JOIN {{source('xrg','entity_config')}} ec
	ON auv.concept_id = ec.concept_id
JOIN {{source('xrg','period_config')}} pc
	ON auv.year::int = pc.year::int
	AND auv.month::int = pc.month::int
JOIN {{source('xrg', 'entity_period_config')}} epc
	ON ec.entity_id = epc.entity_id
	AND pc.period_id  = epc.period_id

		
	

