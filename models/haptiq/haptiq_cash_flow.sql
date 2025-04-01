WITH metric_order AS (
    SELECT * FROM {{source('public', 'report_metric_config')}} WHERE report_id = 5000
),

ebitda AS(
    SELECT
        period,
        year::int,
        month::int,
        'Cash flow from Operating Activities' as category_name,
         m.metric_order,
        'EBITDA' as subcategory_name,
        m2.submetric_order,
        'EBITDA' as gl_name,
        CAST(NULL AS INT) as gl_number,
        SUM(actual_mtd) as mtd,
        SUM(actual_ytd) as ytd,
        SUM(actual_ttm) as ttm
    FROM {{ref('haptiq_profit_and_loss')}} pnl
    LEFT JOIN metric_order m ON 'Cash flow from Operating Activities' = m.metric_name
    LEFT JOIN metric_order m2 ON 'EBITDA' = m2.metric_name
    WHERE pnl.metric_name = 'EBITDA'
    GROUP BY period, year, month, m.metric_order, m2.submetric_order
),

cash_flow AS (
    SELECT 
        period,
        category_name,
        category_id,
        gl_name,
        gl_number,
        SUM(ending_balance) AS ending_balance,
        SUM(beginning_balance) AS beginning_balance,
        -- swap the signs of cash flow from financing
        SUM(
            CASE
                WHEN category_id = 55 THEN -1 * (ending_balance - beginning_balance)
                ELSE ending_balance - beginning_balance
            END
        ) AS mtd,
        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
        '20' || SPLIT_PART(period, '-', 2) AS year,
        metric_order,
		subcategory_name,
		submetric_order
    FROM {{ref('haptiq_target_trial_balance')}}
    WHERE report_id = 5000
    GROUP BY period, category_name, category_id, gl_name, gl_number, metric_order, subcategory_name, submetric_order
),

original_balance AS (
    SELECT
        year::int,
        month::int,
        FIRST_VALUE(sum_beginning_balance) OVER(ORDER BY year, month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS original_balance
    FROM (
        SELECT
            year,
            month,
            SUM(beginning_balance) AS sum_beginning_balance
        FROM cash_flow
        GROUP BY year, month
    ) t1
),

mtd_ytd_ttm AS(
	SELECT
		period,
		year::int,
		month::int,
		category_name,
		metric_order,
		subcategory_name,
		submetric_order,
		gl_name,
		gl_number,
		mtd AS mtd_change,
		SUM(mtd) OVER(PARTITION BY gl_number, year ORDER BY year, month) as ytd_change,
		SUM(mtd) OVER(PARTITION BY gl_number ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ttm_change
	FROM (
		SELECT
			cf1.period,
			cf1.year,
			cf1.month,
			cf1.category_name,
			cf1.metric_order,
			cf1.subcategory_name,
			cf1.submetric_order,
			cf1.gl_name,
			cf1.gl_number,
			cf1.beginning_balance,
			cf1.ending_balance,
			cf1.mtd
		FROM cash_flow cf1
	)c1
	
	UNION ALL
	
	SELECT * FROM ebitda
),

ce AS(
	SELECT
		m.period,
		m.year,
		m.month,
		--mtd beginning, change, end
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) - SUM(mtd_change) + AVG(original_balance)  AS mtd_beginning,
		SUM(mtd_change) AS mtd_change,
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) + AVG(original_balance) AS mtd_end,
		
		--ytd beginning, change, end
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) - SUM(ytd_change) + AVG(original_balance) AS ytd_beginning,
		SUM(ytd_change) AS ytd_change,
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) + AVG(original_balance) AS ytd_end,
		
		--ttm change 
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) - SUM(ttm_change) + AVG(original_balance) AS ttm_beginning,
		SUM(ttm_change) AS ttm_change,
		SUM(SUM(mtd_change)) OVER(ORDER BY m.year, m.month) + AVG(original_balance) AS ttm_end
		
	FROM mtd_ytd_ttm m
	JOIN original_balance o
		ON m.year = o.year
		AND m.month = o.month
	GROUP BY m.period, m.year, m.month
)


SELECT
	period,
	year::int,
	month::int,
	'Cash & cash equivalent at beginning of period' AS category_name,
	mm.metric_order,
	NULL AS subcategory_name,
	mm.submetric_order,
	NULL AS gl_name,
	CAST(NULL AS INT) AS gl_number,
	mtd_beginning AS mtd,
	ytd_beginning AS ytd,
	ttm_beginning AS ttm
FROM ce
LEFT JOIN metric_order mm ON 'Cash & cash equivalent at beginning of period' = mm.metric_name

UNION ALL

SELECT
	period,
	year::int,
	month::int,
	'Change in cash' AS category_name,
	mm.metric_order,
	NULL AS subcategory_name,
	mm.submetric_order,
	NULL AS gl_name,
	CAST(NULL AS INT) AS gl_number,
	mtd_change AS mtd,
	ytd_change AS ytd,
	ttm_change AS ttm
FROM ce
LEFT JOIN metric_order mm ON 'Change in cash' = mm.metric_name


UNION ALL


SELECT
	period,
	year::int,
	month::int,
	'Cash & cash equivalent at end of period' AS category_name,
	mm.metric_order,
	NULL AS subcategory_name,
	mm.submetric_order,
	NULL AS gl_name,
	CAST(NULL AS INT) AS gl_number,
	mtd_end AS mtd,
	ytd_end AS ytd,
	ttm_end AS ttm
FROM ce
LEFT JOIN metric_order mm ON 'Cash & cash equivalent at end of period' = mm.metric_name


UNION ALL


SELECT * FROM mtd_ytd_ttm
