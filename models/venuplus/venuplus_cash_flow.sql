WITH metric_order AS (
    SELECT * FROM {{source('public', 'report_metric_config')}} WHERE report_id = 5000
),

ebitda AS(
    SELECT
        period,
        year::int,
        month::int,
        country_id::int,
        country_name,
        'Cash flow from Operating Activities' as category_name,
        m.metric_order,
        'EBITDA' as subcategory_name,
        m2.submetric_order,
        'EBITDA' as gl_name,
        NULL::int as gl_number,
        SUM(actual_mtd) as mtd,
        SUM(actual_ytd) as ytd,
        SUM(actual_ttm) as ttm
    FROM {{ref('venuplus_profit_and_loss')}} pnl
    LEFT JOIN metric_order m ON 'Cash flow from Operating Activities' = m.metric_name
    LEFT JOIN metric_order m2 ON 'EBITDA' = m2.metric_name
    WHERE pnl.metric_name = 'EBITDA'
    GROUP BY period, year, month, country_id, country_name, m.metric_order, m2.submetric_order
),

cash_flow AS(
	SELECT
		period,
		month,
		year,
		country_id,
    country_name,
    category_name,
    category_id,
    gl_name,
    gl_number,
    ending_balance,
    beginning_balance,
    CASE
      WHEN signage = 1 AND absolute_flag IS TRUE THEN ABS(mtd)
      WHEN signage = 1 AND absolute_flag IS FALSE THEN mtd
      WHEN signage = -1 AND absolute_flag IS TRUE THEN -1 * ABS(mtd)
      WHEN signage = -1 AND absolute_flag IS FALSE THEN -1 * mtd
    END AS mtd,
    metric_order,
    subcategory_name,
    submetric_order
	FROM(
	    SELECT 
	        period,
	        country_id,
	        country_name,
	        category_name,
	        category_id,
	        gl_name,
	        gl_number,
	        SUM(ending_balance) AS ending_balance,
	        SUM(beginning_balance) AS beginning_balance,
	        SUM(ending_balance) - SUM(beginning_balance) AS mtd,
	        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
	        '20' || SPLIT_PART(period, '-', 2) AS year,
	        metric_order,
	        subcategory_name,
	        submetric_order,
	        signage,
	        absolute_flag
	    FROM {{ref('venuplus_target_trial_balance')}}
	    WHERE report_id = 5000
	    GROUP BY period, country_id, country_name, category_name, category_id, gl_name, gl_number, metric_order, subcategory_name, submetric_order, signage, absolute_flag
    )T1
),

original_balance AS (
    SELECT
        country_id,
        country_name,
        year::int,
        month::int,
        FIRST_VALUE(sum_beginning_balance) OVER(PARTITION BY country_id ORDER BY year, month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS original_balance
    FROM (
        SELECT
            country_id,
            country_name,
            year,
            month,
            SUM(beginning_balance) AS sum_beginning_balance
        FROM {{ref('venuplus_target_trial_balance')}}
        WHERE report_id = 5000
        AND subcategory_id = 33 -- only use 'cash and cash equivalents'
        GROUP BY country_id, country_name, year, month
    ) t1
),

mtd_ytd_ttm AS (
    SELECT
        period,
        year::int,
        month::int,
        country_id::int,
        country_name,
        category_name,
        metric_order,
        subcategory_name,
        submetric_order,
        gl_name,
        gl_number::int,
        mtd AS mtd_change,
        SUM(mtd) OVER(PARTITION BY country_id, gl_number, year ORDER BY year, month) as ytd_change,
        SUM(mtd) OVER(PARTITION BY country_id, gl_number ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ttm_change
    FROM (
        SELECT
            cf1.period,
            cf1.year,
            cf1.month,
            cf1.country_id,
            cf1.country_name,
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
    ) c1
    
    UNION ALL
    
    SELECT * FROM ebitda
),

ce AS(
    SELECT
        m.period,
        m.year,
        m.month,
        m.country_id,
        m.country_name,
        -- mtd beginning, change, end
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) - SUM(mtd_change) + AVG(original_balance)  AS mtd_beginning,
        SUM(mtd_change) AS mtd_change,
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) + AVG(original_balance) AS mtd_end,
        
        -- ytd beginning, change, end
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) - SUM(ytd_change) + AVG(original_balance) AS ytd_beginning,
        SUM(ytd_change) AS ytd_change,
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) + AVG(original_balance) AS ytd_end,
        
        -- ttm beginning, change, end
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) - SUM(ttm_change) + AVG(original_balance) AS ttm_beginning,
        SUM(ttm_change) AS ttm_change,
        SUM(SUM(mtd_change)) OVER(PARTITION BY m.country_id ORDER BY m.year, m.month) + AVG(original_balance) AS ttm_end
        
    FROM mtd_ytd_ttm m
    JOIN original_balance o
        ON m.country_id = o.country_id
        AND m.year = o.year
        AND m.month = o.month
    GROUP BY m.period, m.year, m.month, m.country_id, m.country_name
)

SELECT
    period,
    year::int,
    month::int,
    country_id,
    country_name,
    'Cash & cash equivalent at beginning of period' AS category_name,
    mm.metric_order,
    NULL AS subcategory_name,
    mm.submetric_order,
    NULL AS gl_name,
    NULL::int AS gl_number,
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
    country_id,
    country_name,
    'Change in cash' AS category_name,
    mm.metric_order,
    NULL AS subcategory_name,
    mm.submetric_order,
    NULL AS gl_name,
    NULL::int AS gl_number,
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
    country_id,
    country_name,
    'Cash & cash equivalent at end of period' AS category_name,
    mm.metric_order,
    NULL AS subcategory_name,
    mm.submetric_order,
    NULL AS gl_name,
    NULL::int AS gl_number,
    mtd_end AS mtd,
    ytd_end AS ytd,
    ttm_end AS ttm
FROM ce
LEFT JOIN metric_order mm ON 'Cash & cash equivalent at end of period' = mm.metric_name

UNION ALL

SELECT
    period,
    year::int,
    month::int,
    country_id,
    country_name,
    category_name,
    metric_order,
    subcategory_name,
    submetric_order,
    gl_name,
    gl_number,
    mtd_change AS mtd,
    ytd_change AS ytd,
    ttm_change AS ttm
FROM mtd_ytd_ttm



