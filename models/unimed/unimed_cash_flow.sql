WITH metric_order AS (
    SELECT * FROM {{source('public', 'report_metric_config')}} WHERE report_id = 5000
),

net_income AS(
    SELECT
        period,
        year::int,
        month::int,
        country_id::int,
        country_name,
        'Cash flow from Operating Activities' as category_name,
        m.metric_order,
        'Net Income' as subcategory_name,
        m2.submetric_order,
        'Net Income' as gl_name,
        NULL::int as gl_number,
        SUM(actual_mtd) as mtd,
        SUM(actual_ytd) as ytd,
        SUM(SUM(actual_mtd)) OVER (PARTITION BY country_id ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as ttm
    FROM {{ref('unimed_profit_and_loss')}}  pnl
    LEFT JOIN metric_order m ON 'Cash flow from Operating Activities' = m.metric_name
    LEFT JOIN metric_order m2 ON 'Net Income' = m2.metric_name
    WHERE pnl.metric_name = 'Net Income'
    AND year > 2022
    GROUP BY period, year, month, country_id, country_name, m.metric_order, m2.submetric_order
),

cash_flow AS (
	SELECT
		period,
		year,
		month,
		country_id,
		country_name,
		category_name,
		metric_order,
		subcategory_name,
		submetric_order,
		gl_number,
		gl_name,
		ending_balance,
		beginning_balance,
        CASE
            -- 'Interest, Taxes, Depreciation and Amortization'
            WHEN subcategory_id = 23 THEN mtd
            -- 'Trade Receivables' 
            WHEN subcategory_id = 34 THEN 
                CASE 
                    WHEN mtd > 0 THEN -ABS(mtd) 
                    ELSE ABS(mtd) 
                END
            -- 'Inventories'
            WHEN subcategory_id = 15 THEN 
                CASE 
                    WHEN mtd > 0 THEN -ABS(mtd)
                    ELSE ABS(mtd)
                END
            -- 'Prepayments and Other Receivables' 
            WHEN subcategory_id = 69 THEN mtd

            -- 'Trade Payables'
            WHEN subcategory_id = 78 THEN mtd

            -- 'Accrued Liabilities' 
            WHEN subcategory_id = 37 THEN mtd

            -- 'Property, Plant & Equipment'
            WHEN subcategory_id = 101 THEN 
                CASE 
                    WHEN mtd > 0 THEN -ABS(mtd)
                    ELSE ABS(mtd) 
                END
            -- 'Long Term Liabilities'
            WHEN subcategory_id = 107 THEN
                CASE 
                    WHEN mtd > 0 THEN -ABS(mtd)
                    ELSE ABS(mtd) 
                END

            -- 'Long Term Debts'
            WHEN subcategory_id = 108 THEN
                CASE 
                    WHEN mtd > 0 THEN -ABS(mtd)
                    ELSE ABS(mtd) 
                END
            ELSE 
                CASE 
                    WHEN mtd = 0 THEN 0 
                    ELSE mtd 
                END
        END AS mtd
	FROM (
		SELECT
			period,
			year,
			month,
			country_id,
			country_name,
			category_name,
			metric_order,
			subcategory_name,
            subcategory_id,
			submetric_order,
			gl_number,
			gl_name,
			ending_balance,
			COALESCE(LAG(ending_balance, 1) OVER (PARTITION BY country_id, gl_number ORDER BY year, month), 0) AS beginning_balance,
			ending_balance - COALESCE(LAG(ending_balance, 1) OVER (PARTITION BY country_id, gl_number ORDER BY year, month), 0) AS mtd
		FROM (
			SELECT
				'P'||EXTRACT(MONTH FROM date)::VARCHAR||'-'||TO_CHAR(DATE, 'YY') AS period,
				 EXTRACT(YEAR FROM date::DATE) AS year,
				 EXTRACT(MONTH from date::DATE) AS month,
				 date,
				 country_id,
				 country_name,
				 category_name,
		        metric_order, 
		        (CASE WHEN subcategory_name IS NULL THEN category_name
		            ELSE subcategory_name
		            END) AS subcategory_name,
                (CASE WHEN subcategory_id IS NULL THEN category_id
		            ELSE subcategory_id
		            END) AS subcategory_id,
		        submetric_order,
		        gl_number,
		        gl_name,
                -- We have to recalculate ending balance as it should not consider previous years
		        SUM(debit - credit) OVER (PARTITION BY country_id, gl_number ORDER BY date asc) AS ending_balance,
		        ROW_NUMBER() OVER (PARTITION BY country_id, gl_number, EXTRACT(YEAR FROM date::DATE), EXTRACT(MONTH from date::DATE) ORDER BY DATE DESC) AS row
			FROM {{ref('unimed_target_trial_balance')}}
			WHERE report_id = 5000
             -- Exclude cash and cash equivalent accounts (used in below original balance cte)
            AND subcategory_id != 33
            AND EXTRACT(YEAR FROM date::DATE) > 2022
		)T0
		WHERE row = 1
	)T1
),

original_balance AS (
    SELECT DISTINCT
        country_id,
        country_name,
        year,
        month,
        FIRST_VALUE(ending_balance) OVER(PARTITION BY country_id ORDER BY year, month, day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS original_balance
    FROM (
        SELECT
            country_id,
            country_name,
            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(MONTH FROM DATE) AS month,
            EXTRACT(DAY FROM DATE) AS day,
            SUM(ending_balance) AS ending_balance
        FROM {{ref('unimed_target_trial_balance')}}
        WHERE report_id = 5000
        AND subcategory_id = 33 -- only cash and cash equivalent accs
        AND EXTRACT(YEAR FROM date) > 2022
        GROUP BY country_id, country_name, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM DATE), EXTRACT(DAY FROM DATE)
        ) T0
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
    
    SELECT * FROM net_income
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