WITH profit_and_loss AS (
    select * from {{ ref('unimed_profit_and_loss_staged') }}
),
metric_order AS (
    select * from {{ source('public', 'report_metric_config') }} where report_id = 3000
),

mtd_ytd_ttm AS(
SELECT
    p.*,
    COALESCE(p.ending_balance,0) - COALESCE(p.beginning_balance,0) AS actual_mtd,
    p.ending_balance AS actual_ytd,
    SUM(COALESCE(p.ending_balance,0) - COALESCE(p.beginning_balance,0)) OVER (PARTITION BY p.country_id, p.gl_number ORDER BY CAST(p.year AS INT), CAST(p.month AS INT) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS actual_ttm
FROM profit_and_loss p
),

financial_aggregates AS (
SELECT
    period,
    year,
    month,
    country_id,
    country_name,
    -- Revenue Aggregations
    SUM(CASE WHEN category_name = 'Revenue' THEN ending_balance ELSE 0 END) AS actual_revenue,
    SUM(CASE WHEN category_name = 'Revenue' THEN actual_mtd ELSE 0 END) AS actual_revenue_mtd,
    SUM(CASE WHEN category_name = 'Revenue' THEN actual_ytd ELSE 0 END) AS actual_revenue_ytd,
    SUM(CASE WHEN category_name = 'Revenue' THEN actual_ttm ELSE 0 END) AS actual_revenue_ttm,
    -- COGS Aggregations
    SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN ending_balance ELSE 0 END) AS actual_cogs,
    SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_mtd ELSE 0 END) AS actual_cogs_mtd,
    SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_ytd ELSE 0 END) AS actual_cogs_ytd,
    SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_ttm ELSE 0 END) AS actual_cogs_ttm,
    -- Operating Expenses Aggregations (not revenue, cogs, or itda)
    SUM(CASE WHEN category_id not in (2,18,23) THEN ending_balance ELSE 0 END) as actual_operating_expenses,
    SUM(CASE WHEN category_id not in (2,18,23) THEN actual_mtd ELSE 0 END) as actual_operating_expenses_mtd,
    SUM(CASE WHEN category_id not in (2,18,23) THEN actual_ytd ELSE 0 END) as actual_operating_expenses_ytd,
    SUM(CASE WHEN category_id not in (2,18,23) THEN actual_ttm ELSE 0 END) as actual_operating_expenses_ttm,
    -- ITDA Aggregations
    SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN ending_balance ELSE 0 END) as actual_itda,
    SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_mtd ELSE 0 END) as actual_itda_mtd,
    SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_ytd ELSE 0 END) as actual_itda_ytd,
    SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_ttm ELSE 0 END) as actual_itda_ttm

FROM mtd_ytd_ttm
GROUP BY period, year, month, country_id, country_name
ORDER BY year, month, country_name
),

gross_profit AS(

    SELECT
        f.*,
        f.actual_revenue + f.actual_cogs AS actual_gross_profit,
        f.actual_revenue_mtd + f.actual_cogs_mtd AS actual_gross_profit_mtd,
        f.actual_revenue_ytd + f.actual_cogs_ytd AS actual_gross_profit_ytd,
        f.actual_revenue_ttm + f.actual_cogs_ttm AS actual_gross_profit_ttm
    FROM financial_aggregates f
),

ebitda AS(
    SELECT
        g.*,
        COALESCE(g.actual_gross_profit, 0) + f.actual_operating_expenses AS actual_ebitda,
        COALESCE(g.actual_gross_profit_mtd, 0) + f.actual_operating_expenses_mtd AS actual_ebitda_mtd,
        COALESCE(g.actual_gross_profit_ytd, 0) + f.actual_operating_expenses_ytd AS actual_ebitda_ytd,
        COALESCE(g.actual_gross_profit_ttm, 0) + f.actual_operating_expenses_ttm AS actual_ebitda_ttm
    FROM financial_aggregates f
    LEFT JOIN gross_profit g ON f.period = g.period
        AND f.country_id = g.country_id
),

net_income AS(
    SELECT
        e.*,
        COALESCE(e.actual_ebitda, 0) + e.actual_itda AS actual_net_income,
        COALESCE(e.actual_ebitda_mtd, 0) + e.actual_itda_mtd AS actual_net_income_mtd,
        COALESCE(e.actual_ebitda_ytd, 0) + e.actual_itda_ytd AS actual_net_income_ytd,
        COALESCE(e.actual_ebitda_ttm, 0) + e.actual_itda_ttm AS actual_net_income_ttm
    FROM financial_aggregates f
    LEFT JOIN ebitda e ON f.period = e.period
        AND f.country_id = e.country_id
)

SELECT
    mm.metric_name,
    mm.metric_order,
    NULL as gl_name,
    NULL::int as gl_number,
    g.period,
    g.year,
    g.month,
    g.country_id,
    g.country_name,
    g.actual_gross_profit as ending_balance,
    g.actual_gross_profit_mtd as actual_mtd,
    g.actual_gross_profit_ytd as actual_ytd,
    g.actual_gross_profit_ttm as actual_ttm,
    g2.actual_gross_profit as py_ending_balance,
    g2.actual_gross_profit_mtd as py_actual_mtd,
    g2.actual_gross_profit_ytd as py_actual_ytd,
    g2.actual_gross_profit_ttm as py_actual_ttm
FROM gross_profit g
LEFT JOIN gross_profit g2
    ON g.country_id = g2.country_id
    AND g.month = g2.month
    AND g.year::int = g2.year::int + 1
LEFT JOIN metric_order mm
    ON mm.metric_name = 'Gross Profit'
    
UNION ALL

SELECT
    mm.metric_name,
    mm.metric_order,
    NULL as gl_name,
    NULL::int as gl_number,
    e.period,
    e.year,
    e.month,
	e.country_id,
	e.country_name,
    e.actual_ebitda as ending_balance,
    e.actual_ebitda_mtd as actual_mtd,
    e.actual_ebitda_ytd as actual_ytd,
    e.actual_ebitda_ttm as actual_ttm,
    e2.actual_ebitda as py_ending_balance,
    e2.actual_ebitda_mtd as py_actual_mtd,
    e2.actual_ebitda_ytd as py_actual_ytd,
    e2.actual_ebitda_ttm as py_actual_ttm
FROM ebitda e
LEFT JOIN ebitda e2
    ON e.country_id = e2.country_id
    AND e.month = e2.month
    AND e.year::int = e2.year::int + 1
LEFT JOIN metric_order mm
    ON mm.metric_name = 'EBITDA'

UNION ALL 

SELECT
    mm.metric_name,
    mm.metric_order,
    NULL as gl_name,
    NULL::int as gl_number,
    n.period,
    n.year,
    n.month,
	n.country_id,
	n.country_name,
    n.actual_net_income as ending_balance,
    n.actual_net_income_mtd as actual_mtd,
    n.actual_net_income_ytd as actual_ytd,
    n.actual_net_income_ttm as actual_ttm,
    n2.actual_net_income as py_ending_balance,
    n2.actual_net_income_mtd as py_actual_mtd,
    n2.actual_net_income_ytd as py_actual_ytd,
    n2.actual_net_income_ttm as py_actual_ttm
FROM net_income n
LEFT JOIN net_income n2
	ON n.country_id = n2.country_id
    AND n.month = n2.month
    AND n.year::int = n2.year::int + 1
LEFT JOIN metric_order mm
    ON mm.metric_name = 'Net Income'
 
UNION ALL

SELECT
    mm.metric_name,
    mm.metric_order,
    myt.gl_name,
    myt.gl_number,
    myt.period,
    myt.year,
    myt.month,
	myt.country_id,
	myt.country_name,
    myt.ending_balance as ending_balance,
    myt.actual_mtd as actual_mtd,
    myt.actual_ytd as actual_ytd,
    myt.actual_ttm as actual_ttm,
    myt2.ending_balance as py_ending_balance,
    myt2.actual_mtd as py_actual_mtd,
    myt2.actual_ytd as py_actual_ytd,
    myt2.actual_ttm as py_actual_ttm
FROM mtd_ytd_ttm myt
LEFT JOIN mtd_ytd_ttm myt2
    ON myt.country_id = myt2.country_id
    AND myt.gl_number = myt2.gl_number
    AND myt.month = myt2.month
    AND myt.year::int = myt2.year::int + 1
LEFT JOIN metric_order mm 
ON mm.metric_name = myt.category_name

