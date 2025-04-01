WITH profit_and_loss AS (
    select * from {{ ref('pink_taco_profit_and_loss_staged') }}
),
metric_order AS (
    select * from {{ source('public', 'report_metric_config') }} where report_id = 3000
),

year_opening AS(
    SELECT
        p.period,
        p.concept_id,
        p.entity_id,
        p.gl_number,
        p.gl_name,
        p.year,
        p.month,
		p.category_id,
		p.category_name,
        p2.beginning_balance AS year_opening_balance
    FROM profit_and_loss p
    LEFT JOIN profit_and_loss p2 
        ON p.entity_id = p2.entity_id
        AND p.gl_number = p2.gl_number
        AND p.year::int = p2.year::int
        AND p2.month::int = 1 -- Year opening balance
),

mtd_ytd_ttm AS(
    SELECT
        p.*,
        COALESCE(p.ending_balance,0) - COALESCE(p.beginning_balance,0) AS actual_mtd,
        p.budget AS budget_mtd,
        p.ending_balance - COALESCE(y.year_opening_balance, 0) AS actual_ytd,
        SUM(COALESCE(p.ending_balance,0) - COALESCE(p.beginning_balance,0)) OVER (PARTITION BY p.concept_id, p.entity_id, p.gl_number ORDER BY CAST(p.year AS INT), CAST(p.month AS INT) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS actual_ttm,
        SUM(p.budget) OVER (PARTITION BY p.concept_id, p.entity_id, p.gl_number, p.year ORDER BY CAST(p.year AS INT), CAST(p.month AS INT)) AS budget_ytd,
        SUM(p.budget) OVER (PARTITION BY p.concept_id, p.entity_id, p.gl_number ORDER BY CAST(p.year AS INT), CAST(p.month AS INT) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS budget_ttm
    
	FROM profit_and_loss p
    LEFT JOIN year_opening y ON p.period = y.period
        AND p.concept_id = y.concept_id
        AND p.entity_id = y.entity_id
        AND p.gl_number = y.gl_number
        AND p.year = y.year
        AND p.month = y.month
),

financial_aggregates AS(
    SELECT
        period,
        year,
        month,
        concept_id,
        concept,
        concept_name,
        source_entity_id,
        entity_id,
        entity_name,
        -- Revenue Aggregations
        SUM(CASE WHEN category_name = 'Revenue' THEN budget ELSE 0 END) AS budget_revenue,
        SUM(CASE WHEN category_name = 'Revenue' THEN budget_mtd ELSE 0 END) AS budget_revenue_mtd,
        SUM(CASE WHEN category_name = 'Revenue' THEN budget_ytd ELSE 0 END) AS budget_revenue_ytd,
        SUM(CASE WHEN category_name = 'Revenue' THEN budget_ttm ELSE 0 END) AS budget_revenue_ttm,
        SUM(CASE WHEN category_name = 'Revenue' THEN ending_balance ELSE 0 END) AS actual_revenue,
        SUM(CASE WHEN category_name = 'Revenue' THEN actual_mtd ELSE 0 END) AS actual_revenue_mtd,
        SUM(CASE WHEN category_name = 'Revenue' THEN actual_ytd ELSE 0 END) AS actual_revenue_ytd,
        SUM(CASE WHEN category_name = 'Revenue' THEN actual_ttm ELSE 0 END) AS actual_revenue_ttm,
        -- COGS Aggregations
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN budget ELSE 0 END) AS budget_cogs,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN budget_mtd ELSE 0 END) AS budget_cogs_mtd,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN budget_ytd ELSE 0 END) AS budget_cogs_ytd,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN budget_ttm ELSE 0 END) AS budget_cogs_ttm,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN ending_balance ELSE 0 END) AS actual_cogs,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_mtd ELSE 0 END) AS actual_cogs_mtd,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_ytd ELSE 0 END) AS actual_cogs_ytd,
        SUM(CASE WHEN category_name = 'Cost of Goods Sold' THEN actual_ttm ELSE 0 END) AS actual_cogs_ttm,
        -- Operating Expenses Aggregations
        SUM(CASE WHEN category_id not in (2,18,23) THEN budget ELSE 0 END) as budget_operating_expenses,
        SUM(CASE WHEN category_id not in (2,18,23) THEN budget_mtd ELSE 0 END) as budget_operating_expenses_mtd,
        SUM(CASE WHEN category_id not in (2,18,23) THEN budget_ytd ELSE 0 END) as budget_operating_expenses_ytd,
        SUM(CASE WHEN category_id not in (2,18,23) THEN budget_ttm ELSE 0 END) as budget_operating_expenses_ttm,
        SUM(CASE WHEN category_id not in (2,18,23) THEN ending_balance ELSE 0 END) as actual_operating_expenses,
        SUM(CASE WHEN category_id not in (2,18,23) THEN actual_mtd ELSE 0 END) as actual_operating_expenses_mtd,
        SUM(CASE WHEN category_id not in (2,18,23) THEN actual_ytd ELSE 0 END) as actual_operating_expenses_ytd,
        SUM(CASE WHEN category_id not in (2,18,23) THEN actual_ttm ELSE 0 END) as actual_operating_expenses_ttm,
        -- ITDA Aggregations
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN budget ELSE 0 END) as budget_itda,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN budget_mtd ELSE 0 END) as budget_itda_mtd,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN budget_ytd ELSE 0 END) as budget_itda_ytd,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN budget_ttm ELSE 0 END) as budget_itda_ttm,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN ending_balance ELSE 0 END) as actual_itda,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_mtd ELSE 0 END) as actual_itda_mtd,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_ytd ELSE 0 END) as actual_itda_ytd,
        SUM(CASE WHEN category_name = 'Interest, Taxes, Depreciation and Amortization' THEN actual_ttm ELSE 0 END) as actual_itda_ttm

    FROM mtd_ytd_ttm
    GROUP BY period, concept_id, year, month, concept_id, concept, concept_name, source_entity_id, entity_id, entity_name
),

gross_profit AS(
    SELECT
        f.*,
        f.actual_revenue + f.actual_cogs AS actual_gross_profit,
        f.actual_revenue_mtd + f.actual_cogs_mtd AS actual_gross_profit_mtd,
        f.actual_revenue_ytd + f.actual_cogs_ytd AS actual_gross_profit_ytd,
        f.actual_revenue_ttm + f.actual_cogs_ttm AS actual_gross_profit_ttm,
        f.budget_revenue + f.budget_cogs AS budget_gross_profit,
        f.budget_revenue_mtd + f.budget_cogs_mtd AS budget_gross_profit_mtd,
        f.budget_revenue_ytd + f.budget_cogs_ytd AS budget_gross_profit_ytd,
        f.budget_revenue_ttm + f.budget_cogs_ttm AS budget_gross_profit_ttm
    FROM financial_aggregates f
),

ebitda AS(
    SELECT
        g.*,
        COALESCE(g.actual_gross_profit, 0) + f.actual_operating_expenses AS actual_ebitda,
        COALESCE(g.actual_gross_profit_mtd, 0) + f.actual_operating_expenses_mtd AS actual_ebitda_mtd,
        COALESCE(g.actual_gross_profit_ytd, 0) + f.actual_operating_expenses_ytd AS actual_ebitda_ytd,
        COALESCE(g.actual_gross_profit_ttm, 0) + f.actual_operating_expenses_ttm AS actual_ebitda_ttm,
        COALESCE(g.budget_gross_profit, 0) + f.budget_operating_expenses AS budget_ebitda,
        COALESCE(g.budget_gross_profit_mtd, 0) + f.budget_operating_expenses_mtd AS budget_ebitda_mtd,
        COALESCE(g.budget_gross_profit_ytd, 0) + f.budget_operating_expenses_ytd AS budget_ebitda_ytd,
        COALESCE(g.budget_gross_profit_ttm, 0) + f.budget_operating_expenses_ttm AS budget_ebitda_ttm
    FROM financial_aggregates f
    LEFT JOIN gross_profit g ON f.period = g.period
        AND f.concept_id = g.concept_id
        AND f.entity_id = g.entity_id
),

net_income AS(
    SELECT
        e.*,
        COALESCE(e.actual_ebitda, 0) + e.actual_itda AS actual_net_income,
        COALESCE(e.actual_ebitda_mtd, 0) + e.actual_itda_mtd AS actual_net_income_mtd,
        COALESCE(e.actual_ebitda_ytd, 0) + e.actual_itda_ytd AS actual_net_income_ytd,
        COALESCE(e.actual_ebitda_ttm, 0) + e.actual_itda_ttm AS actual_net_income_ttm,
        COALESCE(e.budget_ebitda, 0) + e.budget_itda AS budget_net_income,
        COALESCE(e.budget_ebitda_mtd, 0) + e.budget_itda_mtd AS budget_net_income_mtd,
        COALESCE(e.budget_ebitda_ytd, 0) + e.budget_itda_ytd AS budget_net_income_ytd,
        COALESCE(e.budget_ebitda_ttm, 0) + e.budget_itda_ttm AS budget_net_income_ttm
    FROM financial_aggregates f
    LEFT JOIN ebitda e ON f.period = e.period
        AND f.concept_id = e.concept_id
        AND f.entity_id = e.entity_id
)

-- Final Select
SELECT
    mm.metric_name,
    mm.metric_order,
    NULL as gl_name,
    NULL::int as gl_number,
    g.period,
    g.year,
    g.month,
    g.concept_id,
    g.concept,
    g.concept_name,
    g.source_entity_id,
    g.entity_id,
    g.entity_name,
    g.budget_gross_profit as budget,
    g.budget_gross_profit_mtd as budget_mtd,
    g.budget_gross_profit_ytd as budget_ytd,
    g.budget_gross_profit_ttm as budget_ttm,
    g.actual_gross_profit as ending_balance,
    g.actual_gross_profit_mtd as actual_mtd,
    g.actual_gross_profit_ytd as actual_ytd,
    g.actual_gross_profit_ttm as actual_ttm,
    g2.budget_gross_profit as py_budget,
    g2.budget_gross_profit_mtd as py_budget_mtd,
    g2.budget_gross_profit_ytd as py_budget_ytd,
    g2.budget_gross_profit_ttm as py_budget_ttm,
    g2.actual_gross_profit as py_ending_balance,
    g2.actual_gross_profit_mtd as py_actual_mtd,
    g2.actual_gross_profit_ytd as py_actual_ytd,
    g2.actual_gross_profit_ttm as py_actual_ttm
FROM gross_profit g
LEFT JOIN gross_profit g2
    ON g.concept_id = g2.concept_id
    AND g.entity_id = g2.entity_id
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
    e.concept_id,
    e.concept,
    e.concept_name,
    e.source_entity_id,
    e.entity_id,
    e.entity_name,
    e.budget_ebitda as budget,
    e.budget_ebitda_mtd as budget_mtd,
    e.budget_ebitda_ytd as budget_ytd,
    e.budget_ebitda_ttm as budget_ttm,
    e.actual_ebitda as ending_balance,
    e.actual_ebitda_mtd as actual_mtd,
    e.actual_ebitda_ytd as actual_ytd,
    e.actual_ebitda_ttm as actual_ttm,
    e2.budget_ebitda as py_budget,
    e2.budget_ebitda_mtd as py_budget_mtd,
    e2.budget_ebitda_ytd as py_budget_ytd,
    e2.budget_ebitda_ttm as py_budget_ttm,
    e2.actual_ebitda as py_ending_balance,
    e2.actual_ebitda_mtd as py_actual_mtd,
    e2.actual_ebitda_ytd as py_actual_ytd,
    e2.actual_ebitda_ttm as py_actual_ttm
FROM ebitda e
LEFT JOIN ebitda e2
    ON e.concept_id = e2.concept_id
    AND e.entity_id = e2.entity_id
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
    n.concept_id,
    n.concept,
    n.concept_name,
    n.source_entity_id,
    n.entity_id,
    n.entity_name,
    n.budget_net_income as budget,
    n.budget_net_income_mtd as budget_mtd,
    n.budget_net_income_ytd as budget_ytd,
    n.budget_net_income_ttm as budget_ttm,
    n.actual_net_income as ending_balance,
    n.actual_net_income_mtd as actual_mtd,
    n.actual_net_income_ytd as actual_ytd,
    n.actual_net_income_ttm as actual_ttm,
    n2.budget_net_income as py_budget,
    n2.budget_net_income_mtd as py_budget_mtd,
    n2.budget_net_income_ytd as py_budget_ytd,
    n2.budget_net_income_ttm as py_budget_ttm,
    n2.actual_net_income as py_ending_balance,
    n2.actual_net_income_mtd as py_actual_mtd,
    n2.actual_net_income_ytd as py_actual_ytd,
    n2.actual_net_income_ttm as py_actual_ttm
FROM net_income n
LEFT JOIN net_income n2
    ON n.concept_id = n2.concept_id
    AND n.entity_id = n2.entity_id
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
    myt.concept_id,
    myt.concept,
    myt.concept_name,
    myt.source_entity_id,
    myt.entity_id,
    myt.entity_name,
    myt.budget as budget,
    myt.budget_mtd as budget_mtd,
    myt.budget_ytd as budget_ytd,
    myt.budget_ttm as budget_ttm,
    myt.ending_balance as ending_balance,
    myt.actual_mtd as actual_mtd,
    myt.actual_ytd as actual_ytd,
    myt.actual_ttm as actual_ttm,
    myt2.budget as py_budget,
    myt2.budget_mtd as py_budget_mtd,
    myt2.budget_ytd as py_budget_ytd,
    myt2.budget_ttm as py_budget_ttm,
    myt2.ending_balance as py_ending_balance,
    myt2.actual_mtd as py_actual_mtd,
    myt2.actual_ytd as py_actual_ytd,
    myt2.actual_ttm as py_actual_ttm
FROM mtd_ytd_ttm myt
LEFT JOIN mtd_ytd_ttm myt2
    ON myt.concept_id = myt2.concept_id
    AND myt.entity_id = myt2.entity_id
    AND myt.gl_number = myt2.gl_number
    AND myt.month = myt2.month
    AND myt.year::int = myt2.year::int + 1
LEFT JOIN metric_order mm 
ON mm.metric_name = myt.category_name

