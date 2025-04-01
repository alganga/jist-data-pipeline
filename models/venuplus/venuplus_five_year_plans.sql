with fyp_base as (
    select 
        company_id,
        cast(year as int) as year,
        cast(month as int) as month,
        projected_year,
        date as type,
        revenue,
        ebitda
    from {{source('venuplus', 'five_year_plans')}}
    where deleted_at is NULL
),

profit_and_loss as (
    select *
    from {{ref('venuplus_profit_and_loss')}}
),

revenue_data as (
    select
        cast(pnl.month as int) as month,
        cast(pnl.year as int) as year,
        sum(budget_mtd) as revenue_budget,
		sum(actual_mtd) as revenue_actual
    from profit_and_loss  pnl
    where metric_name = 'Revenue'
    group by 1, 2
),


ebitda_data as (
    select
        cast(pnl.month as int) as month,
        cast(pnl.year as int) as year,
        sum(budget_mtd) as ebitda_budget,
		sum(actual_mtd) as ebitda_actual
    from profit_and_loss pnl
    where metric_name = 'EBITDA'
    group by 1, 2
),

revenue_values as (
	SELECT
        month,
        year,
        SUM(revenue_actual) OVER (PARTITION BY year ORDER BY month) +
        COALESCE(SUM(revenue_budget) OVER (PARTITION BY year ORDER BY month desc),0) - COALESCE(revenue_budget,0) AS revenue_value
    FROM revenue_data
),

ebitda_values as (
    SELECT
        month,
        year,
        SUM(ebitda_actual) OVER (PARTITION BY year ORDER BY month) +
        COALESCE(SUM(ebitda_budget) OVER (PARTITION BY year ORDER BY month desc),0) - COALESCE(ebitda_budget,0) AS ebitda_value
    FROM ebitda_data
),

actual_values as (
    select 
        10 as company_id,
        r.year,
        r.month,
        r.year as projected_year,
        'Actual' as type,
        revenue_value as revenue,
        ebitda_value as ebitda
    from revenue_values r
    inner join ebitda_values e on r.year = e.year and r.month = e.month
),

five_year_plans as (
    select * from fyp_base
    union all
    (select * from actual_values order by year desc, month desc limit 1)
)

select * from five_year_plans