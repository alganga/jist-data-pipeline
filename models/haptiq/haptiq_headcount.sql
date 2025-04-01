with headcount_base as (
    select *
    from {{ source('haptiq', 'headcount') }}
),

profit_and_loss as (
    select *
    from {{ ref('haptiq_profit_and_loss') }}
),

revenue_data as (
    select
        cast(pnl.month as int) as month,
        cast(pnl.year as int) as year,
        round(cast(sum(actual_ttm)/1000000 as numeric), 2) as value -- dividing ttm by a million
    from profit_and_loss  pnl
    where metric_name = 'Revenue'
    group by 1, 2
),

ebitda_data as (
    select
        cast(pnl.month as int) as month,
        cast(pnl.year as int) as year,
        round(cast(sum(actual_ttm)/1000000 as numeric), 2) as value -- dividing ttm by a million
    from profit_and_loss pnl
    where metric_name = 'EBITDA'
    group by 1, 2
),

headcount_agg as (
	select
		h."year",
		h."month",
        h."department",
		sum(h.employees) as employees,
		sum(h.payroll_total) as payroll_total,
		cast(null as float) as revenue,
        cast(null as float) as ebitda
	from headcount_base h
	group by h.month, h.year, h."department"
),

ebitda_revenue as (
    select
        distinct on(h.year, h.month)
        h.year,
        h.month,
        'ebitda_revenue' as department,
        cast(null as bigint) as employees,
        cast(null as bigint) as payroll_total,
        r.value as revenue,
        e.value as ebitda
    from headcount_agg h
    left join ebitda_data e on cast(e.month as int) = cast(h.month as int) and cast(e.year as int) = cast(h.year as int)
    left join revenue_data r on cast(r.month as int) = cast(h.month as int) and cast(r.year as int) = cast(h.year as int)
    order by h.year, h.month
),

headcount as (
	select * from ebitda_revenue
	union all
	select * from headcount_agg
)

select * from headcount
