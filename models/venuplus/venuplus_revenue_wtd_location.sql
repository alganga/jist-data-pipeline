with revenue_wtd as (
select
    location_key as location_code,
    venu_focus as location_type,
    location_type as location_sub_type,
    location as location_name,
    sublocation as sub_location_name,
    date::date as sales_date,
    cast(extract(year from date::date) as int) as year,
    cast(extract(month from date::date) as int) as month,
    extract(day from date::date) as day,
    end_of_week,
    period_end_date,
    sum(revenue_wtd.net_sales::float) as net_sales,
    0 as discounts,
    sum(revenue_wtd.gross_sales::float) as gross_sales,
    sum(revenue_wtd.gross_sales::float) * 0.37 as cogs
from (
	select
        *,
        cast({{ get_end_of_week('date', 'Monday', 'Sunday') }} as date) as end_of_week,
        ((DATE_TRUNC('month', date::date) + INTERVAL '1 month' - INTERVAL '1 day') - (EXTRACT(DOW FROM (DATE_TRUNC('month', date::date) + INTERVAL '1 month' - INTERVAL '1 day'))) * INTERVAL '1 day')::date AS period_end_date
	from {{ source('venuplus', 'revenue_wtd_location') }}
	) as revenue_wtd
where revenue_wtd.is_deleted = FALSE
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
order by location_code, sales_date
),
actual_wtd_calculation as (
select
    t1.location_code,
    t1.location_type,
    t1.location_sub_type,
    t1.location_name,
    t1.sub_location_name,
    t1.sales_date,
    t1.year,
    t1.month,
    t1.day,
    t1.net_sales,
    t1.discounts,
    t1.gross_sales,
    t1.cogs,
    t1.end_of_week,
    t1.period_end_date,
    extract(day from t1.end_of_week) as end_of_week_day,
    extract(month from t1.end_of_week) as end_of_week_month,
    extract(year from t1.end_of_week) as end_of_week_year,
    sum(net_sales) over (partition by location_code, end_of_week order by sales_date) as wtd_net_sales,
    sum(discounts) over (partition by location_code, end_of_week order by sales_date) as wtd_discounts,
    sum(gross_sales) over (partition by location_code, end_of_week order by sales_date) as wtd_gross_sales,
    sum(cogs) over (partition by location_code, end_of_week order by sales_date) as wtd_cogs
from(
    select *
    from revenue_wtd
) t1
),
actual_mtd_ytd_ttm_calculation as (
select
    location_code,
    location_type,
    location_sub_type,
    location_name,
    sub_location_name,
    sales_date,
    year,
    month,
    day,
    net_sales,
    discounts,
    gross_sales,
    cogs,
    end_of_week,
    end_of_week_day,
    end_of_week_month,
    end_of_week_year,
    cast(date_trunc('month', cast(sales_date as date)) as date) as start_of_month,
    period_end_date,
    period_end_month,
    period_end_year,
    sum(net_sales) over (partition by location_code, period_end_year, period_end_month order by sales_date) as mtd_net_sales,
    sum(discounts) over (partition by location_code, period_end_year, period_end_month order by sales_date) as mtd_discounts,
    sum(gross_sales) over (partition by location_code, period_end_year, period_end_month order by sales_date) as mtd_gross_sales,
    sum(cogs) over (partition by location_code,  period_end_year, period_end_month order by sales_date) as mtd_cogs,
    sum(net_sales) over (partition by location_code, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_net_sales,
    sum(discounts) over (partition by location_code, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_discounts,
    sum(gross_sales) over (partition by location_code, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_gross_sales,
    sum(cogs) over (partition by location_code, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_cogs,
    sum(net_sales) over (partition by location_code order by period_end_year, period_end_month rows between 11 preceding and current row) as ttm_net_sales,
    sum(discounts) over (partition by location_code order by period_end_year, period_end_month rows between 11 preceding and current row) as ttm_discounts,
    sum(gross_sales) over (partition by location_code order by period_end_year, period_end_month rows between 11 preceding and current row) as ttm_gross_sales,
    sum(cogs) over (partition by location_code order by period_end_year, period_end_month rows between 11 preceding and current row) as ttm_cogs
from (
	select
		*,
		cast(extract(month from period_end_date) as int) as period_end_month,
	    cast(extract(year from period_end_date) as int) as period_end_year
	from actual_wtd_calculation
	) wtd 
),
actual_gross_cte as(
    select
        mtd_ytd_ttm.location_code,
	    mtd_ytd_ttm.location_type,
	    mtd_ytd_ttm.location_sub_type,
	    mtd_ytd_ttm.location_name,
	    mtd_ytd_ttm.sub_location_name,
        mtd_ytd_ttm.month,
        mtd_ytd_ttm.year,
        mtd_ytd_ttm.day,
        abs(mtd_gross_sales) - abs(mtd_cogs) as  actual_mtd_gross_profit,
        abs(ytd_gross_sales) - abs(ytd_cogs) as actual_ytd_gross_profit,
        abs(ttm_gross_sales) - abs(ttm_cogs) as actual_ttm_gross_profit,
        abs(wtd.wtd_gross_sales) - abs(wtd.wtd_cogs) as actual_wtd_gross_profit
    from actual_mtd_ytd_ttm_calculation mtd_ytd_ttm
     left join actual_wtd_calculation wtd
        on mtd_ytd_ttm.location_code = wtd.location_code
        and mtd_ytd_ttm.year = wtd.year
	    and mtd_ytd_ttm.month = wtd.month
	    and mtd_ytd_ttm.day = wtd.day
),
actual_combined as(
    select
        mtd_ytd_ttm.location_code,
        mtd_ytd_ttm.location_type,
        mtd_ytd_ttm.location_sub_type,
        mtd_ytd_ttm.location_name,
        mtd_ytd_ttm.sub_location_name,
        mtd_ytd_ttm.sales_date,
        mtd_ytd_ttm.year,
        mtd_ytd_ttm.month,
        mtd_ytd_ttm.day,
        mtd_ytd_ttm.end_of_week,
        mtd_ytd_ttm.end_of_week_day,
        mtd_ytd_ttm.end_of_week_month,
        mtd_ytd_ttm.end_of_week_year,
        mtd_ytd_ttm.period_end_date,
        mtd_ytd_ttm.period_end_month,
        mtd_ytd_ttm.period_end_year,
        mtd_ytd_ttm.start_of_month,
        mtd_ytd_ttm.net_sales,
        mtd_ytd_ttm.discounts,
        mtd_ytd_ttm.gross_sales,
        mtd_ytd_ttm.cogs,
        mtd_ytd_ttm.mtd_net_sales,
        mtd_ytd_ttm.mtd_gross_sales,
        mtd_ytd_ttm.mtd_discounts,
        mtd_ytd_ttm.mtd_cogs,
        mtd_ytd_ttm.ytd_net_sales,
        mtd_ytd_ttm.ytd_gross_sales,
        mtd_ytd_ttm.ytd_discounts,
        mtd_ytd_ttm.ytd_cogs,
        mtd_ytd_ttm.ttm_net_sales,
        mtd_ytd_ttm.ttm_gross_sales,
        mtd_ytd_ttm.ttm_discounts,
        mtd_ytd_ttm.ttm_cogs,
        wtd_net_sales,
        wtd_gross_sales,
        wtd_discounts,
        wtd_cogs,
        actual_mtd_gross_profit,
        actual_ytd_gross_profit,
        actual_wtd_gross_profit,
        actual_ttm_gross_profit,
        null as budget_wtd_net_sales,
        null as budget_mtd_net_sales,
        null as budget_ytd_net_sales,
        null as budget_ttm_net_sales,
        null as budget_wtd_gross_sales,
        null as budget_mtd_gross_sales,
        null as budget_ytd_gross_sales,
        null as budget_ttm_gross_sales,
        null as budget_mtd_gross_profit,
        null as budget_wtd_gross_profit,
        null as budget_ytd_gross_profit,
        null as budget_ttm_gross_profit,
        null as cogs_budget_mtd,
        null as cogs_budget_wtd,
        null as cogs_budget_ytd,
        null as cogs_budget_ttm
    from actual_mtd_ytd_ttm_calculation mtd_ytd_ttm
    left join actual_wtd_calculation wtd
    on mtd_ytd_ttm.location_code = wtd.location_code
    and mtd_ytd_ttm.year = wtd.year
    and mtd_ytd_ttm.month = wtd.month
    and mtd_ytd_ttm.day = wtd.day
    left join actual_gross_cte gross
    on gross.location_code = mtd_ytd_ttm.location_code
    and mtd_ytd_ttm.year = gross.year
    and mtd_ytd_ttm.month = gross.month
    and mtd_ytd_ttm.day = gross.day
)
select
    t1.*,
    t2.net_sales as net_sales_py,
    t2.discounts as discounts_py,
    t2.gross_sales as gross_sales_py,
    t2.cogs as cogy_py,
    t2.mtd_net_sales as mtd_net_sales_py,
    t2.mtd_discounts as mtd_discounts_py,
    t2.mtd_gross_sales as mtd_gross_sales_py,
    t2.mtd_cogs as mtd_cogs_py,
    t2.ytd_net_sales as ytd_net_sales_py,
    t2.ytd_discounts as ytd_discounts_py,
    t2.ytd_gross_sales as ytd_gross_sales_py,
    t2.ytd_cogs as ytd_cogs_py,
    t2.ttm_net_sales as ttm_net_sales_py,
    t2.ttm_discounts as ttm_discounts_py,
    t2.ttm_gross_sales as ttm_gross_sales_py,
    t2.ttm_cogs as ttm_cogs_py,
    t2.wtd_net_sales as wtd_net_sales_py,
    t2.wtd_discounts as wtd_discounts_py,
    t2.wtd_gross_sales as wtd_gross_sales_py,
    t2.wtd_cogs as wtd_cogs_py,
    t2.actual_mtd_gross_profit as actual_mtd_gross_profit_py,
    t2.actual_ytd_gross_profit as actual_ytd_gross_profit_py,
    t2.actual_wtd_gross_profit as actual_wtd_gross_profit_py,
    t2.actual_ttm_gross_profit as actual_ttm_gross_profit_py,
    t2.budget_wtd_net_sales as budget_wtd_net_sales_py,
    t2.budget_mtd_net_sales as budget_mtd_net_sales_py,
    t2.budget_ytd_net_sales as budget_ytd_net_sales_py,
    t2.budget_ttm_net_sales as budget_ttm_net_sales_py,
    t2.budget_wtd_gross_sales as budget_wtd_gross_sales_py,
    t2.budget_mtd_gross_sales as budget_mtd_gross_sales_py,
    t2.budget_ytd_gross_sales as budget_ytd_gross_sales_py,
    t2.budget_ttm_gross_sales as budget_ttm_gross_sales_py,
    t2.budget_mtd_gross_profit as budget_mtd_gross_profit_py,
    t2.budget_wtd_gross_profit as budget_wtd_gross_profit_py ,
    t2.budget_ytd_gross_profit as budget_ytd_gross_profit_py,
    t2.budget_ttm_gross_profit as budget_ttm_gross_profit_py,
    t2.cogs_budget_mtd as cogs_budget_mtd_py,
    t2.cogs_budget_wtd as cogs_budget_wtd_py,
    t2.cogs_budget_ytd as cogs_budget_ytd_py,
    t2.cogs_budget_ttm as cogs_budget_ttm_py
from actual_combined t1
left join actual_combined t2
on t1.location_code = t2.location_code
    and EXTRACT(WEEK FROM t1.sales_date)=EXTRACT(WEEK FROM t2.sales_date)
    and t1.year = t2.year + 1
