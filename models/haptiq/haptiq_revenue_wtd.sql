with revenue_wtd as (
select
    revenue_wtd."SALES_PK" as sales_pk,
    cast(substring(revenue_wtd."LOCATION_CODE", 1, length(revenue_wtd."LOCATION_CODE")-2) as int) as location_code,
    entity_config.source_entity_id,
    revenue_wtd."LOCATION_NAME" as location_name,
    entity_config.concept_id as concept_id,
    entity_config.concept_name as concept_name,
    entity_config.entity_id,
    entity_config.entity_name,
    cast(revenue_wtd."SALES_DATE" as date) as sales_date,
    cast(extract(year from revenue_wtd."SALES_DATE") as int) as year,
    cast(extract(month from revenue_wtd."SALES_DATE") as int) as month,
    extract(day from revenue_wtd."SALES_DATE") as day,
    sum(revenue_wtd."TOT_TOTAL_NET_SALES") as net_sales,
    sum(revenue_wtd."TOT_COMPLIMENTARY") as discounts,
    sum(revenue_wtd."TOT_TOTAL_NET_SALES") + sum(revenue_wtd."TOT_COMPLIMENTARY") as gross_sales,
    sum(revenue_wtd."CONSUMPTION_VALUE") as cogs
from {{source('haptiq','revenue_wtd')}} revenue_wtd
left join {{source('haptiq', 'entity_config')}} entity_config
    on cast(substring(revenue_wtd."LOCATION_CODE", 1, length(revenue_wtd."LOCATION_CODE")-2) as int)  = cast(entity_config.source_entity_id as int)
where revenue_wtd.is_deleted = FALSE
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by location_code, sales_date
),

profit_and_loss AS (
    SELECT * from {{ ref('haptiq_profit_and_loss') }}
),

monthly_budget_revenue as (
    select
        period,
        month,
        year,
        concept_id,
        concept_name,
        entity_id,
        entity_name,
        sum(budget) as revenue_budget_mtd
    from profit_and_loss
    where metric_name = 'Revenue'
    group by period, month, year, entity_id, entity_name, concept_id, concept_name
),

monthly_budget_cogs as (
    select
        period,
        month,
        year,
        concept_id,
        concept_name,
        entity_id,
        entity_name,
        sum(budget) as cogs_budget_mtd
    from profit_and_loss
    where metric_name = 'Cost of Goods Sold'
    group by period, month, year, entity_id, entity_name, concept_id, concept_name
),

monthly_budget_gross_profit as (
    select
        period,
        month,
        year,
        concept_id,
        concept_name,
        entity_id,
        entity_name,
        sum(budget) as gross_profit_budget_mtd
    from profit_and_loss
    where metric_name = 'Gross Profit'
    group by period, month, year, entity_id, entity_name, concept_id, concept_name
),

budget_revenue_wtd_mtd_ytd as(
select
    mb.period,
    mb.month,
    mb.year,
    mb.concept_id,
    mb.concept_name,
    mb.entity_id,
    mb.entity_name,
    mb.revenue_budget_mtd,
    mb.revenue_budget_mtd / 4.345 as revenue_budget_wtd,
    sum(mb.revenue_budget_mtd) OVER (
        PARTITION BY mb.entity_id, mb.year 
        ORDER BY cast(mb.year AS INT), cast(mb.month AS INT)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as revenue_budget_ytd
from monthly_budget_revenue mb
order by mb.entity_name, mb.year, mb.month
),

budget_cogs_wtd_mtd_ytd as(
select
    mb.period,
    mb.month,
    mb.year,
    mb.concept_id,
    mb.concept_name,
    mb.entity_id,
    mb.entity_name,
    mb.cogs_budget_mtd,
    mb.cogs_budget_mtd / 4.345 as cogs_budget_wtd,
    sum(mb.cogs_budget_mtd) OVER (
        PARTITION BY mb.entity_id, mb.year 
        ORDER BY cast(mb.year AS INT), cast(mb.month AS INT)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cogs_budget_ytd
from monthly_budget_cogs mb
order by mb.entity_name, mb.year, mb.month
),

budget_gross_cte as(
    select
        rev.period,
        rev.month,
        rev.year,
        rev.concept_id,
        rev.concept_name,
        rev.entity_id,
        rev.entity_name,
        abs(revenue_budget_mtd) - abs(cogs_budget_mtd) as  budget_mtd_gross_sales,
        abs(revenue_budget_wtd) - abs(cogs_budget_wtd) as budget_wtd_gross_sales,
        abs(revenue_budget_ytd) - abs(cogs_budget_ytd) as budget_ytd_gross_sales
    from budget_revenue_wtd_mtd_ytd rev
    left join budget_cogs_wtd_mtd_ytd cogs
    on rev.entity_id = cogs.entity_id
    and rev.month = cogs.month
    and rev.year = cogs.year
),

budget_gross_profit_cte as(
select
    gross.period,
    gross.month,
    gross.year,
    gross.concept_id,
    gross.concept_name,
    gross.entity_id,
    gross.entity_name,
    abs(budget_mtd_gross_sales) - abs(cogs_budget_mtd) as  budget_mtd_gross_profit,
    abs(budget_wtd_gross_sales) - abs(cogs_budget_wtd) as budget_wtd_gross_profit,
    abs(budget_ytd_gross_sales) - abs(cogs_budget_ytd) as budget_ytd_gross_profit
from budget_gross_cte gross
join budget_cogs_wtd_mtd_ytd cogs
on gross.entity_id = cogs.entity_id
    and gross.month = cogs.month
    and gross.year = cogs.year

),

expenses as (
    select
        period,
        lpad(split_part(split_part(period, 'P', 2), '-', 1), 2, '0') as month,
        '20' || split_part(period, '-', 2) as year,
        concept_id,
        concept_name,
        store_id as entity_id,
        store_desc as entity_name,
        sum(net_budget_amount) as aggregate_budget_expenses_mtd,
        sum(sum(net_budget_amount)) over (partition by store_id, '20' || split_part(period, '-', 2) order by lpad(split_part(split_part(period, 'P', 2), '-', 1), 2, '0') rows between unbounded preceding and current row) as aggregate_budget_expenses_ytd,
        sum(net_budget_amount)/4.345 as aggregate_budget_expenses_wtd
    from(
        select
            *
        from {{ref('haptiq_target_trial_balance')}}
        where report_id = 2000
        ) s1
    where subcategory_name != 'Cost of Goods Sold'
    group by 1, 2, 3, 4, 5, 6, 7
),

budget_net_cte as(
    select
        b.concept_id,
        b.concept_name,
        b.entity_id,
        b.entity_name,
        b.month,
        b.year,
        b.budget_wtd_gross_sales - e.aggregate_budget_expenses_wtd as budget_wtd_net_sales,
        b.budget_mtd_gross_sales - e.aggregate_budget_expenses_mtd as budget_mtd_net_sales,
        b.budget_ytd_gross_sales - e.aggregate_budget_expenses_ytd as budget_ytd_net_sales
    from budget_gross_cte b
    left join expenses e
        on b.entity_id = e.entity_id
        and b.year = e.year
        and b.month = e.month
 ),
 
 budget_combined as(
    select
        net.concept_id,
        net.concept_name,
        net.entity_id,
        net.entity_name,
        net.month,
        net.year,
        budget_wtd_net_sales,
        budget_mtd_net_sales,
        budget_ytd_net_sales,
        budget_wtd_gross_sales,
        budget_mtd_gross_sales,
        budget_ytd_gross_sales,
        budget_mtd_gross_profit,
        budget_wtd_gross_profit,
        budget_ytd_gross_profit,
        cogs_budget_mtd,
        cogs_budget_wtd,
        cogs_budget_ytd
    from budget_net_cte net
    left join budget_gross_cte gross
     on net.entity_id = gross.entity_id
        and net.month = gross.month
        and net.year = gross.year
    left join budget_gross_profit_cte gp
        on net.entity_id = gp.entity_id
        and net.month = gp.month
        and net.year = gp.year
    left join budget_cogs_wtd_mtd_ytd cogs
        on net.entity_id = cogs.entity_id
        and net.month = cogs.month
        and net.year = cogs.year
),

actual_wtd_calculation as (
select
    t1.sales_pk,
    t1.entity_id,
    t1.entity_name,
    t1.location_code,
    t1.location_name,
    t1.concept_id,
    t1.concept_name,
    t1.sales_date,
    t1.year,
    t1.month,
    t1.day,
    t1.net_sales,
    t1.discounts,
    t1.gross_sales,
    t1.cogs,
    t1.end_of_week,
    extract(day from t1.end_of_week) as end_of_week_day,
    extract(month from t1.end_of_week) as end_of_week_month,
    extract(year from t1.end_of_week) as end_of_week_year,
    sum(net_sales) over (partition by location_code, end_of_week order by sales_date) as wtd_net_sales,
    sum(discounts) over (partition by location_code, end_of_week order by sales_date) as wtd_discounts,
    sum(gross_sales) over (partition by location_code, end_of_week order by sales_date) as wtd_gross_sales,
    sum(cogs) over (partition by location_code, end_of_week order by sales_date) as wtd_cogs
from(
    select *,
        cast({{ get_end_of_week('sales_date', 'Monday', 'Sunday') }} as date) as end_of_week
    from revenue_wtd
) t1
),

actual_mtd_ytd_calculation as (
select
    sales_pk,
    location_code,
    location_name,
    entity_id,
    entity_name,
    concept_id,
    concept_name,
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
    calendar.period_end_date,
    calendar.period_end_month,
    calendar.period_end_year,
    sum(net_sales) over (partition by entity_id, period_end_year, period_end_month order by sales_date) as mtd_net_sales,
    sum(discounts) over (partition by entity_id, period_end_year, period_end_month order by sales_date) as mtd_discounts,
    sum(gross_sales) over (partition by entity_id, period_end_year, period_end_month order by sales_date) as mtd_gross_sales,
    sum(cogs) over (partition by entity_id,  period_end_year, period_end_month order by sales_date) as mtd_cogs,
    sum(net_sales) over (partition by entity_id, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_net_sales,
    sum(discounts) over (partition by entity_id, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_discounts,
    sum(gross_sales) over (partition by entity_id, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_gross_sales,
    sum(cogs) over (partition by entity_id, period_end_year order by period_end_month rows between unbounded preceding and current row) as ytd_cogs
from actual_wtd_calculation wtd
left join {{source('haptiq', 'fiscal_calendar')}} calendar
    on cast(wtd.end_of_week as varchar) = cast(calendar.week_end_date as varchar) 
),

actual_gross_cte as(
    select
        mtd_ytd.sales_pk,
        mtd_ytd.month,
        mtd_ytd.year,
        mtd_ytd.concept_id,
        mtd_ytd.concept_name,
        mtd_ytd.entity_id,
        mtd_ytd.entity_name,
        abs(mtd_gross_sales) - abs(mtd_cogs) as  actual_mtd_gross_profit,
        abs(ytd_gross_sales) - abs(ytd_cogs) as actual_ytd_gross_profit,
        abs(wtd.wtd_gross_sales) - abs(wtd.wtd_cogs) as actual_wtd_gross_profit
    from actual_mtd_ytd_calculation mtd_ytd
     left join actual_wtd_calculation wtd
        on mtd_ytd.sales_pk = wtd.sales_pk
),


actual_combined as(
    select
        mtd_ytd.sales_pk,
        mtd_ytd.entity_id,
        mtd_ytd.entity_name,
        mtd_ytd.location_code,
        mtd_ytd.location_name,
        mtd_ytd.concept_id,
        mtd_ytd.concept_name,
        mtd_ytd.sales_date,
        mtd_ytd.year,
        mtd_ytd.month,
        mtd_ytd.day,
        mtd_ytd.end_of_week,
        mtd_ytd.end_of_week_day,
        mtd_ytd.end_of_week_month,
        mtd_ytd.end_of_week_year,
        mtd_ytd.period_end_date,
        mtd_ytd.period_end_month,
        mtd_ytd.period_end_year,
        mtd_ytd.start_of_month,
        mtd_ytd.net_sales,
        mtd_ytd.discounts,
        mtd_ytd.gross_sales,
        mtd_ytd.cogs,
        mtd_ytd.mtd_net_sales,
        mtd_ytd.mtd_gross_sales,
        mtd_ytd.mtd_discounts,
        mtd_ytd.mtd_cogs,
        mtd_ytd.ytd_net_sales,
        mtd_ytd.ytd_gross_sales,
        mtd_ytd.ytd_discounts,
        mtd_ytd.ytd_cogs,
        wtd_net_sales,
        wtd_gross_sales,
        wtd_discounts,
        wtd_cogs,
        actual_mtd_gross_profit,
        actual_ytd_gross_profit,
        actual_wtd_gross_profit
    from actual_mtd_ytd_calculation mtd_ytd
    left join actual_wtd_calculation wtd
    on mtd_ytd.sales_pk = wtd.sales_pk
    left join actual_gross_cte gross
    on gross.sales_pk = mtd_ytd.sales_pk

        
),

budget_actual as (
    select
        actual.sales_pk,
        actual.concept_id,
        actual.concept_name,
        actual.entity_id,
        replace(actual.entity_name, 'PINK TACO ', '') as entity_name,
        actual.sales_date,
        actual.year,
        actual.month,
        actual.day,
        actual.end_of_week,
        actual.end_of_week_day,
        actual.end_of_week_month,
        actual.end_of_week_year,
        actual.period_end_date,
        actual.period_end_month,
        actual.period_end_year,
        actual.start_of_month,
        actual.net_sales,
        actual.gross_sales,
        actual.discounts,
        actual.cogs,
        actual.mtd_net_sales,
        actual.mtd_gross_sales,
        actual.mtd_discounts,
        actual.mtd_cogs,
        actual.ytd_net_sales,
        actual.ytd_gross_sales,
        actual.ytd_discounts,
        actual.ytd_cogs,
        actual.wtd_net_sales,
        actual.wtd_gross_sales,
        actual.wtd_discounts,
        actual.wtd_cogs,
        actual_mtd_gross_profit,
        actual_ytd_gross_profit,
        actual_wtd_gross_profit,
        budget_wtd_net_sales,
        budget_mtd_net_sales,
        budget_ytd_net_sales,
        budget_wtd_gross_sales,
        budget_mtd_gross_sales,
        budget_ytd_gross_sales,
        budget_mtd_gross_profit,
        budget_wtd_gross_profit,
        budget_ytd_gross_profit,
        cogs_budget_mtd,
        cogs_budget_wtd,
        cogs_budget_ytd
    from actual_combined actual
    left join budget_combined budget
        on cast(actual.entity_id as numeric) = cast(budget.entity_id as numeric)
        and cast(actual.month as int) = cast(budget.month as int)
        and cast(actual.year as int) = cast(budget.year as int)
)

-- final select
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
    t2.wtd_net_sales as wtd_net_sales_py,
    t2.wtd_discounts as wtd_discounts_py,
    t2.wtd_gross_sales as wtd_gross_sales_py,
    t2.wtd_cogs as wtd_cogs_py,
    t2.actual_mtd_gross_profit as actual_mtd_gross_profit_py,
    t2.actual_ytd_gross_profit as actual_ytd_gross_profit_py,
    t2.actual_wtd_gross_profit as actual_wtd_gross_profit_py,
    t2.budget_wtd_net_sales as budget_wtd_net_sales_py,
    t2.budget_mtd_net_sales as budget_mtd_net_sales_py,
    t2.budget_ytd_net_sales as budget_ytd_net_sales_py,
    t2.budget_wtd_gross_sales as budget_wtd_gross_sales_py,
    t2.budget_mtd_gross_sales as budget_mtd_gross_sales_py,
    t2.budget_ytd_gross_sales as budget_ytd_gross_sales_py,
    t2.budget_mtd_gross_profit as budget_mtd_gross_profit_py,
    t2.budget_wtd_gross_profit as budget_wtd_gross_profit_py ,
    t2.budget_ytd_gross_profit as budget_ytd_gross_profit_py,
    t2.cogs_budget_mtd as cogs_budget_mtd_py,
    t2.cogs_budget_wtd as cogs_budget_wtd_py,
    t2.cogs_budget_ytd as cogs_budget_ytd_py
from budget_actual t1
left join budget_actual t2
on t1.entity_id = t2.entity_id
    and t1.concept_id = t2.concept_id
    and t1.month = t2.month
    and t1.day = t2.day
    and t1.year = t2.year + 1
