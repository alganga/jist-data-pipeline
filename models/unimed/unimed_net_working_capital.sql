with nwc_without_begin as (
        select 
            t0.period,
            t0.year,
            t0.month,
            t0.country_id,
            t0.country_name,
            t0.category_name,
            t0.metric_order,
            t0.subcategory_name,
            t0.submetric_order,
            t0.sub_subgroup,
            t0.subgroup_order,
            t0.gl_number,
            t0.gl_name,
            t0.ending_balance
        from
            (
            select
                'P'||extract(month from date)::varchar||'-'||to_char(date, 'YY') as period,
                extract(year from date) as year,
                extract(month from date) as month,
                country_id,
                country_name,
                category_name,
                metric_order, 
                (case when subcategory_name is null then category_name
                    else subcategory_name
                    end) as subcategory_name,
                submetric_order,
                sub_subgroup,
                subgroup_order, 
                gl_number,
                gl_name,
                ending_balance,
                row_number() over (partition by gl_number, extract(year from date::date), extract(month from date::date) order by date desc) as row_num
            from 
                {{ref('unimed_target_trial_balance')}}
            where report_id = 4000
            )t0
        where t0.row_num = 1
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
),
nwc as (
    select 
        period,
        year,
        month,
        country_id,
        country_name,
        category_name,
        metric_order,
        subcategory_name,
        submetric_order,
        sub_subgroup,
        subgroup_order,
        gl_number,
        gl_name,
        coalesce(lag(ending_balance, 1) over (partition by gl_number order by year::int, month::int), 0) as beginning_balance,
        ending_balance
    from
        nwc_without_begin
),
metric_order AS (
    select * from {{ source('public', 'report_metric_config') }} where report_id = 4000
),
-- CTE 'average_inventory_payable_receivable': Calculates the average daily balance for
-- inventories, accounts payable, and accounts receivable.
average_inventory_payable_receivable as (
    select
        period,
        month,
        year,
        country_id,
        country_name,
        category_name,
        metric_order,
        subcategory_name,
        submetric_order,
        ending_balance/days_in_month as period_average,
        days_in_month
    from (
        select
            period,
            month,
            year,
            country_id,
            country_name,
            subcategory_name as category_name,
            submetric_order as metric_order,
            sub_subgroup as subcategory_name,
            subgroup_order as submetric_order,
            ending_balance,
            {{ get_days_in_month('year', 'month') }} as days_in_month
        from nwc
        where subcategory_name = 'Inventories' or subcategory_name = 'Trade Receivables'
        or subcategory_name = 'Trade Payables'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ) t1
),
-- CTE 'aggregate_cogs': Aggregates the total Cost of Goods Sold (COGS) for each period.
aggregate_cogs as (
    select
        period,
        month,
        year,
        country_id,
        country_name,
        category_name,
        ending_balance
    from nwc
    where category_name = 'Cost of Goods Sold'
    group by 1, 2, 3, 4, 5, 6, 7
),
-- CTE 'aggregate_sales': Aggregates total sales for each period.
aggregate_sales as (
    select
        period,
        month,
        year,
        country_id,
        country_name,
        category_name,
        ending_balance - beginning_balance as sales
    from nwc
    where category_name = 'Revenue'
    group by 1, 2, 3, 4, 5, 6, 7
),
-- CTE 'dio_calculation': Calculates the Days Inventory Outstanding (DIO) for each period.
dio_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.country_id,
        a.country_name,
        a.days_in_month,
        'DIO' as category_name,
        (sum(a.period_average)/nullif(sum(b.ending_balance), 0))*a.days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_cogs b
    on a.period = b.period
    where a.category_name = 'Inventories'
    group by 1, 2, 3, 4, 5, 6, 7
),
-- CTE 'dso_calculation': Calculates the Days Sales Outstanding (DSO) for each period.
dso_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.country_id,
        a.country_name,
        a.days_in_month,
        'DSO' as category_name,
        (sum(a.period_average)/nullif(sum(b.sales), 0))*a.days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_sales b
    on a.period = b.period
    where a.category_name = 'Trade Receivables'
    group by 1, 2, 3, 4, 5, 6, 7
),
-- CTE 'dpo_calculation': Calculates the Days Payable Outstanding (DPO) for each period.
dpo_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.country_id,
        a.country_name,
        a.days_in_month,
        'DPO' as category_name,
        (sum(a.period_average)/nullif(sum(b.ending_balance), 0))*a.days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_cogs b
    on a.period = b.period
    where a.category_name = 'Trade Payables'
    group by 1, 2, 3, 4, 5, 6, 7
),
-- CTE 'aggregate_current_assets': Aggregates current assets and liabilities for each period.
aggregate_current_assets as (
    select 
        period,
        month,
        year,
        country_id,
        country_name,
        subcategory_name,
        sum(ending_balance) as ending_balance
    from {{ ref('unimed_balance_sheet') }}
    where subcategory_name = 'Current Assets'
    or subcategory_name = 'Current Liabilities'
    group by 1, 2, 3, 4, 5, 6
),
-- CTE 'nwc_calculation': Calculates Net Working Capital (NWC) for each period.
nwc_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.country_id,
        a.country_name,
        'NWC' as category_name,
        abs(sum(a.ending_balance)) - abs(sum(b.ending_balance)) as ending_balance
    from (select * from aggregate_current_assets where subcategory_name = 'Current Assets') a
    left join (select * from aggregate_current_assets where subcategory_name = 'Current Liabilities') b
    on a.period = b.period and a.country_id = b.country_id
    group by 1, 2, 3, 4, 5, 6
),
-- CTE 'revenue_ebitda': Aggregates revenue and EBITDA for each period.
revenue_ebitda as (
    select 
        period,
        month,
        year,
        metric_name as category_name,
        metric_order as category_metric_order,
        country_name,
        country_id,
        sum(ending_balance) as ending_balance
    from  {{ ref('unimed_profit_and_loss') }}
    where metric_name = 'Revenue' or metric_name = 'EBITDA'
    group by period, month, year, category_name, category_metric_order, country_id, country_name
),

-- Final SELECT statement: Combines data from NWC calculation, revenue/EBITDA, DIO, and DSO calculations.
-- NWC
nwc_fin as(
    -- NWC
    select
        period,
        month,
        year,
        country_name,
        country_id,
        nwc_calculation.category_name,
        5 as metric_order,
        null as subcategory_name,
        cast(null as int) submetric_order,
        null as gl_name,
        cast(null as int) gl_number,
        ending_balance
    from nwc_calculation


    union all

    -- COGS, EBITDA and Revenue
    select 
        period,
        month,
        year,
        country_name,
        country_id,
        re.category_name,
        case
            when re.category_name = 'EBITDA' then 2
            when re.category_name = 'Revenue' then 1
            else 18
        end as metric_order,
        null as subcategory_name,
        cast(null as int) submetric_order,
        null as gl_name,
        cast(null as int) gl_number,
        ending_balance
    from revenue_ebitda re

    union all

    -- DIO
    select
        period,
        month,
        year,
        country_name,
        country_id,
        dio_calculation.category_name,
        7 as metric_order,
        null as subcategory_name,
        cast(null as int) submetric_order,
        null as gl_name,
        cast(null as int) gl_number,
        abs(ending_balance)
    from dio_calculation

    union all

    -- DSO
    select
        period,
        month,
        year,
        country_name,
        country_id,
        dso_calculation.category_name,
        6 as metric_order,
        null as subcategory_name,
        cast(null as int) submetric_order,
        null as gl_name,
        cast(null as int) gl_number,
        abs(ending_balance)
    from dso_calculation

    union all

    -- DPO
    select
        period,
        month,
        year,
        country_name,
        country_id,
        dpo_calculation.category_name,
        8 as metric_order,
        null as subcategory_name,
        cast(null as int) submetric_order,
        null as gl_name,
        cast(null as int) gl_number,
        abs(ending_balance)
    from dpo_calculation

    union all
    -- Current Assets and Current Liabilities
    select
        period,
        month,
        year,
        country_name,
        country_id,
        category_name,
        case
            when nwc.category_name = 'Current Assets' then 3
            when nwc.category_name = 'Current Liabilities' then 4
        end as metric_order,
        nwc.subcategory_name as subcategory_name,
        case
            when nwc.subcategory_name = 'Cash and Cash Equivalents' or nwc.subcategory_name = 'Trade Payables' then 1
            when nwc.subcategory_name = 'Inventories' or nwc.subcategory_name = 'Accrued Liabilities' then 2
            when nwc.subcategory_name = 'Prepayments and Other Receivables' or nwc.subcategory_name = 'Other Current Liability' then 3
            else 4
        end as submetric_order,
        nwc.gl_name,
        nwc.gl_number,
        nwc.ending_balance
    from nwc
    where nwc.category_name = 'Current Assets' or nwc.category_name = 'Current Liabilities'
)

select * from nwc_fin
order by country_id, metric_order, submetric_order, gl_number, year, month