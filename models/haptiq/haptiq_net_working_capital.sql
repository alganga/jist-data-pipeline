 
-- CTE 'nwc': Aggregates necessary financial data from the trial balance, 
-- including ending and beginning balances, and computes the month and year.
with nwc as (
    select 
        period,
        category_name,
        metric_order,
        subcategory_name,
        submetric_order,
        gl_name,
        gl_number,
        sum(ending_balance) as ending_balance,
        sum(beginning_balance) as beginning_balance,
        sum(period_net_dr) as period_net_dr,
        sum(period_net_cr) as period_net_cr,
        lpad(split_part(split_part(period, 'P', 2), '-', 1), 2, '0') as month,
        '20' || split_part(period, '-', 2) as year
    from {{ ref('haptiq_target_trial_balance') }}
    where report_id = 4000
    group by 1, 2, 3, 4, 5, 6, 7
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
            category_name,
            metric_order,
            subcategory_name,
            submetric_order,
            sum(ending_balance) as ending_balance,
             {{ get_days_in_month('year', 'month') }} as days_in_month
        from nwc
        where subcategory_name = 'Inventories' or subcategory_name = 'Trade Receivables'
        or subcategory_name = 'Accounts Payable'
        group by 1, 2, 3, 4, 5, 6, 7
    ) t1
),

-- CTE 'aggregate_cogs': Aggregates the total Cost of Goods Sold (COGS) for each period.
aggregate_cogs as (
    select
        period,
        month,
        year,
        category_name,
        sum(ending_balance) as ending_balance
    from nwc
    where category_name = 'Cost of Goods Sold'
    group by 1, 2, 3, 4
),

-- CTE 'aggregate_sales': Aggregates total sales for each period.
aggregate_sales as (
    select
        period,
        month,
        year,
        category_name,
        sum(ending_balance) - sum(beginning_balance) as sales
    from nwc
    where category_name = 'Revenue'
    group by 1, 2, 3, 4
),

-- CTE 'dio_calculation': Calculates the Days Inventory Outstanding (DIO) for each period.
dio_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.days_in_month,
        'DIO' as category_name,
        (sum(a.period_average)/nullif(sum(b.ending_balance), 0))*days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_cogs b
    on a.period = b.period
    where a.subcategory_name = 'Inventories'
    group by 1, 2, 3, 4, 5
),

-- CTE 'dso_calculation': Calculates the Days Sales Outstanding (DSO) for each period.
dso_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.days_in_month,
        'DSO' as category_name,
        (sum(a.period_average)/nullif(sum(b.sales), 0))*days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_sales b
    on a.period = b.period
    where a.subcategory_name = 'Trade Receivables'
    group by 1, 2, 3, 4, 5
),

-- CTE 'dpo_calculation': Calculates the Days Payable Outstanding (DPO) for each period.
dpo_calculation as (
    select
        a.period,
        a.month,
        a.year,
        a.days_in_month,
        'DPO' as category_name,
        (sum(a.period_average)/nullif(sum(b.ending_balance), 0))*days_in_month as ending_balance
    from average_inventory_payable_receivable a
    left join aggregate_cogs b
    on a.period = b.period
    where a.subcategory_name = 'Accounts Payable'
    group by 1, 2, 3, 4, 5
),

-- CTE 'aggregate_current_assets': Aggregates current assets and liabilities for each period.
aggregate_current_assets as (
    select 
        period,
        month,
        year,
        subcategory_name,
        sum(ending_balance) as ending_balance
    from {{ ref('haptiq_balance_sheet') }}
    where subcategory_name = 'Current Assets'
    or subcategory_name = 'Current Liabilities'
    group by 1, 2, 3, 4
),

-- CTE 'nwc_calculation': Calculates Net Working Capital (NWC) for each period.
nwc_calculation as (
    select
        a.period,
        a.month,
        a.year,
        'NWC' as category_name,
        abs(sum(a.ending_balance)) - abs(sum(b.ending_balance)) as ending_balance
    from aggregate_current_assets a
    left join aggregate_current_assets b
    on a.period = b.period and b.subcategory_name = 'Current Liabilities'
    where a.subcategory_name = 'Current Assets'
    group by 1, 2, 3, 4
),

-- CTE 'revenue_ebitda': Aggregates revenue and EBITDA for each period.
revenue_ebitda as (
    select 
        period,
        month,
        year,
        metric_name as category_name,
        metric_order as category_metric_order,
        sum(actual_mtd) as ending_balance
    from  {{ ref('haptiq_profit_and_loss') }}
    where metric_name = 'Revenue' or metric_name = 'EBITDA'
    group by period, month, year, category_name, category_metric_order
)

-- Final SELECT statement: Combines data from NWC calculation, revenue/EBITDA, DIO, and DSO calculations.
select
    period,
    month,
    year,
    nwc_calculation.category_name,
    mm.metric_order,
	null as subcategory_name,
    cast(null as int) submetric_order,
    null as gl_name,
    cast(null as int) gl_number,
    ending_balance
from nwc_calculation
left join metric_order mm
on nwc_calculation.category_name = mm.metric_name


union all

select 
    period,
    month,
    year,
    category_name,
    (case 
        when category_name = 'Revenue' then category_metric_order
        else mm.metric_order
        end) as metric_order,
	null as subcategory_name,
    cast(null as int) submetric_order,
    null as gl_name,
    cast(null as int) gl_number,
    ending_balance
from revenue_ebitda re
left join metric_order mm
on re.category_name = mm.metric_name


union all

select
    period,
    month,
    year,
    dio_calculation.category_name,
	mm.metric_order,
	null as subcategory_name,
	cast(null as int) submetric_order,
    null as gl_name,
    cast(null as int) gl_number,
    abs(ending_balance)
from dio_calculation
left join metric_order mm
on dio_calculation.category_name = mm.metric_name


union all

select
    period,
    month,
    year,
    dso_calculation.category_name,
    mm.metric_order,
	null as subcategory_name,
    cast(null as int) submetric_order,
    null as gl_name,
    cast(null as int) gl_number,
    abs(ending_balance)
from dso_calculation
left join metric_order mm
on dso_calculation.category_name = mm.metric_name


union all

select
    period,
    month,
    year,
    dpo_calculation.category_name,
    mm.metric_order,
	null as subcategory_name,
    cast(null as int) submetric_order,
    null as gl_name,
    cast(null as int) gl_number,
    abs(ending_balance)
from dpo_calculation
left join metric_order mm
on dpo_calculation.category_name = mm.metric_name


union all

select
    period,
    month,
    year,
    category_name,
    metric_order,
    subcategory_name,
    submetric_order,
    gl_name,
    gl_number,
    ending_balance
from nwc
where (category_name = 'Current Assets' or category_name = 'Current Liabilities')

