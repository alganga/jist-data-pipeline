with public_equities as (
    select *
    from {{source('xrg', 'public_equities')}}
    where deleted_at is NULL
),


brands as ( 
   select
        cast(pnl.month as int) as month,
        cast(pnl.year as int) as year,
        extract(
            day from TO_DATE(
                pnl.month::text||'-'||pnl.year::text,
                'MM-YYYY'
            ) + INTERVAL '1 month'
            - INTERVAL '1 day') as day,
        TO_DATE(
            pnl.month::text||'-'||pnl.year::text,
            'MM-YYYY'
        ) + INTERVAL '1 month'
        - INTERVAL '1 day' as date,
        'Q'||(pnl.month::int-1)/3+1 as quarter,
         CASE
            WHEN metric_name = 'EBITDA' THEN 2
            ELSE pnl.metric_order
        END AS metric_order,
        pnl.metric_name as category_name,
        cast(sum(actual_ttm) as numeric) as brands
    from {{ref('xrg_profit_and_loss')}} pnl
    where (metric_name = 'Revenue'
    or metric_name = 'EBITDA')
        and period in (
    	select 
            distinct 'P'||substring(quarter, 2)::int*3||'-'||substring(year::text, 3)
        from public_equities
    )
    group by 1, 2, 3, 4, 5, 6, 7
  
)

select 
    year,
    month,
    day,
    date,
    quarter,
    category_name as metric,
    metric_order,
    'XRG Brands' as comparison_company,
    brands as value,
    current_date as created_at,
    current_timestamp as updated_at
from brands

union all

select
    year,
    month,
    day,
    date::date,
    quarter,
    metric,
    metric_order,
    comparison_company,
    value,
    created_at,
    updated_at
from public_equities
