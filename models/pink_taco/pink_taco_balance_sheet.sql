with balance_sheet as (
        select
        period,
        '20' || split_part(period, '-', 2) as year,
        lpad(split_part(split_part(period, 'P', 2), '-', 1), 2, '0') as month,
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
        
        sum(ending_balance) as ending_balance
    from {{ref('pink_taco_target_trial_balance')}}
    where report_id = 1000
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11
),


net_income as(
    select
        period,
        year,
        month,
        pnl.category_name,
        bs.metric_order,
        pnl.subcategory_name,
        bs.submetric_order,
        sub_subgroup,
        subgroup_order, 
        gl_number,
        gl_name,
        ending_balance
        
    from(
        select 
            period,
            year, 
            month,
            'Owners Equity' as category_name,
            'Equity' as subcategory_name,
            cast(null as int) as gl_number,
            'Net Income' as gl_name,
            'Net Income' as sub_subgroup,
			- 1 * sum(actual_ytd) as ending_balance,
            3::int as subgroup_order
        from {{ref('pink_taco_profit_and_loss')}}
        where metric_name ilike '%net%'
        group by period, year, month 
    ) pnl
    left join (
        select distinct
            category_name,
            metric_order,
            subcategory_name,
            submetric_order
        from balance_sheet
        where category_name = 'Owners Equity'
        and subcategory_name = 'Equity'
    ) bs
    on pnl.category_name = bs.category_name
    and pnl.subcategory_name = bs.subcategory_name

    
)

select * from net_income

union all 

select * from balance_sheet
