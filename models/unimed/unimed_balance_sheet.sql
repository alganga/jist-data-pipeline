with balance_sheet as (
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
                CONCAT(CONCAT('P', CAST(EXTRACT(MONTH FROM date) AS VARCHAR)), CONCAT('-', RIGHT(CAST(EXTRACT(YEAR FROM date) AS VARCHAR), 2))) AS period,
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
                row_number() over (partition by gl_number, country_id, extract(year from date::date), extract(month from date::date) order by date desc) as row_num
            from 
                {{ref('unimed_target_trial_balance')}}
            where report_id = 1000
            )t0
        where t0.row_num = 1
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
),
net_income as (
    select
        period,
        year,
        month,
        country_id,
        country_name,
        pnl.category_name,
        bs.metric_order,
        pnl.subcategory_name,
        bs.submetric_order,
        sub_subgroup,
        subgroup_order, 
        gl_number,
        gl_name,
        CASE
            WHEN month = 12 THEN 0
            ELSE ending_balance
        END AS ending_balance
    from
        (
            select 
                period,
                year, 
                month,
                country_id,
                country_name,
                'Owners Equity' as category_name,
                'Equity' as subcategory_name,
                cast(null as int) as gl_number,
                'Net Income' as gl_name,
                'Net Income' as sub_subgroup,
                (case when sum(actual_ytd) = 0 then 0
                    else -1 * sum(actual_ytd)
                    end) as ending_balance,
                3::int as subgroup_order
            from {{ref('unimed_profit_and_loss')}}
            where metric_name ilike '%net%'
            group by period, year, month, country_id, country_name
        )pnl
        left join
        (
            select distinct
                category_name,
                metric_order,
                subcategory_name,
                submetric_order
            from balance_sheet
            where category_name = 'Owners Equity'
            and subcategory_name = 'Equity'
        )bs
    on pnl.category_name = bs.category_name and pnl.subcategory_name = bs.subcategory_name
)

select * from net_income
union all 
select * from balance_sheet
