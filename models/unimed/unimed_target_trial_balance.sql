
with source_trial_balance as (
    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_usa_staged') }} tb

    union all

        select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_tm_staged') }} tb

    union all

    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_uk_staged') }} tb

    union all

    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_aus_staged') }} tb
    
    union all

    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_gr_staged') }} tb

    union all

    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_sg_staged') }} tb
    
    union all

    select
		cast("gl_code_id" as int4) as gl_number,
		"gl_code_name" as gl_name,
		"country_number" as country_id,
		"ref_date" as date,
		"cat_id" as category_id,
		"subcat_id" as subcategory_id,
		"group_id" as group_id,
		"debit" as debit,
		"credit" as credit
    from
        {{ ref('unimed_trial_balances_davie_staged') }} tb
),

report_config as (
    select
        company_id,
        company_name,
        report_id,
        report_name,
        category_id,
        subcategory_id,
        category_name,
        subcategory_name,
        sub_subgroup_id,
        sub_subgroup,
        gl_number,
        gl_name
    from
        {{ source('unimed', 'report_config') }} re
),

entity_config as(
    select
        en.entity_id,
        en.entity_name,
        co.country_id,
        co.country_name
    from {{ source('unimed', 'entity_config_data') }} en
    left join {{ source('public', 'country_config_data') }} co on
        en.country_id = co.country_id
),

metric_config as (
    select
        report_id,
        metric_id,
        metric_name,
        metric_order,
        submetric_order,
        subgroup_order,
        type
    from
      {{ source('public', 'report_metric_config') }}  
),

target_trial_balance_src as (
	 select
        rc.company_id,
        src.date,
		en.country_id,
        en.country_name,
        rc.report_id,
        rc.report_name,
        rc.category_id,
        rc.category_name,
        rc.subcategory_id,
        rc.subcategory_name as subcategory_name,
        src.gl_name,
        src.gl_number,
        rc.sub_subgroup_id,
        rc.sub_subgroup,
        src.debit,
        src.credit,
        sum(debit - credit) over (partition by src.gl_number, rc.report_id, en.country_id order by src.date asc) as cumulative_balance,
        mc_category.metric_order,
        mc_subcategory.submetric_order,
        mc_subgroup.subgroup_order  
    from
        source_trial_balance src
    left join  report_config rc on
        src.gl_number = rc.gl_number
    left join entity_config en on
        src.country_id = en.country_id
    left join metric_config mc_category on
        mc_category.metric_id = rc.category_id 
        and mc_category.type = 'metric' and mc_category.report_id = rc.report_id
    left join metric_config mc_subcategory on
        mc_subcategory.metric_id = rc.subcategory_id and mc_subcategory.type = 'sub_metric'
	    and mc_subcategory.report_id = rc.report_id
    left join metric_config mc_subgroup on
        mc_subgroup.metric_id = rc.sub_subgroup_id and mc_subgroup.type = 'sub_group'
	    and mc_subgroup.report_id = rc.report_id
    order by mc_category.metric_order,
        mc_subcategory.submetric_order,
        mc_subgroup.subgroup_order  
),

target_trial_balance as (
	select 
		company_id,
        date,
		country_id,
        country_name,
        report_id,
        report_name,
        category_id,
        category_name,
        subcategory_id,
        subcategory_name as subcategory_name,
        gl_name,
        gl_number,
        sub_subgroup_id,
        sub_subgroup,
        debit,
        credit,
        case 
	        when (extract(month from date) = 12 and extract(day from date) = 31) and report_id = 2000 then lag(cumulative_balance) over (partition by gl_number, report_id, country_id order by date asc)
	        when (extract(month from date) = 12 and extract(day from date) = 31) and report_id = 3000 then lag(cumulative_balance) over (partition by gl_number, report_id, country_id order by date asc)
            when (extract(month from date) = 12 and extract(day from date) = 31) and (report_id = 4000 and (category_id = 2 or category_id = 18)) then lag(cumulative_balance) over (partition by gl_number, report_id, country_id order by date asc)
            else cumulative_balance
        end as ending_balance,
        metric_order,
        submetric_order,
        subgroup_order,
        'connector' as source
	from
		target_trial_balance_src
    order by
        country_id, report_id, gl_number, date
),

target_trial_balance_final AS (
	select * from target_trial_balance
	
	union all
	
	select * from {{ ref('unimed_target_trial_balance_manual') }}
)

select * from target_trial_balance_final
