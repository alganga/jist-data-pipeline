
with source_trial_balance as (
    select
        "SEGMENT4" as gl_number,
        "ACCOUNT_DESC" as gl_name,
        "LEDGER_ID" as ledger_id,
        "STORE_DESC" as store_desc,
        "SEGMENT2" as store_id,
        "SEGMENT1" as legal_entity_id,
        "LEGAL_ENTITY_DESC" as legal_entity_desc,
        "P_PERIOD" as period,
        sum("BEGINNING_BAL") as beginning_balance,
        sum("ENDING_BAL") as ending_balance,
        sum("NET_BUDGET_AMOUNT") as net_budget_amount,
        sum("PERIOD_NET_DR") as period_net_dr,
        sum("PERIOD_NET_CR") as period_net_cr
    from
        {{ source('haptiq', 'trial_balances') }}
    where "SEGMENT4" is not null
    and is_deleted = FALSE
    group by 1, 2, 3, 4, 5, 6, 7, 8
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
        {{ source('haptiq', 'report_config') }} re
),


entity_config as (
    select
        en.company_id,
        en.company_name,
        source_entity_id,
        entity_id,
        entity_name,
        co.country_id,
        co.country_name,
        cc.concept_id,
        cc.concept,
        cc.concept_name
    from
       {{ source('haptiq', 'entity_config') }} en
    left join {{ source('public', 'country_config_data') }} co
        on en.country_id = co.country_id
    left join {{ source('haptiq', 'concept_config') }} cc
        on en.concept_id = cc.concept_id
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

target_trial_balance as (
 select
        rc.company_id,
        src.period,
        src.store_id,
        src.store_desc,
        ent.entity_id,
        ent.entity_name,
        ent.concept_id,
        ent.concept,
        ent.concept_name,
        ent.country_id,
        ent.country_name,
        rc.report_id,
        rc.report_name,
        rc.category_id,
        rc.category_name,
        rc.subcategory_id,
        rc.subcategory_name as subcategory_name,
        rc.gl_name,
        rc.gl_number,
        rc.sub_subgroup_id,
        rc.sub_subgroup,
        src.beginning_balance,
        src.ending_balance,
        src.net_budget_amount,
        src.period_net_dr,
        src.period_net_cr,
        mc_category.metric_order,
        mc_subcategory.submetric_order,
        mc_subgroup.subgroup_order  
    from
        source_trial_balance src
    left join entity_config ent on
        src.store_id = ent.source_entity_id
    left join  report_config rc on
        src.gl_number = rc.gl_number
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
)
select * from target_trial_balance