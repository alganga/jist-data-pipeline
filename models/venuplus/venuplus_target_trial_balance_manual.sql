with report_config AS (
    SELECT
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
        signage,
        absolute_flag,
        gl_number::text,
        gl_name
    FROM
        {{source('venuplus', 'report_config')}}
),

entity_config AS (
    SELECT
        en.company_id,
        en.company_name,
        source_entity_id,
        entity_id,
        entity_name,
        co.country_id,
        co.country_name
    FROM
       {{source('venuplus', 'entity_config')}} en
    LEFT JOIN {{source('public', 'country_config_data')}} co
        ON en.country_id = co.country_id
),

metric_config AS (
    SELECT
        report_id,
        metric_id,
        metric_name,
        metric_order,
        submetric_order,
        subgroup_order,
        type
    FROM
      {{source('public', 'report_metric_config')}} rmc 
),

source_trial_balance_manual AS (
    {% if adapter.get_relation(database=None, schema='venuplus', identifier='trial_balances_manual') is not none %}
        SELECT
            "entity_name" AS entity_name,
            "gl_account_number"::text AS gl_number,
            "year" AS year,
            "period" AS month,
            CONCAT(CONCAT('P', CAST("period" AS VARCHAR)), CONCAT('-', RIGHT(CAST("year" AS VARCHAR), 2))) AS period,
            SUM("period_ending_balance") AS ending_balance,
            SUM("budget_amt") AS net_budget_amount
        FROM
            {{source('venuplus', 'trial_balances_manual')}} tbm
        WHERE is_deleted IS FALSE
        GROUP BY entity_name, gl_number, year, month, period
    {% else %}
        select 
            null as entity_name,
            null as gl_number,
            null as year,
            null as month,
            null as period,
            null::numeric as ending_balance,
            null::numeric as net_budget_amount
        where false
    {% endif %}
),

target_trial_balance_manual AS (
    SELECT
        T1.*,
        CASE   -- When the category is a revenue or expense account, the beginning balance is 0 in January
            WHEN category_id in (19, 25, 52, 23, 2, 5, 18, 24) AND month::int = 1 THEN 0
            ELSE COALESCE(LAG(ending_balance) OVER (PARTITION BY entity_id, report_id, gl_number ORDER BY year::int, month::int), 0)
        END AS beginning_balance
    FROM(
        SELECT
            rc.company_id,
            ent.source_entity_id,
            ent.entity_name,
            ent.entity_id,
            ent.country_id,
            ent.country_name,
            rc.report_id,
            rc.report_name,
            rc.gl_number,
            rc.gl_name,
            rc.category_id,
            rc.category_name,
            rc.subcategory_id,
            rc.subcategory_name,
            rc.sub_subgroup_id,
            rc.sub_subgroup,
            rc.signage,
            rc.absolute_flag,
            mc_category.metric_order,
            mc_subcategory.submetric_order,
            mc_subgroup.subgroup_order,
            man.year,
            NULL::int AS year_code,
            man.month,
            man.period,
            'USD' AS currency,
            NULL::numeric AS period_net_dr,
            NULL::numeric AS period_net_cr,
            SUM(man.ending_balance) AS ending_balance,
            SUM(man.net_budget_amount) AS net_budget_amount
        FROM source_trial_balance_manual man
        LEFT JOIN entity_config ent
            ON LOWER(man.entity_name) = LOWER(ent.entity_name)
        JOIN report_config rc 
            ON CASE 
                WHEN RIGHT(man.gl_number::text, 1) ~ '[A-Za-z]' THEN LEFT(man.gl_number::text, LENGTH(man.gl_number::text) - 1)
                ELSE man.gl_number::text
            END = rc.gl_number::text
        LEFT JOIN metric_config mc_category 
            ON mc_category.metric_id = rc.category_id 
            AND mc_category.type = 'metric' 
            AND mc_category.report_id = rc.report_id
        LEFT JOIN metric_config mc_subcategory 
            ON mc_subcategory.metric_id = rc.subcategory_id 
            AND mc_subcategory.type = 'sub_metric'
            AND mc_subcategory.report_id = rc.report_id 
        LEFT JOIN metric_config mc_subgroup 
            ON mc_subgroup.metric_id = rc.sub_subgroup_id 
            AND mc_subgroup.type = 'sub_group'
            AND mc_subgroup.report_id = rc.report_id
        GROUP BY
            rc.company_id, ent.source_entity_id, ent.entity_name, ent.entity_id, ent.country_id, ent.country_name,
            rc.report_id, rc.report_name, rc.gl_number, rc.gl_name, rc.category_id, rc.category_name, rc.subcategory_id,
            rc.subcategory_name, rc.sub_subgroup_id, rc.sub_subgroup, rc.signage, rc.absolute_flag, mc_category.metric_order,
            mc_subcategory.submetric_order, mc_subgroup.subgroup_order, man.year, year_code, man.month, man.period, currency,
            period_net_dr, period_net_cr
    ) T1
),

target_trial_balance_manual_type AS (
    SELECT
        company_id,
        source_entity_id,
        entity_name,
        entity_id,
        country_id,
        country_name,
        report_id,
        report_name,
        gl_number,
        gl_name,
        category_id,
        category_name,
        subcategory_id,
        subcategory_name,
        sub_subgroup_id,
        sub_subgroup,
        signage,
        absolute_flag,
        metric_order,
        submetric_order,
        subgroup_order,
        year::text,
        year_code,
        month::text,
        period,
        currency,
        'balance' AS type,
        beginning_balance,
        ending_balance,
        NULL::numeric AS net_budget_amount,
        period_net_dr,
        period_net_cr,
        'manual' AS source
    FROM target_trial_balance_manual
    
    UNION ALL
    
    SELECT
        company_id,
        source_entity_id,
        entity_name,
        entity_id,
        country_id,
        country_name,
        report_id,
        report_name,
        gl_number,
        gl_name,
        category_id,
        category_name,
        subcategory_id,
        subcategory_name,
        sub_subgroup_id,
        sub_subgroup,
        signage,
        absolute_flag,
        metric_order,
        submetric_order,
        subgroup_order,
        year::text,
        year_code,
        month::text,
        period,
        currency,
        'budget' AS type,
        NULL::numeric AS beginning_balance,
        NULL::numeric AS ending_balance, 
        net_budget_amount,
        period_net_dr,
        period_net_cr,
        'manual' AS source
    FROM target_trial_balance_manual
)

select * from target_trial_balance_manual_type
