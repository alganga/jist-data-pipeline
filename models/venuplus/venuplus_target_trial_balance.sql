WITH source_trial_balance_sage AS (
    SELECT
        "ACC"::text AS gl_number,
        "$uuid" AS unique_ledger_id,
        "STE" AS legal_entity_id,
        "P_PERIOD" AS period,
        "TYPE" AS type,
        "MONTH" AS month,
        "FYR" AS year,
        "CR" AS currency,
        "FYN" AS year_code,
        SUM("LED_OPENING_BALANCE") AS beginning_balance,
        SUM("LED_ENDING_BALANCE") AS ending_balance,
        SUM("BUDGET") AS net_budget_amount,
        SUM("LED_DEBIT") AS period_net_dr,
        SUM("LED_CREDIT") AS period_net_cr
    FROM
 
       {{source('venuplus', 'trial_balances')}} tb
    WHERE is_deleted IS FALSE
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

report_config AS (
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

target_trial_balance_sage AS (
	SELECT
		rc.company_id::int,
		ent.source_entity_id,
		ent.entity_name,
		ent.entity_id,
		ent.country_id,
		ent.country_name,
		rc.report_id,
		rc.report_name,
		rc.gl_number::text,
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
		src.year::text,
		src.year_code,
		src.month::text,
		src.period,
		src.currency,
		src.type,
		SUM(src.beginning_balance) AS beginning_balance,
		SUM(src.ending_balance) AS ending_balance,
		SUM(src.net_budget_amount) AS net_budget_amount,
		SUM(src.period_net_dr) AS period_net_dr,
		SUM(src.period_net_cr) AS period_net_cr,
		'sage' AS source
	FROM
		source_trial_balance_sage src
	LEFT JOIN entity_config ent ON
	    src.legal_entity_id = ent.source_entity_id
	LEFT JOIN report_config rc ON
	    CASE 
	        WHEN RIGHT(src.gl_number::text, 1) ~ '[A-Za-z]' THEN LEFT(src.gl_number::text, LENGTH(src.gl_number::text) - 1)
	        ELSE src.gl_number::text
	    END = rc.gl_number::text
	LEFT JOIN metric_config mc_category ON
	    mc_category.metric_id = rc.category_id 
	    AND mc_category.type = 'metric' 
	    AND mc_category.report_id = rc.report_id
	LEFT JOIN metric_config mc_subcategory ON
	    mc_subcategory.metric_id = rc.subcategory_id 
	    AND mc_subcategory.type = 'sub_metric'
	    AND mc_subcategory.report_id = rc.report_id
	LEFT JOIN metric_config mc_subgroup ON
	    mc_subgroup.metric_id = rc.sub_subgroup_id 
	    AND mc_subgroup.type = 'sub_group'
	    AND mc_subgroup.report_id = rc.report_id
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27
),
   
target_trial_balance AS (
	select * from {{ ref('venuplus_target_trial_balance_manual') }}
	
	union all
	
	select * from target_trial_balance_sage
)

select * from target_trial_balance
