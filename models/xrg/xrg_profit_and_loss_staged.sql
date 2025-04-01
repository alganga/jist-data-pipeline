WITH profit_and_loss AS (
    SELECT
        period,
        concept_id,
        concept,
        concept_name,
        store_id as source_entity_id,
        entity_id,
        entity_name,
        category_id,
        category_name,
        metric_order,
        subcategory_id,
        subcategory_name,
        submetric_order,
        gl_name,
        gl_number,
        sub_subgroup_id,
        sub_subgroup,
        subgroup_order,
        LPAD(SPLIT_PART(SPLIT_PART(period, 'P', 2), '-', 1), 2, '0') AS month,
        '20' || SPLIT_PART(period, '-', 2) AS year,
        NULLIF(-1 * SUM(beginning_balance), 0) AS beginning_balance,
        NULLIF(-1 * SUM(ending_balance), 0) AS ending_balance,
        NULLIF(-1 * SUM(net_budget_amount), 0) AS budget
    FROM {{ref('xrg_target_trial_balance')}}
    WHERE report_id = 3000
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
)
select * from profit_and_loss
