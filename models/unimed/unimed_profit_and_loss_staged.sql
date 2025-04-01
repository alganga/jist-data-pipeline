WITH balances AS (
    SELECT T0.*
    FROM (
        SELECT
            EXTRACT(YEAR FROM date::date) AS year,
            EXTRACT(MONTH FROM date::date) AS month,
            date,
            country_id,
            country_name,
            category_id,
            category_name,
            metric_order,
            gl_name,
            gl_number,
            debit,
            credit,
            ending_balance,
            ROW_NUMBER() OVER (PARTITION BY gl_number, country_id, EXTRACT(YEAR FROM date::date), EXTRACT(MONTH FROM date::date) ORDER BY date DESC) AS row_num
        FROM {{ref('unimed_target_trial_balance')}} uttb 
        WHERE report_id = 3000
    ) T0
    WHERE T0.row_num = 1 
),
balances_with_begin AS (
    SELECT
        year,
        month,
        country_id,
        country_name,
        category_id,
        category_name,
        metric_order,
        gl_name,
        gl_number,
        debit,
        credit,
        case
            when month = 1 then 0
            else coalesce(lag(ending_balance, 1) over (partition by gl_number, country_id order by year::int, month::int), 0) 
        end as beginning_balance,
        ending_balance
    FROM balances
),

pnl_staged AS (
SELECT
	CONCAT(CONCAT('P', CAST(month AS VARCHAR)), CONCAT('-', RIGHT(CAST(year AS VARCHAR), 2))) AS period,
    year,
    month,
    country_id,
    country_name,
    category_id,
    category_name,
    metric_order,
    gl_name,
    gl_number,
    debit,
    credit,
    case
        when coalesce(-1 * beginning_balance, 0) = -0 then 0
        else coalesce(-1 * beginning_balance, 0)
    end as beginning_balance,
    case
        when coalesce(-1 * ending_balance, 0) = -0 then 0
        else coalesce(-1 * ending_balance, 0)
    end as ending_balance
FROM balances_with_begin
)

SELECT * FROM pnl_staged


