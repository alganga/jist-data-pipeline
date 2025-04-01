WITH sales AS(
    SELECT
        country_id,
        country_name,
        date,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        EXTRACT(DAY FROM date::date) AS day,
        end_of_week,
        sales,
        COALESCE(sales - LAG(sales, 1) OVER (PARTITION BY country_id, EXTRACT(YEAR FROM date::date)  ORDER BY date ASC), sales) AS daily_sales
    FROM (
        SELECT
            country_id,
            country_name,
            date,
            SUM(ending_balance) AS sales,
            CAST({{ get_end_of_week('date', 'Monday', 'Sunday') }} AS date) AS end_of_week
        FROM {{ref('unimed_target_trial_balance')}}
        WHERE report_id = 2000
        AND category_id = 2 -- Revenue accounts
        GROUP BY 1, 2, 3
        ORDER BY country_id, date
    ) T1
),

cogs AS(
    SELECT
        country_id,
        country_name,
        date,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        EXTRACT(DAY FROM date::date) AS day,
        end_of_week,
        cogs,
        COALESCE(cogs - LAG(cogs, 1) OVER (PARTITION BY country_id, EXTRACT(YEAR FROM date::date) ORDER BY date ASC), cogs) AS daily_cogs
    FROM (
        SELECT
            country_id,
            country_name,
            date,
            SUM(ending_balance) AS cogs,
            CAST({{ get_end_of_week('date', 'Monday', 'Sunday') }} AS date) AS end_of_week
        FROM {{ref('unimed_target_trial_balance')}}
        WHERE report_id = 2000
        AND subcategory_id = 18 -- COGS accounts
        GROUP BY 1, 2, 3
        ORDER BY country_id, date
    ) T1
),

cy AS (
    SELECT
        s.country_id,
        s.country_name,
        s.year,
        s.month,
        s.day,
        s.date,
        s.end_of_week,
        EXTRACT(YEAR FROM s.end_of_week) AS end_of_week_year,
        EXTRACT(MONTH FROM s.end_of_week) AS end_of_week_month,
        EXTRACT(DAY FROM s.end_of_week) AS end_of_week_day,
        date_trunc('month', s.date::date at time zone 'UTC') + interval '1 month' - interval '1 day' AS period_end_date,
        s.sales AS gross_sales,
        c.cogs,
        s.sales - c.cogs AS gross_margin,
        s.daily_sales AS daily_gross_sales,
        c.daily_cogs AS daily_cogs,
        -- wtd
        SUM(s.daily_sales) OVER (PARTITION BY s.country_id, s.end_of_week ORDER BY s.date ASC) AS actual_wtd_gross_sales,
        SUM(c.daily_cogs) OVER (PARTITION BY s.country_id, s.end_of_week ORDER BY s.date ASC) AS actual_wtd_cogs,
        SUM(abs(s.daily_sales) - abs(c.daily_cogs)) OVER (PARTITION BY s.country_id, s.end_of_week ORDER BY s.date ASC) AS actual_wtd_gross_margin,
        -- mtd
        SUM(s.daily_sales) OVER (PARTITION BY s.country_id, s.year, s.month ORDER BY s.date ASC) AS actual_mtd_gross_sales,
        SUM(c.daily_cogs) OVER (PARTITION BY s.country_id, s.year, s.month ORDER BY s.date ASC) AS actual_mtd_cogs,
        SUM(abs(s.daily_sales) - abs(c.daily_cogs)) OVER (PARTITION BY s.country_id, s.year, s.month ORDER BY s.date ASC) AS actual_mtd_gross_margin,
        -- ytd
        SUM(s.daily_sales) OVER (PARTITION BY s.country_id, s.year ORDER BY s.date ASC) AS actual_ytd_gross_sales,
        SUM(c.daily_cogs) OVER (PARTITION BY s.country_id, s.year ORDER BY s.date ASC) AS actual_ytd_cogs,
        SUM(abs(s.daily_sales) - abs(c.daily_cogs)) OVER (PARTITION BY s.country_id, s.year ORDER BY s.date ASC) AS actual_ytd_gross_margin
    FROM sales s
    LEFT JOIN cogs c
        ON s.date = c.date
        AND s.country_id = c.country_id
    ORDER BY s.country_id, s.date
),

cy_py AS(
SELECT
    cy.*,
    py.actual_wtd_gross_sales AS actual_wtd_gross_sales_py,
    py.actual_mtd_gross_sales AS actual_mtd_gross_sales_py,
    py.actual_ytd_gross_sales AS actual_ytd_gross_sales_py,
    py.actual_wtd_cogs AS actual_wtd_cogs_py,
    py.actual_mtd_cogs AS actual_mtd_cogs_py,
    py.actual_ytd_cogs AS actual_ytd_cogs_py,
    py.actual_wtd_gross_margin AS actual_wtd_gross_margin_py,
    py.actual_mtd_gross_margin AS actual_mtd_gross_margin_py,
    py.actual_ytd_gross_margin AS actual_ytd_gross_margin_py,
    EXTRACT(YEAR FROM cy.period_end_date) AS period_end_year,
    EXTRACT(MONTH FROM cy.period_end_date) AS period_end_month,
    EXTRACT(DAY FROM cy.period_end_date) AS period_end_day
FROM cy
LEFT JOIN cy py
    ON cy.country_id = py.country_id
    AND cy.month = py.month
    AND cy.day = py.day
    AND cy.year = py.year + 1
)

SELECT * FROM cy_py