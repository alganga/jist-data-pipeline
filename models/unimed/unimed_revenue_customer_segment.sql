WITH base AS (
    SELECT
        usa."CardCode" AS card_code,
        usa."CardName" AS card_name,
        usa."GroupCode" AS group_code,
        usa."GroupName" AS group_name,
        usa."DocNum" AS doc_number,
        usa."DocDate" AS date,
        usa."DocTotalSy" AS revenue,
        usa."DiscSumSy" AS discount,
        usa."VatSumSy" AS tax,
        usa."GrosProfSy" AS gross_profit,
        usa."country_number",
        usa."country_name"
    FROM {{source('unimed', 'revenue_customer_usa')}} usa

    UNION ALL

    SELECT
        tm."CardCode" AS card_code,
        tm."CardName" AS card_name,
        tm."GroupCode" AS group_code,
        tm."GroupName" AS group_name,
        tm."DocNum" AS doc_number,
        tm."DocDate" AS date,
        tm."DocTotalSy" AS revenue,
        tm."DiscSumSy" AS discount,
        tm."VatSumSy" AS tax,
        tm."GrosProfSy" AS gross_profit,
        tm."country_number",
        tm."country_name"
    FROM {{source('unimed', 'revenue_customer_tm')}} tm

    UNION ALL

    SELECT
        uk."CardCode" AS card_code,
        uk."CardName" AS card_name,
        uk."GroupCode" AS group_code,
        uk."GroupName" AS group_name,
        uk."DocNum" AS doc_number,
        uk."DocDate" AS date,
        uk."DocTotalSy" AS revenue,
        uk."DiscSumSy" AS discount,
        uk."VatSumSy" AS tax,
        uk."GrosProfSy" AS gross_profit,
        uk."country_number",
        uk."country_name"
    FROM {{source('unimed', 'revenue_customer_uk')}} uk

    UNION ALL

    SELECT
        sg."CardCode" AS card_code,
        sg."CardName" AS card_name,
        sg."GroupCode" AS group_code,
        sg."GroupName" AS group_name,
        sg."DocNum" AS doc_number,
        sg."DocDate" AS date,
        sg."DocTotalSy" AS revenue,
        sg."DiscSumSy" AS discount,
        sg."VatSumSy" AS tax,
        sg."GrosProfSy" AS gross_profit,
        sg."country_number",
        sg."country_name"
    FROM {{source('unimed', 'revenue_customer_sg')}} sg

    UNION ALL

    SELECT
        aus."CardCode" AS card_code,
        aus."CardName" AS card_name,
        aus."GroupCode" AS group_code,
        aus."GroupName" AS group_name,
        aus."DocNum" AS doc_number,
        aus."DocDate" AS date,
        aus."DocTotalSy" AS revenue,
        aus."DiscSumSy" AS discount,
        aus."VatSumSy" AS tax,
        aus."GrosProfSy" AS gross_profit,
        aus."country_number",
        aus."country_name"
    FROM {{source('unimed', 'revenue_customer_aus')}} aus

    UNION ALL

    SELECT
        davie."CardCode" AS card_code,
        davie."CardName" AS card_name,
        davie."GroupCode" AS group_code,
        davie."GroupName" AS group_name,
        davie."DocNum" AS doc_number,
        davie."DocDate" AS date,
        davie."DocTotalSy" AS revenue,
        davie."DiscSumSy" AS discount,
        davie."VatSumSy" AS tax,
        davie."GrosProfSy" AS gross_profit,
        davie."country_number",
        davie."country_name"
    FROM {{source('unimed', 'revenue_customer_davie')}} davie
),

aggregated AS (
    SELECT
        country_number,
        country_name,
        COALESCE(segment, 'Other') AS group_name,
        date::date,
        SUM(revenue::FLOAT) AS revenue,
        SUM(discount::FLOAT) AS discount,
        SUM(tax::FLOAT) AS tax,
        SUM(gross_profit::FLOAT) AS gross_profit
    FROM base
    LEFT JOIN {{source('unimed', 'customer_segment_data')}} cus
        ON base.group_name = cus.group_name
    GROUP BY 1, 2, 3, 4
),

wtd_mtd_ytd AS (
    SELECT
        *,
        SUM(T1.revenue) OVER (PARTITION BY country_id, group_name, T1.end_of_week ORDER BY T1.date) AS actual_wtd_revenue,
        SUM(T1.revenue) OVER (PARTITION BY country_id, group_name, year, month ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_mtd_revenue,
        SUM(T1.revenue) OVER (PARTITION BY country_id, group_name, year ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_ytd_revenue,
        SUM(T1.discount) OVER (PARTITION BY country_id, group_name, T1.end_of_week ORDER BY T1.date) AS actual_wtd_discount,
        SUM(T1.discount) OVER (PARTITION BY country_id, group_name, year, month ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_mtd_discount,
        SUM(T1.discount) OVER (PARTITION BY country_id, group_name, year ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_ytd_discount,
        SUM(T1.tax) OVER (PARTITION BY country_id, group_name, T1.end_of_week ORDER BY T1.date) AS actual_wtd_tax,
        SUM(T1.tax) OVER (PARTITION BY country_id, group_name, year, month ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_mtd_tax,
        SUM(T1.tax) OVER (PARTITION BY country_id, group_name, year ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_ytd_tax,
        SUM(T1.gross_profit) OVER (PARTITION BY country_id, group_name, T1.end_of_week ORDER BY T1.date) AS actual_wtd_gross_profit,
        SUM(T1.gross_profit) OVER (PARTITION BY country_id, group_name, year, month ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_mtd_gross_profit,
        SUM(T1.gross_profit) OVER (PARTITION BY country_id, group_name, year ORDER BY T1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS actual_ytd_gross_profit
    FROM (
        SELECT
            country_id,
            country_name,
            group_name,
            date,
            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(DAY FROM date) AS day,
            revenue,
            discount,
            tax,
            gross_profit,
            CAST({{ get_end_of_week('date', 'Monday', 'Sunday') }} AS date) AS end_of_week,
            date_trunc('month', date at time zone 'UTC') + interval '1 month' - interval '1 day' AS period_end_date
        FROM aggregated a
        LEFT JOIN {{ source('unimed', 'entity_config_data') }} en
            ON a.country_number = en.country_id
        ) T1
),

cy_py AS (

SELECT
    cy.*,
    py.actual_wtd_revenue AS actual_wtd_revenue_py,
    py.actual_mtd_revenue AS actual_mtd_revenue_py,
    py.actual_ytd_revenue AS actual_ytd_revenue_py,
    py.actual_wtd_discount AS actual_wtd_discount_py,
    py.actual_mtd_discount AS actual_mtd_discount_py,
    py.actual_ytd_discount AS actual_ytd_discount_py,
    py.actual_wtd_tax AS actual_wtd_tax_py,
    py.actual_mtd_tax AS actual_mtd_tax_py,
    py.actual_ytd_tax AS actual_ytd_tax_py,
    py.actual_wtd_gross_profit AS actual_wtd_gross_profit_py,
    py.actual_mtd_gross_profit AS actual_mtd_gross_profit_py,
    py.actual_ytd_gross_profit AS actual_ytd_gross_profit_py
FROM wtd_mtd_ytd cy
LEFT JOIN wtd_mtd_ytd py
    ON cy.country_id = py.country_id
    AND cy.group_name = py.group_name
    AND cy.month = py.month
    and cy.day = py.day
    AND cy.year = py.year + 1
)

SELECT * 
FROM cy_py
ORDER BY country_id, group_name, year, month, date asc
