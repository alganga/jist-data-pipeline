WITH GUEST_CHECK_BASE AS (
    SELECT
        entity_period_id,
        T1.*
    FROM(
        SELECT
            CAST(SUBSTRING(gc."LOCATION_CODE", 1, LENGTH(gc."LOCATION_CODE")-2) AS INT) AS location_code,
            gc."LOCATION_NAME" AS location_name,
            entity.concept_id AS concept_id,
            entity.concept_name AS concept_name,
            entity.entity_id,
            entity.entity_name,
            CAST(gc."POS_DATE" AS DATE) AS point_of_sale_date,
            CAST(EXTRACT(YEAR FROM gc."POS_DATE") AS INT) AS year,
            CAST(EXTRACT(MONTH FROM gc."POS_DATE") AS INT) AS month,
            CAST(EXTRACT(DAY FROM gc."POS_DATE") AS INT) AS day,
            COUNT(gc."INV_DTL_POS_CHECK_PK") AS check_count,
            SUM(gc."GUEST_COUNT") AS guest_count
        FROM {{source('pink_taco', 'guest_check')}} gc
        LEFT JOIN {{source('pink_taco', 'entity_config')}} entity
            ON CAST(SUBSTRING(gc."LOCATION_CODE", 1, LENGTH(gc."LOCATION_CODE")-2) AS INT)  = CAST(entity.source_entity_id AS INT)
        WHERE gc.is_deleted = FALSE
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) T1
    LEFT JOIN {{source('pink_taco', 'entity_period_config')}} epc
        ON T1.entity_id = epc.entity_id
        AND T1.year = epc.year
        AND T1.month = epc.month
),
/*
    This selection calculates the actual weighted, monthly, and yearly average check values for each location and entity.
    It uses the GUEST_CHECK_BASE table as the base data and joins it with the pink_taco_revenue_wtd table to retrieve the net sales values.
    The calculated average check values are based on the check count and net sales values for the specified time periods (week-to-date, month-to-date, year-to-date).
    If the check count is zero for a particular time period, the average check value is set to 0.
    The result set includes the location information, concept information, entity information, point of sale date, year, month, day, check count, guest count, end of week, start of month, and the calculated average check values.
*/
ACTUAL_WTD_MTD_YTD_CALCULATION AS (
    SELECT
        t2.*,
        CASE
            WHEN wtd_check_count > 0 THEN wtd_net_sales/wtd_check_count
            ELSE 0
        END AS wtd_avg_check_value,
        CASE
            WHEN mtd_check_count > 0 THEN mtd_net_sales/mtd_check_count
            ELSE 0
        END AS mtd_avg_check_value,
        CASE
            WHEN ytd_check_count > 0 THEN ytd_net_sales/ytd_check_count
            ELSE 0
        END AS ytd_avg_check_value
    FROM(
        SELECT
            t1.entity_period_id,
            t1.location_code,
            t1.location_name,
            t1.concept_id,
            t1.concept_name,
            t1.entity_id,
            t1.entity_name,
            t1.point_of_sale_date,
            t1.year,
            t1.month,
            t1.day,
            t1.check_count,
            t1.guest_count,
            t1.end_of_week,
            t1.start_of_month,
            revenue_wtd.net_sales,
            revenue_wtd.wtd_net_sales AS wtd_net_sales,
            revenue_wtd.mtd_net_sales AS mtd_net_sales,
            revenue_wtd.ytd_net_sales AS ytd_net_sales,
            SUM(check_count) OVER (PARTITION BY location_code, t1.end_of_week ORDER BY point_of_sale_date) AS wtd_check_count,
            SUM(guest_count) OVER (PARTITION BY location_code, t1.end_of_week ORDER BY point_of_sale_date) AS wtd_guest_count,
            SUM(check_count) OVER (PARTITION BY location_code, calendar.period_end_year, calendar.period_end_month ORDER BY point_of_sale_date) AS mtd_check_count,
            SUM(guest_count) OVER (PARTITION BY location_code, calendar.period_end_year, calendar.period_end_month ORDER BY point_of_sale_date) AS mtd_guest_count,
            SUM(check_count) OVER (PARTITION BY location_code, calendar.period_end_year ORDER BY point_of_sale_date) AS ytd_check_count,
            SUM(guest_count) OVER (PARTITION BY location_code, calendar.period_end_year ORDER BY point_of_sale_date) AS ytd_guest_count
        FROM (
            SELECT
                *,
                CAST({{get_end_of_week('point_of_sale_date', 'Monday', 'Sunday')}} AS DATE) AS end_of_week,
                CAST(DATE_TRUNC('month', point_of_sale_date) AS DATE) AS start_of_month
            FROM GUEST_CHECK_BASE
        ) t1
        LEFT JOIN{{source('pink_taco', 'fiscal_calendar')}} calendar
            ON cast(t1.end_of_week AS VARCHAR) = cast(calendar.week_end_date AS VARCHAR)
        LEFT JOIN {{ref('pink_taco_revenue_wtd')}} revenue_wtd
            ON t1.entity_id = revenue_wtd.entity_id
            AND t1.year = revenue_wtd.year
            AND t1.month = revenue_wtd.month
            AND t1.day = revenue_wtd.day
    ) t2
)
--Final Select
-- This query combines data from the table ACTUAL_WTD_MTD_YTD_CALCULATION with its corresponding data from the previous year.
-- It retrieves various metrics such as check count, guest count, net sales, and average check value for both the current year and the previous year.
-- The data is joined based on the entity ID, concept ID, month, day, and year.
SELECT
    t1.*,
    t2.check_count AS check_count_py,
    t2.guest_count AS guest_count_py,
    t2.net_sales AS net_sales_py,
    t2.wtd_net_sales AS wtd_net_sales_py,
    t2.mtd_net_sales AS mtd_net_sales_py,
    t2.ytd_net_sales AS ytd_net_sales_py,
    t2.wtd_check_count AS wtd_check_count_py,
    t2.wtd_guest_count AS wtd_guest_count_py,
    t2.mtd_check_count AS mtd_check_count_py,
    t2.mtd_guest_count AS mtd_guest_count_py,
    t2.ytd_check_count AS ytd_check_count_py,
    t2.ytd_guest_count AS ytd_guest_count_py,
    t2.wtd_avg_check_value AS wtd_avg_check_value_py,
    t2.mtd_avg_check_value AS mtd_avg_check_value_py,
    t2.ytd_avg_check_value AS ytd_avg_check_value_py
FROM ACTUAL_WTD_MTD_YTD_CALCULATION t1
LEFT JOIN ACTUAL_WTD_MTD_YTD_CALCULATION t2
    ON t1.entity_id = t2.entity_id
    AND t1.concept_id = t2.concept_id
    AND t1.month = t2.month
    AND t1.day = t2.day
    AND t1.year = t2.year + 1