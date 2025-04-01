{% macro get_days_in_month(year, month) %}
    CASE
        WHEN cast({{ month }} as int) IN (1, 3, 5, 7, 8, 10, 12) THEN 31
        WHEN cast({{ month }} as int) IN (4, 6, 9, 11) THEN 30
        WHEN cast({{ month }} as int) = 2 THEN
            CASE
                WHEN (cast({{ year }} as int) % 4 = 0 AND (cast({{ year }} as int) % 100 != 0 OR cast({{ year }} as int) % 400 = 0)) THEN 29
                ELSE 28
            END
        ELSE null
    END
{% endmacro %}
