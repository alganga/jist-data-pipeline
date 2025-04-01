{% macro get_end_of_week(date_field, start_of_week='Monday', end_of_week='Sunday') %}
    {% set day_adjustments = {
        'Monday': 0,
        'Tuesday': 1,
        'Wednesday': 2,
        'Thursday': 3,
        'Friday': 4,
        'Saturday': 5,
        'Sunday': 6
    } %}

    {% set start_adjustment = day_adjustments[start_of_week] %}
    {% set end_adjustment = day_adjustments[end_of_week] %}
    {% set total_adjustment = end_adjustment - start_adjustment %}

    date_trunc('week', cast({{ date_field }} as date) at time zone 'UTC') + interval '{{ total_adjustment }} days'
{% endmacro %}
