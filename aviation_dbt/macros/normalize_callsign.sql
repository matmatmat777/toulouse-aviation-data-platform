{% macro normalize_callsign(column_name) %}

    upper(trim({{ column_name }}))

{% endmacro %}