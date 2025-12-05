{% macro standardize_text(column_name) %}
    -- Removes leading/trailing whitespace, replaces common separators with a space,
    -- and capitalizes the first letter of each word.
    INITCAP(REPLACE(TRIM(LOWER({{ column_name }})), '_', ' '))
{% endmacro %}