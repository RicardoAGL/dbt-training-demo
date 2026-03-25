-- Ex 10.1: Custom macro - generate a surrogate key
-- This is how you write your OWN macro
-- Compare with dbt_utils.generate_surrogate_key which does the same thing!

{% macro generate_surrogate_key(column1, column2) %}
    md5(concat({{ column1 }}, {{ column2 }}))
{% endmacro %}
