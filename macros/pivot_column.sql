-- Ex 10.2: Convert the pivot logic into a reusable macro
-- Arguments: pivot_column (e.g. 'payment_method'), value_column (e.g. 'amount')
-- This makes the pivot pattern reusable across ANY model, not just payments

{% macro pivot_column(pivot_column, value_column, value_list) %}

    {%- for val in value_list %}

    sum(case when {{ pivot_column }} = '{{ val }}' then {{ value_column }} else 0 end) as {{ val }}_amount

    {%- if not loop.last -%}
    ,
    {%- endif -%}

    {%- endfor %}

{% endmacro %}
