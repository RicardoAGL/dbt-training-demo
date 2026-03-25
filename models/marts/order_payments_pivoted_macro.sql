-- Ex 10.2: Same pivot, but using our custom macro
-- Compare this with order_payments_pivoted.sql - same result, much cleaner!

{%- set payment_methods = dbt_utils.get_column_values(
    table=ref('stg_payments'),
    column='payment_method'
) -%}

select
    order_id,
    {{ pivot_column('payment_method', 'amount', payment_methods) }}
from {{ ref('stg_payments') }}
group by 1
