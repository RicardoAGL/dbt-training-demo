-- Ex 8.4: Jinja - Step 4: Dynamic pivot with dbt_utils.get_column_values
-- Now the list comes from the DATA, not from our code
-- If a new payment method appears tomorrow, this model handles it automatically
-- Requires: dbt deps (to install dbt_utils)

{% set payment_methods = dbt_utils.get_column_values(
    table=ref('stg_payments'),
    column='payment_method'
) -%}

with payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,

        {%- for payment_method in payment_methods %}

        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount

        {%- if not loop.last -%}
        ,
        {%- endif -%}

        {%- endfor %}

    from payments

    group by 1

)

select * from pivoted
