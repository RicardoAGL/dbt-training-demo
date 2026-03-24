-- Ex 8.3: Jinja - Step 3: Fix trailing comma with set + loop.last
-- {% set %} defines the list at the top (single source of truth)
-- {% if not loop.last %} only adds a comma between columns, not after the last one
-- Try: dbt compile --select order_payments_pivoted  (clean SQL now!)

{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,

        {% for payment_method in payment_methods %}

        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount

        {% if not loop.last %}
        ,
        {% endif %}

        {% endfor %}

    from payments

    group by 1

)

select * from pivoted
