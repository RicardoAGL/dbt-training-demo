-- Ex 8.2: Jinja - Step 2: Replace repetition with {% for %} loop
-- Better! But we have a problem: trailing comma after the last column
-- Try: dbt compile --select order_payments_pivoted  (see the compiled SQL)

with payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,

        {% for payment_method in ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        -- ^ trailing comma on last column will cause a SQL error!

        {% endfor %}

        sum(amount) as total_amount

    from payments

    group by 1

)

select * from pivoted
