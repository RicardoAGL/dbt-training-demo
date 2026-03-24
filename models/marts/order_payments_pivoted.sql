-- Ex 8.1: Jinja - Step 1: Pure SQL pivot
-- Problem: We want payment amounts broken out by method per order
-- This works, but notice the repetition...

with payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,

        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,

        sum(amount) as total_amount

    from payments

    group by 1

)

select * from pivoted
