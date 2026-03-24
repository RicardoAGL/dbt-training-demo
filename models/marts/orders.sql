-- Ex 9.1: Materializations - Incremental model
-- Problem: What if this table has millions of rows and grows daily?
-- Solution: Only process NEW rows since the last run
--
-- Config: materialized='incremental' + unique_key for upsert behavior
-- The {% if is_incremental() %} block only runs on incremental runs (not full-refresh)
-- First run: builds the full table (like materialized='table')
-- Next runs: only inserts/updates rows where order_date > max existing date
--
-- Run: dbt run --select orders              (incremental)
-- Run: dbt run --select orders --full-refresh  (rebuild from scratch)

{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (

    select
        order_id,
        sum(amount) as amount

    from payments

    group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        coalesce(order_payments.amount, 0) as amount

    from orders

    left join order_payments using (order_id)

)

select * from final

{% if is_incremental() %}

    -- only get rows that are newer than the latest order_date in our table
    where order_date > (select max(order_date) from {{ this }})

{% endif %}
