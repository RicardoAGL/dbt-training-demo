-- Ex 3.2: Staging model using source()
-- Renames id -> order_id, user_id -> customer_id for clarity

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from {{ source('jaffle_shop', 'raw_orders') }}
