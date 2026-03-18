-- Ex 3.2: Staging model using source()
-- Converts amount from cents to dollars

select
    id as payment_id,
    order_id,
    payment_method,

    -- amount is stored in cents, convert to dollars
    amount / 100 as amount

from {{ source('jaffle_shop', 'raw_payments') }}
