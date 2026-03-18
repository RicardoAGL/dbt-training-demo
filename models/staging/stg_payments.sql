select
    id as payment_id,
    order_id,
    payment_method,
    status,

    -- amount is stored in cents, convert to dollars
    amount / 100 as amount

from {{ source('jaffle_shop', 'raw_payments') }}
