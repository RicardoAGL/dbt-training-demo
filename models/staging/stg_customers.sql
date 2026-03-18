-- Ex 3.2: Staging model using source()
-- Replaces hardcoded table reference with {{ source('jaffle_shop', 'raw_customers') }}
-- Renames columns for downstream consistency

select
    id as customer_id,
    first_name,
    last_name

from {{ source('jaffle_shop', 'raw_customers') }}
