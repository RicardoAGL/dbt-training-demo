/*
    Staging: raw bookings from source systems.
    Each booking is tied to a source_key (the customer who booked).

    These facts will later get a person_key FK via the identity bridge.
*/

with source as (
    select * from {{ ref('raw_bookings') }}
)

select
    booking_id,
    source_key,                                                  -- [source_key] links to identity system
    booking_date,
    amount,
    destination
from source
