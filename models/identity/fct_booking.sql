/*
    Fact table: bookings joined to person identity via source_key.

    The person_key FK comes from the bridge, which derives it from the PID.
    Because PID is stable, this FK NEVER needs reattribution.

    We join to the LATEST bridge state (T4) to get current person assignments.
    In production, you'd join to the bridge at the pipeline run matching the fact's load date.
*/

with bookings as (
    select * from {{ ref('raw_bookings') }}
),

-- Use latest bridge state for current person assignments
bridge_latest as (
    select
        source_key,
        person_key,
        person_persistent_id,
        person_technical_key,
        row_number() over (
            partition by source_key
            order by run_date desc
        ) as rn
    from {{ ref('bridge_person_identity') }}
)

select
    b.booking_id,
    b.source_key,
    b.booking_date,
    b.amount,
    b.destination,
    bl.person_key,
    bl.person_persistent_id,
    bl.person_technical_key as current_ptk
from bookings b
inner join bridge_latest bl
    on b.source_key = bl.source_key
    and bl.rn = 1
