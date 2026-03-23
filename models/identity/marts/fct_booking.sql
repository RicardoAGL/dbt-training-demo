/*
    Fact table: bookings with person_key FK.

    JOIN PATH: booking -> source_key -> bridge (latest) -> person_key
    The person_key FK comes from the bridge, which derives it from the PID.
    Because PID is stable, this FK NEVER needs reattribution.

    We join to the LATEST bridge state to get current person assignments.
    In production, you'd join to the bridge at the pipeline run matching the fact's load date.
*/

with bookings as (
    select * from {{ ref('stg_bookings') }}
),

-- Get the most recent person assignment for each source_key
bridge_latest as (
    select
        source_key,                                              -- [source_key] the join key to facts
        person_key,                                              -- [person_key] = sha256(PID), the stable FK
        person_persistent_id,                                    -- [PID] for reference
        person_technical_key,                                    -- [PTK] for reference
        row_number() over (
            partition by source_key                              -- one row per source_key
            order by run_date desc                               -- latest assignment wins
        ) as rn
    from {{ ref('bridge_person_identity') }}
)

select
    b.booking_id,
    b.source_key,                                                -- [source_key] from the source system
    b.booking_date,
    b.amount,
    b.destination,
    bl.person_key,                                               -- [person_key] FK -> dim_person (stable!)
    bl.person_persistent_id,                                     -- [PID] for debugging
    bl.person_technical_key as current_ptk                       -- [PTK] for debugging
from bookings b
inner join bridge_latest bl
    on b.source_key = bl.source_key                              -- join on source_key
    and bl.rn = 1                                                -- latest assignment only
