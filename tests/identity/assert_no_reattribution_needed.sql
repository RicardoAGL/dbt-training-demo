/*
    NO-REATTRIBUTION TEST: Simulates what would happen if we checked facts
    at T1 against the dim at T4. With PID-based person_key, all FKs should
    still resolve -- no orphaned facts.

    This is the key business value: facts written at ANY point in time
    remain joinable to dim_person without updates.
*/

with facts as (
    select * from {{ ref('fct_booking') }}
),

dim as (
    select * from {{ ref('dim_person') }}
)

-- Fail if any fact's person_key doesn't exist in dim_person
select
    f.booking_id,
    f.person_key as orphaned_person_key
from facts f
left join dim d on f.person_key = d.person_key
where d.person_key is null
