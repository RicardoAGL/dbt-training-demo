/*
    BATCH 5: The payoff -- check all bookings against the final dim_person.
    Every booking from any time period resolves to a valid person.
    No orphans. No reattribution needed.

    Run: SELECT * FROM batch_5_fact_check ORDER BY booking_date;
*/

with facts as (
    select * from {{ ref('fct_booking') }}
),

dim as (
    select * from {{ ref('dim_person') }}
)

select
    '5 - Fact Check' as batch,
    f.booking_id,
    f.source_key,
    f.booking_date,
    f.amount,
    f.destination,
    left(f.person_key, 12) as person_key,
    d.current_group,
    d.member_count as current_members,
    case
        when d.person_key is not null
        then 'RESOLVED'
        else 'ORPHANED -- would need reattribution!'
    end as fk_status
from facts f
left join dim d on f.person_key = d.person_key
order by f.booking_date
