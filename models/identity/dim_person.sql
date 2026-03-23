/*
    Gold dimension: one row per person (latest state).
    person_key = md5(PID) -- stable, never changes even when PTK does.
*/

with registry as (
    select * from {{ ref('int_person_identity_registry') }}
),

-- Take the latest run per PID
latest as (
    select
        person_persistent_id,
        person_technical_key,
        group_id,
        member_count,
        assignment_reason,
        pipeline_run,
        run_date,
        row_number() over (
            partition by person_persistent_id
            order by run_date desc
        ) as rn
    from registry
)

select
    md5(person_persistent_id) as person_key,
    person_persistent_id,
    person_technical_key as current_ptk,
    group_id as current_group,
    member_count,
    pipeline_run as last_updated_run,
    run_date as last_updated_at
from latest
where rn = 1
