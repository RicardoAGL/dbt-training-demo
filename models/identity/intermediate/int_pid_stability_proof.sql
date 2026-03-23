/*
    Exploration view: shows how PTK changes across runs but PID stays stable.
    Use this to visually verify the design works.

    Query this table and you'll see:
    - PTK column changes across runs (group membership changed)
    - PID column stays the same (identity is stable)
    - person_key column stays the same (derived from PID)
*/

with registry as (
    select * from {{ ref('int_person_identity_registry') }}
)

select
    pipeline_run,
    run_date,
    group_id,
    left(person_technical_key, 8) as ptk_short,
    person_persistent_id as pid,
    sha256(person_persistent_id) as person_key,
    member_count,
    assignment_reason
from registry
order by person_persistent_id, pipeline_run
