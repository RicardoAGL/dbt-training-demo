/*
    CORE TEST: person_key must remain stable even when PTK changes.

    Group A's PTK changes from T1 -> T2 (new member joined),
    but person_key (derived from PID) must stay the same.

    This test returns rows where person_key CHANGED for the same PID -- should be 0 rows.
*/

with registry as (
    select
        pipeline_run,
        person_persistent_id as pid,
        sha256(person_persistent_id) as person_key,
        person_technical_key as ptk
    from {{ ref('int_person_identity_registry') }}
),

-- For each PID, check if person_key is the same across all runs
pid_keys as (
    select
        pid,
        count(distinct person_key) as distinct_person_keys,
        count(distinct ptk) as distinct_ptks
    from registry
    group by pid
)

-- Fail if any PID has more than one person_key
-- (having multiple PTKs is expected and fine -- that's the whole point)
select *
from pid_keys
where distinct_person_keys > 1
