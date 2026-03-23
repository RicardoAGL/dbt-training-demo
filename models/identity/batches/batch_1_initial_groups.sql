/*
    BATCH 1: Initial state (January 2026)
    - Two person groups identified from source data
    - Group A: two source keys matched (davinci:101 + smc:202)
    - Group B: one source key, no matches yet (davinci:303)
    - Each group gets a fresh PID (first time seen)

    Run: SELECT * FROM batch_1_initial_groups ORDER BY group_id;
*/

with matching as (
    select * from {{ ref('int_person_matching') }}
    where pipeline_run = 'T1'
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T1'
),

bridge as (
    select * from {{ ref('bridge_person_identity') }}
    where pipeline_run = 'T1'
)

select
    '1 - Initial' as batch,
    r.group_id,
    r.assignment_reason,
    r.member_count,
    m.sorted_members as source_keys_in_group,
    left(r.person_technical_key, 12) as ptk,
    left(r.person_persistent_id, 12) as pid,
    left(md5(r.person_persistent_id), 12) as person_key
from registry r
inner join matching m
    on r.pipeline_run = m.pipeline_run
    and r.group_id = m.group_id
order by r.group_id
