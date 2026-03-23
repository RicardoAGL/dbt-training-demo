/*
    BATCH 4: Group splits (April 2026)
    - dav:303 no longer matches the rest of Group C
    - Majority (3 members: dav:101, smc:202, dav:404) -> keeps the PID
    - Minority (1 member: dav:303) -> gets a NEW PID

    Key observation: The majority subgroup's person_key is STILL the same
    as it was in Batch 1. Four runs, three PTK changes, zero person_key changes.

    Run: SELECT * FROM batch_4_group_splits ORDER BY group_id;
*/

with matching as (
    select * from {{ ref('int_person_matching') }}
    where pipeline_run = 'T4'
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T4'
),

batch1 as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T1'
)

select
    '4 - Split' as batch,
    r.group_id,
    r.assignment_reason,
    r.member_count,
    m.sorted_members as source_keys_in_group,
    left(r.person_technical_key, 12) as ptk,
    left(r.person_persistent_id, 12) as pid,
    left(md5(r.person_persistent_id), 12) as person_key,
    case
        when b1.person_persistent_id is not null
        then 'same as Batch 1!'
        else 'new (split minority)'
    end as continuity_from_batch_1
from registry r
inner join matching m
    on r.pipeline_run = m.pipeline_run
    and r.group_id = m.group_id
left join batch1 b1
    on r.person_persistent_id = b1.person_persistent_id
order by r.group_id
