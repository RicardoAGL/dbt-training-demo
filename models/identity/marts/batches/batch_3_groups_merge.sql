/*
    BATCH 3: Groups merge (March 2026)
    - Group A (3 members) and Group B (1 member) merge into Group C (4 members)
    - PTK changes again (new combined membership)
    - PID: Group A had more members -> its PID survives (majority wins)
    - Group B's PID is effectively superseded

    Key observation: Two groups became one. The majority's PID survived.
    All historical facts for both groups now resolve through the same person_key.

    Run: SELECT * FROM batch_3_groups_merge ORDER BY group_id;
*/

with matching as (
    select * from {{ ref('int_person_matching') }}
    where pipeline_run = 'T3'
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T3'
),

prev_registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T2'
)

select
    '3 - Merge' as batch,
    r.group_id,
    r.assignment_reason,
    r.member_count,
    m.sorted_members as source_keys_in_group,
    left(r.person_technical_key, 12) as ptk,
    left(r.person_persistent_id, 12) as pid,
    left(sha256(r.person_persistent_id), 12) as person_key,
    (select left(p.person_persistent_id, 12)
     from prev_registry p
     where p.group_id = 'group_a') as was_group_a_pid,
    (select left(p.person_persistent_id, 12)
     from prev_registry p
     where p.group_id = 'group_b') as was_group_b_pid,
    'Group A PID won (majority: 3 vs 1)' as merge_explanation
from registry r
inner join matching m
    on r.pipeline_run = m.pipeline_run
    and r.group_id = m.group_id
order by r.group_id
