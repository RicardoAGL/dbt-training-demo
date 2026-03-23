/*
    BATCH 2: Group A grows (February 2026)
    - New source key davinci:404 matched into Group A
    - PTK changes (different members = different hash)
    - PID stays the same (inherited from Batch 1)
    - Group B unchanged

    Key observation: PTK column changed, PID and person_key did NOT.

    Run: SELECT * FROM batch_2_group_grows ORDER BY group_id;
*/

with matching as (
    select * from {{ ref('int_person_matching') }}
    where pipeline_run = 'T2'
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T2'
),

prev_registry as (
    select * from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T1'
)

select
    '2 - Growth' as batch,
    r.group_id,
    r.assignment_reason,
    r.member_count,
    m.sorted_members as source_keys_in_group,
    left(r.person_technical_key, 12) as ptk,
    left(r.person_persistent_id, 12) as pid,
    left(md5(r.person_persistent_id), 12) as person_key,
    case
        when p.person_technical_key is not null
             and p.person_technical_key != r.person_technical_key
        then 'YES'
        else 'no'
    end as ptk_changed,
    case
        when p.person_persistent_id is not null
             and p.person_persistent_id != r.person_persistent_id
        then 'YES -- BUG!'
        else 'no (stable)'
    end as pid_changed
from registry r
inner join matching m
    on r.pipeline_run = m.pipeline_run
    and r.group_id = m.group_id
left join prev_registry p
    on r.person_persistent_id = p.person_persistent_id
order by r.group_id
