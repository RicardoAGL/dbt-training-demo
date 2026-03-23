/*
    SPLIT TEST: When group C splits at T4,
    the majority subgroup (group_a, 3 members) keeps the PID.
    The minority (group_d, 1 member) gets a NEW PID.
*/

with t3_group_c as (
    select person_persistent_id as pid_t3
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T3' and group_id = 'group_c'
),

t4_majority as (
    select person_persistent_id as pid_t4_majority
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T4' and group_id = 'group_a'
),

t4_minority as (
    select person_persistent_id as pid_t4_minority
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T4' and group_id = 'group_d'
)

-- Fail if: majority didn't keep PID OR minority has same PID as parent
select 'majority lost PID' as failure_reason
from t3_group_c, t4_majority
where pid_t3 != pid_t4_majority

union all

select 'minority kept parent PID (should have new one)' as failure_reason
from t3_group_c, t4_minority
where pid_t3 = pid_t4_minority
