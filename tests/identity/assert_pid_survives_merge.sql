/*
    MERGE TEST: When groups A and B merge into C at T3,
    the PID from the older group (A, first seen T1) must survive.

    Group A's PID at T1 should equal Group C's PID at T3.
*/

with t1_group_a as (
    select person_persistent_id as pid_t1
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T1' and group_id = 'group_a'
),

t3_group_c as (
    select person_persistent_id as pid_t3
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T3' and group_id = 'group_c'
)

-- Fail if the PIDs don't match (merge didn't preserve the older PID)
select 'PID mismatch on merge' as failure_reason, t1.pid_t1, t3.pid_t3
from t1_group_a t1, t3_group_c t3
where t1.pid_t1 != t3.pid_t3
