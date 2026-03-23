/*
    MERGE EXCLUSION TEST: After merge at T3, group_c's PID must NOT be
    group_b's PID. This ensures the majority rule actually picked group_a
    (not just that "some valid PID" was assigned).

    Complements assert_pid_survives_merge which checks the positive case.
*/

with t1_group_b as (
    select person_persistent_id as pid_b
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T1' and group_id = 'group_b'
),

t3_group_c as (
    select person_persistent_id as pid_c
    from {{ ref('int_person_identity_registry') }}
    where pipeline_run = 'T3' and group_id = 'group_c'
)

-- Fail if group_c inherited group_b's PID (minority should NOT win)
select 'merge picked minority PID' as failure_reason, b.pid_b, c.pid_c
from t1_group_b b, t3_group_c c
where b.pid_b = c.pid_c
