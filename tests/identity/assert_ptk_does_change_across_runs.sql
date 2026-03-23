/*
    POSITIVE PROOF: PTK must actually change across runs for PIDs that span
    multiple runs. This proves the design is doing real work -- stabilizing
    a key that would otherwise change.

    Expects 0 rows: at least one PID must have 2+ distinct PTKs.
    If all PIDs have only 1 PTK, the prototype isn't testing anything meaningful.
*/

with registry as (
    select person_persistent_id as pid, person_technical_key as ptk
    from {{ ref('int_person_identity_registry') }}
),

multi_run_pids as (
    select
        pid,
        count(distinct ptk) as distinct_ptks,
        count(*) as run_count
    from registry
    group by pid
    having count(*) >= 2
)

-- Fail if NO multi-run PID has changing PTKs (means the test data is too simple)
select 'no PTK changes detected across runs' as failure_reason
where not exists (
    select 1 from multi_run_pids where distinct_ptks >= 2
)
