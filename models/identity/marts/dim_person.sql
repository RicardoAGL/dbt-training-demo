/*
    Gold dimension: one row per person, keyed on person_key = sha256(PID).

    WHY THIS WORKS:
    - The PID is stable across group growth, merge, and split.
    - person_key = sha256(PID) therefore NEVER changes.
    - Fact tables join on person_key, so they never need reattribution.

    CONTRAST WITH PHASE 1 (PTK-based person_key):
    If person_key = sha256(PTK), then every time a group grows/merges/splits,
    the person_key changes and all fact rows need correction via a reattribution
    post-hook (ADR-003 Section 10.2). With PID-based derivation, this entire
    reattribution pipeline is eliminated.

    This model takes the LATEST state for each PID from the registry.
*/

with registry as (
    select * from {{ ref('int_person_identity_registry') }}
),

-- Take the latest pipeline run per PID (most recent state wins)
latest as (
    select
        person_persistent_id,                                    -- [PID] the stable person identifier
        person_technical_key,                                    -- [PTK] the latest group fingerprint
        group_id,                                                -- [group_id] current cluster label
        member_count,
        assignment_reason,
        pipeline_run,
        run_date,
        row_number() over (
            partition by person_persistent_id                    -- one row per PID
            order by run_date desc                               -- latest run first
        ) as rn
    from registry
)

select
    sha256(person_persistent_id) as person_key,                     -- [person_key] = sha256(PID), the FK for all facts
    person_persistent_id,                                        -- [PID] stable identifier
    person_technical_key as current_ptk,                         -- [PTK] latest group fingerprint (informational)
    group_id as current_group,
    member_count,
    pipeline_run as last_updated_run,
    run_date as last_updated_at
from latest
where rn = 1
