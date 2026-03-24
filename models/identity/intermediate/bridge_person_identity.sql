/*
    Bridge: maps each source_key to its person_key (the Gold FK).
    Lives in the INTERMEDIATE layer (not Gold) per ADR-003.

    JOIN PATH:  source_key -> group_id -> PTK -> PID -> person_key
                (source)     (cluster)   (mutable)  (stable)  (FK on facts)

    This is the "Rosetta Stone" that translates between:
    - What source systems know (source_key)
    - What identity resolution produces (group_id, PTK)
    - What the warehouse uses (person_key = sha256(PID))

    DESIGN NOTE (Sergi, 2026-03-11):
    Sergi's feedback was that facts and dimensions should link to the persistent
    person identifier, NOT the technical key. By deriving person_key = sha256(PID)
    instead of sha256(PTK), we eliminate the reattribution problem entirely:
    when a group grows, merges, or splits, the PTK changes but the PID (and
    therefore person_key) stays stable. No post-hook correction needed on facts.
    This is the Phase 2 approach from ADR-003 Section 11.
*/

with match_groups as (
    select * from {{ ref('stg_match_groups') }}
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
)

select
    mg.pipeline_run,
    mg.run_date,
    mg.source_key,                                               -- [source_key] from source systems
    mg.group_id,                                                 -- [group_id] identity cluster
    r.person_technical_key,                                      -- [PTK] mutable: changes with group composition
    r.person_persistent_id,                                      -- [PID] stable: assigned once, carried forward
    sha256(r.person_persistent_id) as person_key                    -- [person_key] = sha256(PID), the Gold FK
from match_groups mg
inner join registry r
    on mg.pipeline_run = r.pipeline_run                          -- same pipeline run
    and mg.group_id = r.group_id                                 -- same identity cluster
order by mg.pipeline_run, mg.source_key
