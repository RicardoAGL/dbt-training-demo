/*
    Bridge: maps each source_key to its PTK and PID per pipeline run.
    This is the join path from source facts to dim_person.
*/

with match_groups as (
    select * from {{ ref('raw_match_groups') }}
),

registry as (
    select * from {{ ref('int_person_identity_registry') }}
)

select
    mg.pipeline_run,
    mg.run_date,
    mg.source_key,
    mg.group_id,
    r.person_technical_key,
    r.person_persistent_id,
    -- The Gold FK: derived from the STABLE PID, not the mutable PTK
    md5(r.person_persistent_id) as person_key
from match_groups mg
inner join registry r
    on mg.pipeline_run = r.pipeline_run
    and mg.group_id = r.group_id
order by mg.pipeline_run, mg.source_key
