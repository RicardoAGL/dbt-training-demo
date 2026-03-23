/*
    Staging: identity resolution match groups.
    Each row = "in pipeline run X, source_key Y belongs to group Z."

    This is the OUTPUT of the identity resolution engine (Davinci/SMC matching).
    The prototype seeds this data to simulate 4 pipeline runs (T1-T4).
*/

with source as (
    select * from {{ ref('raw_match_groups') }}
)

select
    pipeline_run,                                                -- T1, T2, T3, T4
    run_date,
    source_key,                                                  -- [source_key] which customer record
    group_id                                                     -- [group_id] which identity cluster
from source
