/*
    Person Matching: computes a Person Technical Key (PTK) per group per pipeline run.

    KEY GLOSSARY (for this prototype):
    - source_key  : identifier from a source system (e.g. dav:101, smc:202)
    - group_id    : the identity-resolution cluster label (e.g. group_a)
    - PTK         : Person Technical Key = sha256('ptk|' + sorted source_keys)
                    CHANGES whenever group membership changes (new member, merge, split).
                    This is the "fingerprint" of the group's current composition.

    INPUT:  raw_match_groups (source_key, group_id, pipeline_run)
    OUTPUT: one row per (pipeline_run, group_id) with the computed PTK
*/

with match_groups as (
    select * from {{ ref('stg_match_groups') }}
),

-- Step 1: For each (pipeline_run, group_id), collect all source_keys
--         and concatenate them in sorted order. This gives a deterministic
--         "membership fingerprint" for the group.
group_members as (
    select
        pipeline_run,
        run_date,
        group_id,
        source_key,                                              -- [source_key] from each source system
        listagg(source_key, '|') within group (order by source_key)
            over (partition by pipeline_run, group_id) as sorted_members,  -- e.g. "dav:101|dav:404|smc:202"
        count(*) over (partition by pipeline_run, group_id) as member_count
    from match_groups
),

-- Step 2: Hash the membership fingerprint to produce the PTK.
--         If the group gains/loses a member, sorted_members changes -> PTK changes.
--         This is BY DESIGN: PTK tracks the current graph state, not the person.
ptk_per_group as (
    select distinct
        pipeline_run,
        run_date,
        group_id,                                                -- [group_id] identity-resolution cluster
        sorted_members,
        member_count,
        sha256('ptk|' || sorted_members) as person_technical_key    -- [PTK] = sha256 of membership fingerprint
    from group_members
)

select
    pipeline_run,
    run_date,
    group_id,
    sorted_members,
    member_count,
    person_technical_key
from ptk_per_group
order by pipeline_run, group_id
