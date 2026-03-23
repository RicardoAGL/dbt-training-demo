/*
    Person Matching: computes a Person Technical Key (PTK) per group per pipeline run.
    PTK = md5 of sorted, pipe-delimited source keys with 'ptk|' namespace prefix.

    The PTK changes whenever group membership changes (new member, merge, split).
    This is correct behavior -- PTK reflects the CURRENT state of the identity graph.
*/

with match_groups as (
    select * from {{ ref('raw_match_groups') }}
),

-- Sort source keys within each group and concatenate them
group_members as (
    select
        pipeline_run,
        run_date,
        group_id,
        source_key,
        -- Sorted, pipe-delimited source keys per group (deterministic)
        listagg(source_key, '|') within group (order by source_key)
            over (partition by pipeline_run, group_id) as sorted_members,
        count(*) over (partition by pipeline_run, group_id) as member_count
    from match_groups
),

-- Compute PTK per group
ptk_per_group as (
    select distinct
        pipeline_run,
        run_date,
        group_id,
        sorted_members,
        member_count,
        md5('ptk|' || sorted_members) as person_technical_key
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
