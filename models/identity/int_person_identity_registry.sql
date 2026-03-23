/*
    Person Identity Registry: assigns a stable Persistent Person ID (PID) to each group.

    Rules:
    - First time a group appears -> assign a new PID (pseudo-UUID via md5)
    - Group grows (PTK changes, shares source keys) -> keep same PID
    - Two groups merge -> older PID survives (by first_seen_at)
    - Group splits -> majority subgroup keeps PID, minority gets new one

    Processes runs T1 -> T2 -> T3 -> T4 sequentially, carrying PIDs forward.
    In production this would be an incremental model; here we simulate all runs.
*/

with match_groups as (
    select * from {{ ref('raw_match_groups') }}
),

matching as (
    select * from {{ ref('int_person_matching') }}
),

-- ============================================================
-- T1: All groups are new -> assign fresh PIDs
-- ============================================================
t1_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,
        m.person_technical_key,
        m.member_count,
        md5('pid|' || m.group_id) as person_persistent_id,
        'new' as assignment_reason
    from matching m
    where m.pipeline_run = 'T1'
),

-- ============================================================
-- T2: Group A grew (dav:404 joined). Group B unchanged.
-- Trace each T2 source key back to its T1 group to inherit PID.
-- ============================================================
t2_source_to_prev as (
    -- For each source key in T2, find which T1 group it was in
    select
        t2.source_key,
        t2.group_id as t2_group,
        t1.group_id as t1_group
    from match_groups t2
    left join match_groups t1
        on t2.source_key = t1.source_key
        and t1.pipeline_run = 'T1'
    where t2.pipeline_run = 'T2'
),

t2_lineage as (
    select
        t2_group,
        count(distinct t1_group) as prev_group_count,
        -- On merge: pick PID from contributing group with most members (majority wins).
        -- Consistent with T3/T4 merge rules. If tied, smallest PID (deterministic).
        (select t1g2.person_persistent_id
         from t2_source_to_prev s2
         inner join t1_groups t1g2 on s2.t1_group = t1g2.group_id
         where s2.t2_group = s.t2_group
         order by t1g2.member_count desc, t1g2.person_persistent_id asc
         limit 1
        ) as inherited_pid,
        count(case when t1_group is null then 1 end) as new_members
    from t2_source_to_prev s
    left join t1_groups t1g on s.t1_group = t1g.group_id
    group by t2_group
),

t2_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,
        m.person_technical_key,
        m.member_count,
        case
            when l.inherited_pid is not null then l.inherited_pid
            else md5('pid|' || m.group_id || '|T2')
        end as person_persistent_id,
        case
            when l.prev_group_count > 1 then 'merge'
            when l.inherited_pid is not null then 'inherited'
            else 'new'
        end as assignment_reason
    from matching m
    inner join t2_lineage l on m.group_id = l.t2_group
    where m.pipeline_run = 'T2'
),

-- ============================================================
-- T3: Groups A and B merged into C. Older PID wins.
-- ============================================================
t3_source_to_prev as (
    select
        t3.source_key,
        t3.group_id as t3_group,
        t2.group_id as t2_group
    from match_groups t3
    left join match_groups t2
        on t3.source_key = t2.source_key
        and t2.pipeline_run = 'T2'
    where t3.pipeline_run = 'T3'
),

t3_lineage as (
    select
        t3_group,
        count(distinct t2_group) as prev_group_count,
        -- On merge: pick the PID from the contributing group with most members
        -- (majority wins). If tied on size, alphabetically smallest PID (deterministic).
        (select t2g.person_persistent_id
         from t3_source_to_prev s2
         inner join t2_groups t2g on s2.t2_group = t2g.group_id
         where s2.t3_group = s.t3_group
         order by t2g.member_count desc, t2g.person_persistent_id asc
         limit 1
        ) as inherited_pid
    from t3_source_to_prev s
    group by t3_group
),

t3_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,
        m.person_technical_key,
        m.member_count,
        case
            when l.inherited_pid is not null then l.inherited_pid
            else md5('pid|' || m.group_id || '|T3')
        end as person_persistent_id,
        case
            when l.prev_group_count > 1 then 'merge'
            when l.inherited_pid is not null then 'inherited'
            else 'new'
        end as assignment_reason
    from matching m
    inner join t3_lineage l on m.group_id = l.t3_group
    where m.pipeline_run = 'T3'
),

-- ============================================================
-- T4: Group C splits. Majority (3 members -> group_a) keeps PID.
-- Minority (1 member -> group_d) gets new PID.
-- ============================================================
t4_source_to_prev as (
    select
        t4.source_key,
        t4.group_id as t4_group,
        t3.group_id as t3_group
    from match_groups t4
    left join match_groups t3
        on t4.source_key = t3.source_key
        and t3.pipeline_run = 'T3'
    where t4.pipeline_run = 'T4'
),

t4_lineage as (
    select
        s.t4_group,
        count(distinct s.t3_group) as prev_group_count,
        max(s.t3_group) as from_t3_group,
        count(*) as member_count_in_split
    from t4_source_to_prev s
    group by s.t4_group
),

-- Find the majority subgroup (inherits the PID)
t4_split_majority as (
    select
        t4_group,
        from_t3_group,
        member_count_in_split,
        row_number() over (
            partition by from_t3_group
            order by member_count_in_split desc, t4_group asc
        ) as size_rank
    from t4_lineage
),

t4_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,
        m.person_technical_key,
        m.member_count,
        case
            -- Majority subgroup inherits PID from parent
            when sm.size_rank = 1 then t3g.person_persistent_id
            -- Minority gets a new PID
            else md5('pid|' || m.group_id || '|T4')
        end as person_persistent_id,
        case
            when sm.size_rank = 1 then 'split_majority'
            else 'split_minority_new'
        end as assignment_reason
    from matching m
    inner join t4_split_majority sm on m.group_id = sm.t4_group
    inner join t3_groups t3g on sm.from_t3_group = t3g.group_id
    where m.pipeline_run = 'T4'
)

-- ============================================================
-- Union all runs into the registry
-- ============================================================
select * from t1_groups
union all
select * from t2_groups
union all
select * from t3_groups
union all
select * from t4_groups
order by pipeline_run, group_id
