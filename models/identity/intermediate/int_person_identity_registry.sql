/*
    Person Identity Registry: assigns a stable Persistent Person ID (PID) to each group.

    KEY GLOSSARY:
    - PTK (Person Technical Key)  : changes when group membership changes (from int_person_matching)
    - PID (Person Persistent ID)  : STABLE identifier for a person, assigned once, carried forward
    - person_key = sha256(PID)       : the FK used on all Gold-layer fact/dim tables

    ASSIGNMENT RULES:
    - New group       -> mint a fresh PID
    - Group grows     -> PTK changes, but PID is INHERITED (same person, more source keys)
    - Groups merge    -> majority group's PID wins (more members = more history)
    - Group splits    -> majority subgroup keeps PID, minority gets a new PID

    THE CORE INSIGHT (Sergi, 2026-03-11 / ADR-003 Section 11):
    PTK is ephemeral (graph fingerprint), PID is permanent (person identity).
    Sergi's feedback: "Facts should be linked to the persistent person, not the
    technical key." By deriving person_key from PID instead of PTK, we avoid the
    reattribution flow that would otherwise be needed (detect -> audit -> post-hook
    correction on every fact table). The PTK still exists for identity graph accuracy,
    but it never reaches Gold.

    Processes runs T1 -> T2 -> T3 -> T4 sequentially, carrying PIDs forward.
    In production this would be an incremental model; here we simulate all runs.
*/

with match_groups as (
    select * from {{ ref('stg_match_groups') }}
),

matching as (
    select * from {{ ref('int_person_matching') }}
),

-- ============================================================
-- T1: All groups are new -> assign fresh PIDs
--     No prior run exists, so every group gets a brand-new PID.
-- ============================================================
t1_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,                                              -- [group_id] cluster label from identity resolution
        m.person_technical_key,                                  -- [PTK] fingerprint of this group's members
        m.member_count,
        sha256('pid|' || m.group_id) as person_persistent_id,       -- [PID] NEW: first-time assignment
        'new' as assignment_reason
    from matching m
    where m.pipeline_run = 'T1'
),

-- ============================================================
-- T2: Group A grew (dav:404 joined). Group B unchanged.
--     PTK for group_a CHANGES (new member), but PID is INHERITED.
--     This is the key moment: PTK changed, person_key didn't.
-- ============================================================
t2_source_to_prev as (
    -- Trace each T2 source_key back to its T1 group.
    -- dav:404 is new (no T1 match), so t1_group will be NULL for it.
    select
        t2.source_key,                                           -- [source_key] the link between runs
        t2.group_id as t2_group,                                 -- [group_id] in T2
        t1.group_id as t1_group                                  -- [group_id] in T1 (NULL if new member)
    from match_groups t2
    left join match_groups t1
        on t2.source_key = t1.source_key                         -- match on source_key across runs
        and t1.pipeline_run = 'T1'
    where t2.pipeline_run = 'T2'
),

t2_lineage as (
    -- Determine how many PREVIOUS groups contributed to each T2 group.
    -- If >1 previous groups -> merge. If 1 -> growth/unchanged. If 0 -> entirely new.
    select
        t2_group,
        count(distinct t1_group) as prev_group_count,
        -- [PID] INHERITED: pick PID from the T1 group with most members (majority wins)
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
        m.person_technical_key,                                  -- [PTK] CHANGED for group_a (new member)
        m.member_count,
        case
            when l.inherited_pid is not null then l.inherited_pid -- [PID] INHERITED from T1 (stable!)
            else sha256('pid|' || m.group_id || '|T2')              -- [PID] NEW (only if no lineage)
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
-- T3: Groups A (3 members) and B (1 member) merged into C (4 members).
--     Two DIFFERENT PIDs collide. Resolution: majority PID wins.
--     Group A had 3 members -> its PID survives. Group B's PID is retired.
-- ============================================================
t3_source_to_prev as (
    -- Trace each T3 source_key back to its T2 group.
    -- group_c in T3 will have members from BOTH group_a AND group_b in T2.
    select
        t3.source_key,                                           -- [source_key] the link between runs
        t3.group_id as t3_group,                                 -- [group_id] in T3 (group_c)
        t2.group_id as t2_group                                  -- [group_id] in T2 (group_a or group_b)
    from match_groups t3
    left join match_groups t2
        on t3.source_key = t2.source_key
        and t2.pipeline_run = 'T2'
    where t3.pipeline_run = 'T3'
),

t3_lineage as (
    select
        t3_group,
        count(distinct t2_group) as prev_group_count,            -- >1 means MERGE happened
        -- [PID] MERGE RESOLUTION: pick PID from the group with more members
        -- Group A (3 members) beats Group B (1 member), so Group A's PID survives
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
        m.person_technical_key,                                  -- [PTK] CHANGED again (4 members now)
        m.member_count,
        case
            when l.inherited_pid is not null then l.inherited_pid -- [PID] INHERITED from majority group (A)
            else sha256('pid|' || m.group_id || '|T3')
        end as person_persistent_id,
        case
            when l.prev_group_count > 1 then 'merge'            -- prev_group_count=2 -> this was a merge
            when l.inherited_pid is not null then 'inherited'
            else 'new'
        end as assignment_reason
    from matching m
    inner join t3_lineage l on m.group_id = l.t3_group
    where m.pipeline_run = 'T3'
),

-- ============================================================
-- T4: Group C (4 members) splits into:
--     - group_a (3 members: dav:101, smc:202, dav:404) -> KEEPS the PID
--     - group_d (1 member: dav:303)                     -> gets NEW PID
--     Majority keeps the PID. Same PID as Batch 1!
-- ============================================================
t4_source_to_prev as (
    -- Trace each T4 source_key back to its T3 group.
    -- Both group_a and group_d in T4 came from group_c in T3 (a split).
    select
        t4.source_key,                                           -- [source_key] the link between runs
        t4.group_id as t4_group,                                 -- [group_id] in T4 (group_a or group_d)
        t3.group_id as t3_group                                  -- [group_id] in T3 (group_c for all)
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
        max(s.t3_group) as from_t3_group,                        -- which T3 group they came from
        count(*) as member_count_in_split                        -- how many members in this subgroup
    from t4_source_to_prev s
    group by s.t4_group
),

-- Rank subgroups by size to determine who keeps the PID
t4_split_majority as (
    select
        t4_group,
        from_t3_group,
        member_count_in_split,
        row_number() over (
            partition by from_t3_group
            order by member_count_in_split desc, t4_group asc    -- largest subgroup = rank 1
        ) as size_rank
    from t4_lineage
),

t4_groups as (
    select
        m.pipeline_run,
        m.run_date,
        m.group_id,
        m.person_technical_key,                                  -- [PTK] CHANGED again (different members)
        m.member_count,
        case
            when sm.size_rank = 1 then t3g.person_persistent_id  -- [PID] INHERITED: majority keeps it
            else sha256('pid|' || m.group_id || '|T4')              -- [PID] NEW: minority gets a fresh one
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
