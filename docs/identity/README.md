# PID-based Person Key Prototype

## What this is

A dbt-duckdb prototype that validates the design where `person_key = md5(PID)` instead of `md5(PTK)`. This eliminates fact reattribution when person groups change.

Based on Sergi's feedback: the PTK is a transient grouping device used only during identity resolution. Once a PID (persistent UUID) is assigned, everything downstream uses the PID.

## Quick start

```bash
# Make sure you're on the right branch
git checkout prototype/pid-person-key

# Install dependencies (if not already)
uv pip install dbt-duckdb

# Build everything (seeds + models + tests)
dbt build --select identity

# All 23 artifacts should pass
```

## Exploring the batches

The prototype simulates 4 pipeline runs and packages them into 5 explorable batch views. Query them in order to see the identity system evolve:

```sql
-- Batch 1: Two person groups identified, fresh PIDs assigned
SELECT * FROM batch_1_initial_groups ORDER BY group_id;

-- Batch 2: New source key joins Group A. PTK changes. PID stays the same.
SELECT * FROM batch_2_group_grows ORDER BY group_id;

-- Batch 3: Groups A and B merge. Majority PID wins.
SELECT * FROM batch_3_groups_merge;

-- Batch 4: Group splits. Majority keeps PID, minority gets new one.
SELECT * FROM batch_4_group_splits ORDER BY group_id;

-- Batch 5: All bookings resolve against final dim_person. Zero orphans.
SELECT * FROM batch_5_fact_check ORDER BY booking_date;
```

You can also query the underlying models directly:

```sql
-- Full registry: see all PIDs across all runs
SELECT * FROM int_person_identity_registry ORDER BY pipeline_run, group_id;

-- Stability proof: PTK changes vs PID stability side by side
SELECT * FROM int_pid_stability_proof ORDER BY pid, pipeline_run;

-- Final dimension
SELECT * FROM dim_person;

-- Final fact table with FK
SELECT * FROM fct_booking;
```

## Test scenarios

| Scenario | Run | What happens | Expected |
|----------|-----|-------------|----------|
| Group growth | T1 -> T2 | dav:404 joins Group A | PTK changes, PID stable |
| Merge | T2 -> T3 | Groups A+B merge into C | Majority PID (Group A) survives |
| Split | T3 -> T4 | dav:303 leaves Group C | Majority keeps PID, minority gets new |
| Fact integrity | All | Bookings from any run | All resolve to dim_person, zero orphans |

## Running tests only

```bash
# All tests (schema + custom)
dbt test --select identity

# Just the custom identity tests
dbt test --select test_type:singular tag:identity
```

## Key design decisions

1. **PTK is ephemeral** -- lives only in the intermediate layer for identity graph accuracy
2. **PID is stable** -- assigned once, survives growth/merge, drives everything in Gold
3. **person_key = md5(PID)** -- the FK on all fact tables, never needs updating
4. **Merge rule: majority wins** -- the contributing group with more members keeps its PID
5. **Split rule: majority inherits** -- largest subgroup keeps PID, others get new ones

## Models

```
seeds/identity/
  raw_source_records.csv    -- 4 source system records
  raw_match_groups.csv      -- 15 rows: group assignments across 4 pipeline runs
  raw_bookings.csv          -- 5 bookings from different time periods

models/identity/
  int_person_matching.sql           -- Computes PTK per group per run
  int_person_identity_registry.sql  -- PTK -> PID mapping (the core logic)
  bridge_person_identity.sql        -- source_key -> PTK -> PID -> person_key
  dim_person.sql                    -- Gold dimension (person_key = md5(PID))
  fct_booking.sql                   -- Fact table with stable person_key FK
  int_pid_stability_proof.sql       -- Exploration: PTK changes vs PID stability

models/identity/batches/
  batch_1_initial_groups.sql  -- Step 1: fresh groups
  batch_2_group_grows.sql     -- Step 2: group grows, PTK changes, PID stable
  batch_3_groups_merge.sql    -- Step 3: merge, majority PID wins
  batch_4_group_splits.sql    -- Step 4: split, majority keeps PID
  batch_5_fact_check.sql      -- Step 5: all facts resolve

tests/identity/
  assert_person_key_stable_across_ptk_changes.sql  -- Core: PID stability
  assert_pid_survives_merge.sql                    -- Merge: majority wins
  assert_merge_excludes_minority_pid.sql           -- Merge: minority loses
  assert_split_majority_keeps_pid.sql              -- Split: majority keeps
  assert_ptk_does_change_across_runs.sql           -- Positive: PTK does change
  assert_no_reattribution_needed.sql               -- FK integrity: zero orphans
```

---

## Interactive demo with Claude

If Claude Code is reading this, here are instructions for running an interactive batch-by-batch walkthrough during a team meeting.

### Claude: Interactive Batch Demo

When asked to run the interactive demo (e.g., "run the identity demo", "show the batches", "walk us through the prototype"):

1. **Set context**: Briefly explain what the prototype validates (PID-based person_key eliminates reattribution).

2. **Run each batch one at a time**, pausing between each for discussion:

   ```
   Batch 1 -> explain initial state, two groups, fresh PIDs
   [pause for questions]
   Batch 2 -> highlight: PTK changed, PID didn't. Ask: "what would happen with the old design?"
   [pause for questions]
   Batch 3 -> the merge. Show which PID won and why. Show the was_group_a_pid / was_group_b_pid columns.
   [pause for questions]
   Batch 4 -> the split. Point out "same as Batch 1!" in continuity column.
   [pause for questions]
   Batch 5 -> the payoff. All facts resolve. No reattribution. fk_status = RESOLVED for all rows.
   ```

3. **For each batch**, run:
   ```python
   python3 -c "
   import duckdb, os
   os.chdir(os.path.expanduser('~/Desktop/github/dbt-training-demo'))
   con = duckdb.connect('jaffle_shop.duckdb', read_only=True)
   df = con.execute('SELECT * FROM <batch_table_name>').fetchdf()
   print(df.to_string(index=False))
   con.close()
   "
   ```

4. **After Batch 5**, offer to show:
   - The stability proof table (`int_pid_stability_proof`) for a full timeline view
   - The test results (`dbt test --select identity`)
   - Any specific model's SQL for those who want to see the logic

5. **Key talking points per batch**:
   - Batch 1: "This is our starting point. Two persons, each identified by their source keys."
   - Batch 2: "A new booking source matched into Group A. The PTK changed because the group composition changed. But look at the PID and person_key columns -- unchanged. With the old design, we'd need to reattribute all of Group A's facts right now."
   - Batch 3: "This is the interesting one. Two groups merged. We had to pick which PID survives. Rule: majority wins. Group A had 3 members, Group B had 1. Group A's PID survived. All historical facts for both groups now resolve through one person_key."
   - Batch 4: "Now dav:303 split away. The majority (3 members) kept the original PID -- same one since Batch 1. The minority got a new PID. Four pipeline runs, three PTK changes, zero person_key changes for the main group."
   - Batch 5: "The payoff. Every booking from January through March resolves to a valid person in the April dimension. No orphans. No reattribution job. No audit columns needed on facts."
