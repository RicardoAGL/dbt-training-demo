-- ============================================================
-- DEMO QUERIES: Run these one by one during the presentation
-- Open with: duckdb jaffle_shop.duckdb
-- ============================================================

-- BATCH 1: Initial state -- two groups, fresh PIDs
SELECT * FROM batch_1_initial_groups ORDER BY group_id;

-- BATCH 2: Group grows -- PTK changes, PID stays stable
SELECT * FROM batch_2_group_grows ORDER BY group_id;

-- BATCH 3: Merge -- majority PID wins
SELECT * FROM batch_3_groups_merge;

-- BATCH 4: Split -- majority keeps PID, minority gets new
SELECT * FROM batch_4_group_splits ORDER BY group_id;

-- BATCH 5: The payoff -- all facts resolve, zero orphans
SELECT * FROM batch_5_fact_check ORDER BY booking_date;

-- BONUS: Full stability proof (PTK changes vs PID stability)
SELECT * FROM int_pid_stability_proof ORDER BY pid, pipeline_run;

-- BONUS: The dimension table
SELECT * FROM dim_person;

-- BONUS: Show all tables
SHOW TABLES;
