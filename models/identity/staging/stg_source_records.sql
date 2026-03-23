/*
    Staging: source system records.
    One row per known source_key (e.g. dav:101 = Davinci customer 101).

    This is the "who exists" table -- every natural key from every source system.
*/

with source as (
    select * from {{ ref('raw_source_records') }}
)

select
    source_system,                                               -- e.g. davinci, smc
    source_id,                                                   -- numeric ID in that system
    source_key,                                                  -- [source_key] namespaced: "dav:101"
    first_seen_at
from source
