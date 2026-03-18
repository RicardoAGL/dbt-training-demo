-- Ex 5.3: Custom (singular) test
-- Tests are just SELECT statements that return rows that FAIL
-- If this query returns 0 rows = test passes
-- If this query returns any rows = test fails (those are the bad records)

select
    payment_id,
    amount

from {{ ref('stg_payments') }}

where amount < 0
