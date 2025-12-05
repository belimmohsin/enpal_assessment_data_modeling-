-- This test checks if the maximum change_time is not more than 10 days in the future.
-- If the result of this query returns any rows, the test fails.
SELECT
    deal_id,
    change_time
FROM 
    {{ source('pipedrive_crm_raw', 'deal_changes') }}
WHERE 
    -- PostgreSQL syntax for comparing to the current time plus an interval
    change_time > CURRENT_TIMESTAMP + interval '10 days'