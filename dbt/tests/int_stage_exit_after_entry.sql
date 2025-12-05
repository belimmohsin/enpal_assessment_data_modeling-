-- Checks that stage_exit_at is never before stage_enter_at for closed deals.
SELECT
    *
FROM
    {{ ref('int_deal_stage_history') }}
WHERE
    stage_exit_at IS NOT NULL
    AND stage_exit_at < stage_enter_at
