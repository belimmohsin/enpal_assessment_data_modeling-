WITH deal_stage_events AS (
    -- 1. Select and filter for only the critical stage change events
    SELECT
        deal_id,
        change_at,
        -- When field_key is 'stage_id', the new stage ID is in new_value.
        -- When field_key is 'add_time', stage 0 as it was deal created time as per info in fields table
        CASE 
            WHEN field_key = 'stage_id' THEN CAST(new_value AS BIGINT)
            WHEN field_key = 'add_time' THEN 0 
            ELSE NULL 
        END AS stage_id_changed_to,
        field_key
    FROM 
        {{ ref('stg_deal_changes') }}
    WHERE 
        field_key IN ('stage_id', 'add_time') 
        AND new_value IS NOT NULL
),

staged_changes AS (
    -- 2. Rank and prepare for LEAD() function
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY deal_id 
            ORDER BY change_at ASC
        ) AS rn
    FROM 
        deal_stage_events
    WHERE stage_id_changed_to IS NOT NULL -- Exclude rows where we couldn't parse the stage_id
),

calculate_exit_time AS (
    -- 3. Calculate the exit time for the current stage 
    SELECT
        deal_id,
        stage_id_changed_to AS stage_id,
        change_at AS stage_enter_at,
        LEAD(change_at, 1) OVER (
            PARTITION BY deal_id
            ORDER BY change_at ASC
        ) AS stage_exit_at 
        
    FROM 
        staged_changes
    -- Remove the initial 'add_time' event (stage_id=0), as it only serves to for the deal created timeline
    WHERE stage_id_changed_to > 0 
)

SELECT
    deal_id,
    stage_id,
    stage_enter_at,
    stage_exit_at     --if stage_exit_at is NULL, the deal is currently in this stage.
    
FROM 
    calculate_exit_time