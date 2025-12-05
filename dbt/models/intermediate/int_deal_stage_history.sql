WITH deal_stage_events AS (
    -- 1. Select and filter for only the critical stage change events
    SELECT
        deal_id,
        change_at,
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
    -- 2. Identify and remove sequential duplicate stage_ids
    SELECT
        *,
        -- Grab the stage_id from the *previous* chronological row for the same deal
        LAG(stage_id_changed_to) OVER (
            PARTITION BY deal_id 
            ORDER BY change_at ASC
        ) AS previous_stage_id
    FROM 
        deal_stage_events
    WHERE stage_id_changed_to IS NOT NULL
),

filter_duplicates AS (
    -- 3. Filter out rows where the stage_id NOT change from the previous row
    SELECT
        deal_id,
        change_at,
        stage_id_changed_to
    FROM 
        staged_changes
    WHERE
        -- Only keep the row if the stage_id is different from the previous record
        stage_id_changed_to IS DISTINCT FROM previous_stage_id
),

calculate_exit_time AS (
    -- 4. Applying LEAD() function on the clean, distinct timeline
    SELECT
        deal_id,
        stage_id_changed_to AS stage_id,
        change_at AS stage_enter_at,
        
        -- Use LEAD() to find the timestamp of the NEXT stage change.
        LEAD(change_at, 1) OVER (
            PARTITION BY deal_id
            ORDER BY change_at ASC
        ) AS stage_exit_at 
        
    FROM 
        filter_duplicates
    WHERE stage_id_changed_to > 0 -- Remove the initial 'add_time' event (stage_id=0)
)

SELECT
    deal_id,
    stage_id,
    stage_enter_at,
    stage_exit_at 
    
FROM 
    calculate_exit_time
