WITH stage_map AS (
    -- 1. Map Stages (from the dedicated table)
    SELECT
        'stage' AS source_type,
        CAST(stage_id AS TEXT) AS source_id,
        -- APPLY MACRO: Standardize the stage name
        {{ standardize_text('stage_name') }} AS kpi_name,
        {{ standardize_text('stage_name') }} AS funnel_step
    FROM
        {{ ref('stg_stages') }}
),

activity_map AS (
    -- 2. Map Activities (Sales Call 1, Sales Call 2)
    SELECT
        'activity' AS source_type,
        -- We must select a consistent ID type (BIGINT)
        t1.type_key AS source_id, 
        
        -- APPLY MACRO: Standardize the activity name
        {{ standardize_text('t2.activity_type_name') }} AS kpi_name,
        {{ standardize_text('t2.activity_type_name') }} AS funnel_step
    FROM 
        {{ ref('stg_activity') }} AS t1 
    INNER JOIN 
        {{ ref('stg_activity_types') }} AS t2
    ON t1.type_key = t2.type_key
    WHERE
        -- Filter for relevant activity types
        t2.type_key IN ('meeting', 'sc_2')
    GROUP BY 1, 2, 3, 4
),

flattened_options_map AS (
    -- 3. Use the JSON flattened options model 
    -- (Labels are ALREADY STANDARDIZED in the int_field_options model)
    SELECT
        'stage_official' AS source_type,
        CAST(lookup_id AS TEXT) AS source_id,
        lookup_label AS kpi_name, 
        lookup_label AS funnel_step
    FROM
        {{ ref('int_field_options') }}
    WHERE option_type = 'Funnel Stage'
)

-- 4. Combine all maps
SELECT source_type, source_id, kpi_name, funnel_step FROM stage_map
UNION ALL
SELECT source_type, source_id, kpi_name, funnel_step FROM activity_map
UNION ALL
SELECT source_type, source_id, kpi_name, funnel_step FROM flattened_options_map