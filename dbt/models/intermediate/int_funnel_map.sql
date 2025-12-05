WITH stage_map AS (
    -- 1. Map Stages (1-9) directly to the KPI name
    SELECT
        'stage' AS source_type,
        stage_id AS source_id,
        stage_name AS kpi_name,
        stage_name AS funnel_step
    FROM
        {{ ref('stg_stages') }}
),

activity_map AS (
    -- 2. Map Activities (Sales Call 1, Sales Call 2)
    SELECT
        'activity' AS source_type,
        CAST(t1.type_key AS BIGINT) AS source_id, -- Cast key to align with stage_id type
        t2.activity_type_name AS kpi_name,
        t2.activity_type_name AS funnel_step
    FROM 
        {{ ref('stg_activity') }} AS t1 -- Use stg_activity just to get distinct type_keys
    INNER JOIN 
        {{ ref('stg_activity_types') }} AS t2
    ON t1.type_key = t2.type_key
    WHERE
        t2.type_key IN ('meeting', 'sc_2')
    GROUP BY 1, 2, 3, 4
),

flattened_options AS (
    -- 3. Use the JSON flattened options model to get official stage names for safety
    SELECT
        'stage_official' AS source_type,
        lookup_id AS source_id,
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
SELECT source_type, source_id, kpi_name, funnel_step FROM flattened_options