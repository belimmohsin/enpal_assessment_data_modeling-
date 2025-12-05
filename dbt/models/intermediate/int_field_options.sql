WITH fields_with_options AS (
    -- 1. Select only the rows that contain the JSON array of options
    SELECT
        field_key,
        field_name,
        field_value_options_json
    FROM
        {{ ref('stg_fields') }}
    WHERE 
        field_value_options_json IS NOT NULL
),

flattened_options AS (
    -- 2. Use jsonb_array_elements to unnest the JSON array
    SELECT
        field_key,
        field_name,
        jsonb_array_elements(field_value_options_json) AS option_object
    FROM 
        fields_with_options
)

SELECT
    -- 3. Extract the ID and Label from the unnested JSON object
    field_key,
    field_name,
    
    -- The option_object looks like: {"id": "1", "label": "Lead Generation"}
    -- Extract the ID and cast it to BIGINT for consistency
    CAST(JSON_EXTRACT_PATH_TEXT(option_object, 'id') AS BIGINT) AS lookup_id,
    JSON_EXTRACT_PATH_TEXT(option_object, 'label') AS lookup_label,
    
    -- Classify the options for easy joining later
    CASE
        WHEN field_key = 'stage_id' THEN 'Funnel Stage'
        WHEN field_key = 'lost_reason' THEN 'Lost Reason'
        ELSE 'Other'
    END AS option_type

FROM 
    flattened_options