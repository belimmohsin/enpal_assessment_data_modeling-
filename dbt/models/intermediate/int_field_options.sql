WITH fields_with_options AS (
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
    SELECT
        field_key,
        field_name,
        jsonb_array_elements(field_value_options_json) AS option_object
    FROM 
        fields_with_options
)

SELECT
    field_key,
    field_name,
    
    CAST((option_object ->> 'id') AS BIGINT) AS lookup_id,
    
    -- APPLY MACRO: Standardize the extracted label once here
    {{ standardize_text('option_object ->> \'label\'') }} AS lookup_label,
    
    CASE
        WHEN field_key = 'stage_id' THEN 'Funnel Stage'
        WHEN field_key = 'lost_reason' THEN 'Lost Reason'
        ELSE 'Other'
    END AS option_type

FROM 
    flattened_options