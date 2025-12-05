SELECT
    CAST(ID AS BIGINT) AS field_id,
    FIELD_KEY AS field_key,
    NAME AS field_name,
    
    -- Safely cast the raw text to the PostgreSQL JSON compatible format.
    -- CASE WHEN to handle empty/NULL strings.
    CASE 
        WHEN FIELD_VALUE_OPTIONS IS NOT NULL AND TRIM(FIELD_VALUE_OPTIONS) != '' 
        THEN FIELD_VALUE_OPTIONS::jsonb
        ELSE NULL
    END AS field_value_options_json,

    _loaded_at -- Assumption: timestamp when the record was loaded into the source table
FROM
    {{ source('postgres_public', 'fields') }}