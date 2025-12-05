SELECT
    CAST(ID AS BIGINT) AS field_id,
    FIELD_KEY AS field_key,
    NAME AS field_name,
    
    CASE 
        WHEN FIELD_VALUE_OPTIONS IS NOT NULL 
        THEN 
            -- Check the raw string value (explicitly cast to TEXT) is not empty
            CASE WHEN TRIM(FIELD_VALUE_OPTIONS::text) != '' 
                THEN FIELD_VALUE_OPTIONS::jsonb
                ELSE NULL
            END
        ELSE NULL
    END AS field_value_options_json 
FROM
    {{ source('pipedrive_crm_raw', 'fields') }}
