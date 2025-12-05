SELECT
    CAST(activity_id AS BIGINT) AS activity_id,
    type AS type_key,
    CAST(assigned_to_user AS BIGINT) AS user_id,
    CAST(deal_id AS BIGINT) AS deal_id,
    
    CASE 
        WHEN UPPER(done::TEXT) = 'TRUE' THEN TRUE 
        WHEN UPPER(done::TEXT) = 'FALSE' THEN FALSE
        ELSE NULL
    END AS is_done, 
    
    CAST(due_to AS TIMESTAMP) AS due_at
FROM
    {{ source('pipedrive_crm_raw', 'activity') }}
