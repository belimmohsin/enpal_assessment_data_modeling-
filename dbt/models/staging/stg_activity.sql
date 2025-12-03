SELECT
    CAST(activity_id AS BIGINT) AS activity_id,
    type AS type_key,
    CAST(assigned_to_user AS BIGINT) AS user_id,
    CAST(deal_id AS BIGINT) AS deal_id,
    -- Use UPPER() for robustness and performance over ILIKE
    CASE 
        WHEN UPPER(done) = 'TRUE' THEN TRUE 
        WHEN UPPER(done) = 'FALSE' THEN FALSE
        ELSE NULL -- Handle potential nulls or unexpected values
    END AS is_done, 
    CAST(due_to AS TIMESTAMP) AS due_at,
    _loaded_at
FROM
    {{ source('postgres_public', 'activity') }}
