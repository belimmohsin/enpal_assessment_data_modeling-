SELECT
    CAST(id AS BIGINT) AS user_id,
    name AS user_name,
    email AS user_email,
    CAST(modified AS TIMESTAMP) AS modified_at
FROM
    {{ source('pipedrive_crm_raw', 'users') }}