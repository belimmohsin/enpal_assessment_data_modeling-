SELECT
    CAST(id AS BIGINT) AS activity_type_id,
    name AS activity_type_name,
    active AS is_active,
    type AS type_key
FROM
    {{ source('pipedrive_crm_raw', 'activity_types') }}