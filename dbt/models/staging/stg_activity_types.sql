SELECT
    CAST(id AS BIGINT) AS activity_type_id,
    name AS activity_type_name,
    active AS is_active,
    type AS type_key,
    _loaded_at -- assumption: timestamp when the record was loaded into the source table
FROM
    {{ source('postgres_public', 'activity_types') }}