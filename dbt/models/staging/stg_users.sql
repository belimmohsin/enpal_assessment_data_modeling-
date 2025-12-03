SELECT
    CAST(id AS BIGINT) AS user_id,
    name AS user_name,
    email AS user_email,
    CAST(modified AS TIMESTAMP) AS modified_at,
    _loaded_at -- assumption: timestamp when the record was loaded into the source table
FROM
    {{ source('postgres_public', 'users') }}