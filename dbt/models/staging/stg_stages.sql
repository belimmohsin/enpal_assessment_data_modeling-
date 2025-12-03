SELECT
    CAST(stage_id AS BIGINT) AS stage_id,
    stage_name,
    _loaded_at -- assumption: timestamp when the record was loaded into the source table
FROM
    {{ source('postgres_public', 'stages') }}