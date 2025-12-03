SELECT
    CAST(deal_id AS BIGINT) AS deal_id,
    CAST(change_time AS TIMESTAMP) AS change_at,
    changed_field_key AS field_key,
    new_value,
    _loaded_at -- assumption: timestamp when the record was loaded into the source table
FROM
    {{ source('postgres_public', 'deal_changes') }}