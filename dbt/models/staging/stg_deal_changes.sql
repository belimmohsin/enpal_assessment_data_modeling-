SELECT
    CAST(deal_id AS BIGINT) AS deal_id,
    CAST(change_time AS TIMESTAMP) AS change_at,
    changed_field_key AS field_key,
    new_value
FROM
    {{ source('pipedrive_crm_raw', 'deal_changes') }}