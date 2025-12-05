SELECT
    CAST(stage_id AS BIGINT) AS stage_id,
    stage_name
FROM
    {{ source('pipedrive_crm_raw', 'stages') }}