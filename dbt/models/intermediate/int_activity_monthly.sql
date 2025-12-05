WITH relevant_activity AS (
    SELECT
        t1.activity_id,
        t1.deal_id,
        t1.due_at,
        t1.is_done,
        t2.activity_type_name,
        t2.type_key
    FROM
        {{ ref('stg_activity') }} AS t1
    INNER JOIN
        {{ ref('stg_activity_types') }} AS t2
    ON t1.type_key = t2.type_key
    WHERE
        t2.type_key IN ('meeting', 'sc_2')
        AND t1.is_done IS TRUE
),

monthly_aggregation AS (
    SELECT
        DATE_TRUNC('month', due_at) AS month_start_date,
        deal_id,
        activity_type_name,
        type_key,
        COUNT(activity_id) AS activities_count
    FROM
        relevant_activity
    GROUP BY 1, 2, 3, 4
)

SELECT
    month_start_date,
    deal_id,
    
    -- APPLY MACRO: Ensure the final kpi_name (derived from the CASE statement) is standardized
    {{ standardize_text(
        "CASE 
            WHEN type_key = 'meeting' THEN 'Sales Call 1'
            WHEN type_key = 'sc_2' THEN 'Sales Call 2'
            ELSE activity_type_name
        END"
    ) }} AS kpi_name,
    
    activities_count
FROM
    monthly_aggregation