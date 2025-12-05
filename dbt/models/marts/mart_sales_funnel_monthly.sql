-- Configuration for the mart layer, overriding existing dbt_project.yml config
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['month', 'kpi_name'], 
        cluster_by = ['month', 'kpi_name']
    )
}}

-- 1. STAGE PROGRESSION METRICS (Deals Entering Stages)
WITH stage_progression AS (
    SELECT
        DATE_TRUNC('month', stage_enter_at) AS month_start_date,
        t2.lookup_label AS kpi_name,
        COUNT(t1.deal_id) AS deals_count
    FROM
        {{ ref('int_deal_stage_history') }} AS t1
    INNER JOIN
        {{ ref('int_field_options') }} AS t2
        -- Join using the cleaned stage ID and filter for 'Funnel Stage' only
        ON t1.stage_id = t2.lookup_id AND t2.option_type = 'Funnel Stage'
    GROUP BY 1, 2
),

-- 2. ACTIVITY COMPLETION METRICS (Calls Completed)
activity_counts AS (
    SELECT
        month_start_date,
        kpi_name,
        activities_count AS deals_count
    FROM
        {{ ref('int_activity_monthly') }}
),

-- 3. COMBINE ALL METRICS
combined_metrics AS (
    SELECT month_start_date, kpi_name, deals_count FROM stage_progression
    UNION ALL
    SELECT month_start_date, kpi_name, deals_count FROM activity_counts
),

-- 4. Uses dim_date to fill in zero-count months
reporting_spine AS (
    SELECT 
        t1.month_start_date,
        t2.kpi_name
    FROM
        -- Use the already optimized dim_date (contains month_start_date)
        (SELECT DISTINCT month_start_date FROM {{ ref('dim_date') }} WHERE month_start_date <= DATE_TRUNC('month', CURRENT_DATE)) t1
    CROSS JOIN 
        -- Cross join with the list of all possible KPIs
        (SELECT DISTINCT kpi_name FROM {{ ref('int_funnel_map') }}) t2
    GROUP BY 1, 2
),

-- 5. FINAL AGGREGATION AND RANKING
final_mart AS (
    SELECT
        t1.month_start_date AS month,
        t1.kpi_name,
        COALESCE(t2.deals_count, 0) AS deals_count, -- Fills missing months with 0
        
        -- Assign a static order for BI tool sorting (1.0, 2.0, 2.1, etc.)
        CASE 
            WHEN t1.kpi_name = 'Lead Generation' THEN '1.0'
            WHEN t1.kpi_name = 'Qualified Lead' THEN '2.0'
            WHEN t1.kpi_name = 'Sales Call 1' THEN '2.1'
            WHEN t1.kpi_name = 'Needs Assessment' THEN '3.0'
            WHEN t1.kpi_name = 'Sales Call 2' THEN '3.1'
            WHEN t1.kpi_name = 'Proposal/Quote Preparation' THEN '4.0'
            WHEN t1.kpi_name = 'Negotiation' THEN '5.0'
            WHEN t1.kpi_name = 'Closing' THEN '6.0'
            WHEN t1.kpi_name = 'Implementation/Onboarding' THEN '7.0'
            WHEN t1.kpi_name = 'Follow-Up/Customer Success' THEN '8.0'
            WHEN t1.kpi_name = 'Renewal/Expansion' THEN '9.0'
            ELSE '99.0'
        END AS funnel_step -- final required column name
    
    FROM 
        reporting_spine t1
    LEFT JOIN 
        combined_metrics t2
        ON t1.month_start_date = t2.month_start_date
        AND t1.kpi_name = t2.kpi_name
)

-- 6. Final Select and Incremental Logic
SELECT
    month,
    kpi_name,
    funnel_step,
    deals_count
FROM 
    final_mart

-- Incremental logic based on the reporting period (month)
{% if is_incremental() %}
    -- Rebuild the current month and the previous month for data correction/late arrival
    WHERE month >= DATE_TRUNC('month', CURRENT_DATE - interval '1 month')
{% endif %}

ORDER BY
    month, funnel_step