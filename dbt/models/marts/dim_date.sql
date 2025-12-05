{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    -- Use PostgreSQL's generate_series to create the base date spine.
    SELECT
        GENERATE_SERIES(
            DATE '2020-01-01', -- Start date (safe historical start)
            DATE '2031-12-31', -- End date (future-proof)
            '1 day'::interval 
        )::date AS date_day
)

SELECT
    -- Create a numerical surrogate key for efficient joins (e.g., 20240101)
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS date_key,
    date_day,
    
    -- Calendar Attributes
    EXTRACT(YEAR FROM date_day) AS year,
    DATE_TRUNC('year', date_day) AS year_start_date,
    DATE_TRUNC('quarter', date_day) AS quarter_start_date,
    DATE_TRUNC('month', date_day) AS month_start_date, -- ADDED FOR FINAL MART GROUPING
    
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(DOW FROM date_day) AS day_of_week_num, -- 0=Sunday, 6=Saturday
    
    -- Flags
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend

FROM date_spine