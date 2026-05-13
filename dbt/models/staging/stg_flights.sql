-- Staging model for flights data
-- Cleans and standardizes raw flight data

{{ config(
    materialized='view',
    tags=['staging', 'daily']
) }}

WITH source_flights AS (
    SELECT 
        flight_id,
        date,
        time,
        carrier,
        carrier_name,
        origin,
        origin_city,
        dest,
        dest_city,
        dep_delay,
        arr_delay,
        distance,
        passengers,
        delay_category
    FROM {{ source('raw', 'flights') }}
    WHERE dep_delay IS NOT NULL
        AND arr_delay IS NOT NULL
        AND distance > 0
)

SELECT 
    flight_id,
    date,
    time,
    EXTRACT(YEAR FROM date) as flight_year,
    EXTRACT(MONTH FROM date) as flight_month,
    EXTRACT(DAY FROM date) as flight_day,
    EXTRACT(HOUR FROM time) as flight_hour,
    carrier,
    carrier_name,
    origin,
    origin_city,
    dest,
    dest_city,
    CAST(dep_delay AS DECIMAL(10, 2)) as departure_delay_minutes,
    CAST(arr_delay AS DECIMAL(10, 2)) as arrival_delay_minutes,
    CAST(distance AS INT) as flight_distance_miles,
    CAST(passengers AS INT) as passenger_count,
    delay_category,
    
    -- Flags for analysis
    CASE WHEN dep_delay > 0 THEN 1 ELSE 0 END as is_delayed,
    CASE WHEN dep_delay > 60 THEN 1 ELSE 0 END as is_severely_delayed,
    CASE WHEN distance > 2000 THEN 'Long Haul' 
         WHEN distance > 500 THEN 'Medium Haul' 
         ELSE 'Short Haul' END as flight_range,
    
    -- Metadata
    CURRENT_TIMESTAMP as dbt_loaded_at
    
FROM source_flights
