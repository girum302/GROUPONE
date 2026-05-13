-- Staging model for weather data
-- Cleans and standardizes raw weather data

{{ config(
    materialized='view',
    tags=['staging', 'daily']
) }}

WITH source_weather AS (
    SELECT 
        airport,
        city,
        temperature,
        humidity,
        wind_speed,
        conditions,
        visibility
    FROM {{ source('raw', 'weather') }}
    WHERE airport IS NOT NULL
)

SELECT 
    airport,
    city,
    CAST(temperature AS DECIMAL(5, 2)) as temperature_fahrenheit,
    CAST(humidity AS INT) as humidity_percent,
    CAST(wind_speed AS DECIMAL(5, 2)) as wind_speed_mph,
    conditions as weather_condition,
    CAST(visibility AS DECIMAL(10, 2)) as visibility_miles,
    
    -- Weather classifications
    CASE WHEN temperature < 32 THEN 'Freezing'
         WHEN temperature < 50 THEN 'Cold'
         WHEN temperature < 70 THEN 'Cool'
         WHEN temperature < 85 THEN 'Warm'
         ELSE 'Hot' END as temperature_category,
    
    CASE WHEN conditions IN ('Rainy', 'Stormy') THEN 1 ELSE 0 END as is_adverse_weather,
    
    CURRENT_TIMESTAMP as dbt_loaded_at
    
FROM source_weather
