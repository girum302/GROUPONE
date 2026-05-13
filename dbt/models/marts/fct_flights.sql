-- Fact table for flights with all details
-- Combines flights, weather, and delay information

{{ config(
    materialized='table',
    tags=['marts', 'daily']
) }}

WITH flights AS (
    SELECT * FROM {{ ref('stg_flights') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT 
    f.flight_id,
    f.date,
    f.time,
    f.flight_year,
    f.flight_month,
    f.flight_day,
    f.flight_hour,
    f.carrier,
    f.carrier_name,
    f.origin,
    f.origin_city,
    f.dest,
    f.dest_city,
    f.departure_delay_minutes,
    f.arrival_delay_minutes,
    f.flight_distance_miles,
    f.passenger_count,
    f.delay_category,
    f.is_delayed,
    f.is_severely_delayed,
    f.flight_range,
    
    -- Weather at origin
    w_orig.temperature_fahrenheit as origin_temperature,
    w_orig.humidity_percent as origin_humidity,
    w_orig.weather_condition as origin_weather,
    w_orig.is_adverse_weather as origin_has_adverse_weather,
    
    f.dbt_loaded_at
    
FROM flights f
LEFT JOIN weather w_orig 
    ON f.origin = w_orig.airport
