-- Dimension table for flight routes
-- Business logic for route analysis

{{ config(
    materialized='table',
    tags=['marts', 'daily']
) }}

WITH route_stats AS (
    SELECT 
        origin,
        dest,
        origin_city,
        dest_city,
        flight_range,
        COUNT(DISTINCT flight_id) as total_flights,
        ROUND(AVG(departure_delay_minutes), 2) as avg_departure_delay,
        ROUND(AVG(flight_distance_miles), 0) as avg_distance_miles,
        SUM(passenger_count) as total_passengers,
        COUNT(CASE WHEN is_delayed = 1 THEN 1 END) as delayed_flights,
        ROUND(COUNT(CASE WHEN is_delayed = 1 THEN 1 END) * 100.0 / 
              COUNT(DISTINCT flight_id), 2) as delay_rate_percent
    FROM {{ ref('fct_flights') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    origin,
    dest,
    CONCAT(origin, '-', dest) as route_code,
    origin_city,
    dest_city,
    CONCAT(origin_city, ' to ', dest_city) as route_description,
    flight_range as distance_category,
    total_flights,
    avg_departure_delay,
    avg_distance_miles,
    total_passengers,
    delayed_flights,
    delay_rate_percent,
    CASE WHEN delay_rate_percent >= 20 THEN 'Critical'
         WHEN delay_rate_percent >= 15 THEN 'High'
         WHEN delay_rate_percent >= 10 THEN 'Moderate'
         ELSE 'Low' END as delay_risk_level,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM route_stats
WHERE total_flights >= 10
ORDER BY delay_rate_percent DESC
