-- Dimension table for airlines
-- Business logic for carrier analysis

{{ config(
    materialized='table',
    tags=['marts', 'daily']
) }}

WITH carrier_stats AS (
    SELECT 
        carrier,
        carrier_name,
        COUNT(DISTINCT flight_id) as total_flights,
        ROUND(AVG(departure_delay_minutes), 2) as avg_departure_delay,
        ROUND(AVG(arrival_delay_minutes), 2) as avg_arrival_delay,
        ROUND(AVG(flight_distance_miles), 0) as avg_distance,
        COUNT(CASE WHEN is_severely_delayed = 1 THEN 1 END) as severely_delayed_flights,
        ROUND(COUNT(CASE WHEN is_severely_delayed = 1 THEN 1 END) * 100.0 / 
              COUNT(DISTINCT flight_id), 2) as severe_delay_percentage
    FROM {{ ref('fct_flights') }}
    GROUP BY 1, 2
)

SELECT 
    carrier,
    carrier_name,
    total_flights,
    avg_departure_delay,
    avg_arrival_delay,
    avg_distance,
    severely_delayed_flights,
    severe_delay_percentage,
    CASE WHEN severe_delay_percentage >= 15 THEN 'High Risk'
         WHEN severe_delay_percentage >= 10 THEN 'Medium Risk'
         ELSE 'Low Risk' END as reliability_rating,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM carrier_stats
ORDER BY total_flights DESC
