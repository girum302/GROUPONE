-- Test: Arrival delay should be less than or equal to departure delay
-- (Assuming no time zone issues for simplicity)

SELECT *
FROM {{ ref('fct_flights') }}
WHERE arrival_delay_minutes > departure_delay_minutes + 120
   OR arrival_delay_minutes < departure_delay_minutes - 120
