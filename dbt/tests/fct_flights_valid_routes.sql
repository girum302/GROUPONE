-- Test: All flights must have valid origin and destination
-- This ensures data quality for route analysis

SELECT *
FROM {{ ref('fct_flights') }}
WHERE origin IS NULL
   OR dest IS NULL
   OR origin = dest
