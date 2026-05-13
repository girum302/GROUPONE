-- Generic test for negative delays (delay should be >= 0)

{% test delay_should_be_non_negative(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
