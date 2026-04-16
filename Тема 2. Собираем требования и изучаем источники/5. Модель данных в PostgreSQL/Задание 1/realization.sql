SELECT DISTINCT json_field ->> 'product_name'
FROM (
  SELECT json_array_elements(event_value::json -> 'product_payments') AS json_field
  FROM outbox
) AS subquery;
