CREATE OR REFRESH MATERIALIZED VIEW silver_orders
CLUSTER BY (order_date)
AS
SELECT
  order_id,
  customer_id,
  rep_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(order_amount AS DECIMAL(18,2)) AS order_amount,
  CAST(ship_date AS DATE) AS ship_date,
  ship_status,
  product_category,
  CAST(quantity AS INT) AS quantity,
  is_confirmed,
  _ingested_at
FROM bronze_orders
WHERE order_id IS NOT NULL
  AND order_amount > 0;
