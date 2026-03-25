CREATE OR REFRESH MATERIALIZED VIEW silver_deals
CLUSTER BY (created_date, stage)
AS
SELECT
  deal_id,
  customer_id,
  rep_id,
  deal_name,
  stage,
  CAST(deal_amount AS DECIMAL(18,2)) AS deal_amount,
  CAST(forecasted_amount AS DECIMAL(18,2)) AS forecasted_amount,
  CAST(probability AS DECIMAL(5,2)) AS probability,
  product_category,
  CAST(created_date AS DATE) AS created_date,
  CAST(expected_close_date AS DATE) AS expected_close_date,
  deal_type,
  next_step,
  _ingested_at
FROM bronze_deals
WHERE deal_id IS NOT NULL
  AND deal_amount > 0
  AND stage NOT IN ('Closed Lost');
