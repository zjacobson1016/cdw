CREATE OR REFRESH MATERIALIZED VIEW silver_customers
CLUSTER BY (region, tier)
AS
SELECT
  customer_id,
  customer_name,
  vertical,
  tier,
  region,
  assigned_rep_id,
  CAST(created_date AS DATE) AS created_date,
  _ingested_at
FROM bronze_customers
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL;
