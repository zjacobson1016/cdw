CREATE OR REFRESH MATERIALIZED VIEW silver_sales_reps
CLUSTER BY (region)
AS
SELECT
  rep_id,
  rep_name,
  email,
  region,
  role,
  manager_name,
  CAST(hire_date AS DATE) AS hire_date,
  CAST(annual_quota AS DECIMAL(18,2)) AS annual_quota,
  _ingested_at
FROM bronze_sales_reps
WHERE rep_id IS NOT NULL
  AND rep_name IS NOT NULL;
