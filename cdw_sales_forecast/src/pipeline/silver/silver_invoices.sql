CREATE OR REFRESH MATERIALIZED VIEW silver_invoices
CLUSTER BY (invoice_date)
AS
SELECT
  invoice_id,
  customer_id,
  rep_id,
  CAST(invoice_date AS DATE) AS invoice_date,
  CAST(invoice_amount AS DECIMAL(18,2)) AS invoice_amount,
  status,
  CAST(revenue_recognized_pct AS DECIMAL(5,2)) AS revenue_recognized_pct,
  ROUND(CAST(invoice_amount AS DECIMAL(18,2)) * CAST(revenue_recognized_pct AS DECIMAL(5,2)), 2) AS recognized_revenue,
  product_category,
  po_number,
  payment_terms,
  _ingested_at
FROM bronze_invoices
WHERE invoice_id IS NOT NULL
  AND invoice_amount > 0
  AND status != 'Cancelled';
