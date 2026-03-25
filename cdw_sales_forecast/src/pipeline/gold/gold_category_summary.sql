-- Revenue breakdown by product category and month for trend analysis
CREATE OR REFRESH MATERIALIZED VIEW gold_category_summary
CLUSTER BY (month_date, product_category)
AS
SELECT
  DATE_TRUNC('MONTH', invoice_date) AS month_date,
  product_category,
  SUM(invoice_amount) AS total_invoiced,
  SUM(recognized_revenue) AS total_recognized,
  COUNT(*) AS invoice_count,
  AVG(invoice_amount) AS avg_invoice_amount
FROM silver_invoices
GROUP BY DATE_TRUNC('MONTH', invoice_date), product_category;
