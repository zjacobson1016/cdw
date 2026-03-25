-- Invoice-level detail with rep and customer context for drilldown
CREATE OR REFRESH MATERIALIZED VIEW gold_invoice_details
CLUSTER BY (invoice_date, rep_id)
AS
SELECT
  inv.invoice_id,
  inv.rep_id,
  r.rep_name,
  r.region AS rep_region,
  inv.customer_id,
  c.customer_name,
  c.vertical AS customer_vertical,
  c.tier AS customer_tier,
  inv.invoice_date,
  inv.invoice_amount,
  inv.status,
  inv.revenue_recognized_pct,
  inv.recognized_revenue,
  inv.product_category,
  inv.po_number,
  inv.payment_terms
FROM silver_invoices inv
LEFT JOIN silver_sales_reps r ON inv.rep_id = r.rep_id
LEFT JOIN silver_customers c ON inv.customer_id = c.customer_id;
