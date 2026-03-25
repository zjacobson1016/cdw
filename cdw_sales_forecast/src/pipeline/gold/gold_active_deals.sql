-- Active deals with customer and rep enrichment for the app's deal pipeline view
CREATE OR REFRESH MATERIALIZED VIEW gold_active_deals
CLUSTER BY (expected_close_date, stage)
AS
SELECT
  d.deal_id,
  d.deal_name,
  d.rep_id,
  r.rep_name,
  r.region AS rep_region,
  d.customer_id,
  c.customer_name,
  c.vertical AS customer_vertical,
  c.tier AS customer_tier,
  d.stage,
  d.deal_amount,
  d.forecasted_amount,
  d.probability,
  d.product_category,
  d.created_date,
  d.expected_close_date,
  d.deal_type,
  d.next_step
FROM silver_deals d
LEFT JOIN silver_sales_reps r ON d.rep_id = r.rep_id
LEFT JOIN silver_customers c ON d.customer_id = c.customer_id
WHERE d.stage NOT IN ('Closed Won');
