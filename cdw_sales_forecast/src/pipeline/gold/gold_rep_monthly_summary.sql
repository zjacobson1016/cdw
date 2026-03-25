-- Monthly summary per sales rep combining invoiced, confirmed, and recognized revenue
CREATE OR REFRESH MATERIALIZED VIEW gold_rep_monthly_summary
CLUSTER BY (month_date, rep_id)
AS
WITH invoice_summary AS (
  SELECT
    rep_id,
    DATE_TRUNC('MONTH', invoice_date) AS month_date,
    SUM(invoice_amount) AS total_invoiced,
    SUM(recognized_revenue) AS total_recognized,
    COUNT(*) AS invoice_count
  FROM silver_invoices
  GROUP BY rep_id, DATE_TRUNC('MONTH', invoice_date)
),
order_summary AS (
  SELECT
    rep_id,
    DATE_TRUNC('MONTH', order_date) AS month_date,
    SUM(order_amount) AS total_confirmed_orders,
    COUNT(*) AS confirmed_order_count,
    SUM(CASE WHEN ship_status IN ('Shipped', 'Delivered') THEN order_amount ELSE 0 END) AS shipped_revenue
  FROM silver_orders
  WHERE is_confirmed = true
  GROUP BY rep_id, DATE_TRUNC('MONTH', order_date)
),
deal_summary AS (
  SELECT
    rep_id,
    DATE_TRUNC('MONTH', expected_close_date) AS month_date,
    SUM(deal_amount) AS total_pipeline_value,
    SUM(forecasted_amount) AS total_weighted_pipeline,
    COUNT(*) AS active_deal_count
  FROM silver_deals
  WHERE stage NOT IN ('Closed Won')
  GROUP BY rep_id, DATE_TRUNC('MONTH', expected_close_date)
)
SELECT
  COALESCE(i.rep_id, o.rep_id, d.rep_id) AS rep_id,
  r.rep_name,
  r.region,
  r.role,
  r.annual_quota,
  COALESCE(i.month_date, o.month_date, d.month_date) AS month_date,
  COALESCE(i.total_invoiced, 0) AS total_invoiced,
  COALESCE(i.total_recognized, 0) AS total_recognized,
  COALESCE(i.invoice_count, 0) AS invoice_count,
  COALESCE(o.total_confirmed_orders, 0) AS total_confirmed_orders,
  COALESCE(o.confirmed_order_count, 0) AS confirmed_order_count,
  COALESCE(o.shipped_revenue, 0) AS shipped_revenue,
  COALESCE(d.total_pipeline_value, 0) AS total_pipeline_value,
  COALESCE(d.total_weighted_pipeline, 0) AS total_weighted_pipeline,
  COALESCE(d.active_deal_count, 0) AS active_deal_count,
  COALESCE(i.total_invoiced, 0) + COALESCE(d.total_weighted_pipeline, 0) AS blended_forecast
FROM invoice_summary i
FULL OUTER JOIN order_summary o ON i.rep_id = o.rep_id AND i.month_date = o.month_date
FULL OUTER JOIN deal_summary d ON COALESCE(i.rep_id, o.rep_id) = d.rep_id AND COALESCE(i.month_date, o.month_date) = d.month_date
LEFT JOIN silver_sales_reps r ON COALESCE(i.rep_id, o.rep_id, d.rep_id) = r.rep_id;
